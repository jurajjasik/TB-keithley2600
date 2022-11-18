# -*- coding: utf-8 -*-
"""
Created on Tue Oct  4 14:12:33 2022

@author: jasik
"""
import time
import logging
from typing import (
    IO,
    Optional,
    Any,
    Dict,
    Union,
    List,
    Tuple,
    Set,
    Sequence,
    Iterable,
    Iterator,
)
import numpy as np
import pandas as pd
from pandas import DataFrame
from keithley2600.keithley_driver import Keithley2600, KeithleyClass


logger = logging.getLogger(__name__)


class _Keithley2600Extend(Keithley2600):
    def __init__(self, *args, **kwargs) -> None:
        super(_Keithley2600Extend, self).__init__(*args, **kwargs)
    
    def current_voltage_sweep_dual_smu(
        self,
        smui: KeithleyClass,
        smuv: KeithleyClass,
        smui_sweeplist: Sequence[float],
        smuv_sweeplist: Sequence[float],
        t_int: float,
        delay: float,
        pulsed: bool,
    ) -> Tuple[List[float], List[float], List[float], List[float]]:
        """
        Sweeps voltages at two SMUs. Measures and returns current and voltage
        during sweep.

        :param smui: 1st keithley smu instance to be swept.
        :param smuv: 2nd keithley smu instance to be swept.
        :param smui_sweeplist: Currents to sweep at ``smui`` (can be a numpy array,
            list, tuple or any other iterable with numbers).
        :param smuv_sweeplist: Voltages to sweep at ``smuv`` (can be a numpy array,
            list, tuple or any other iterable with numbers).
        :param t_int: Integration time per data point. Must be between 0.001 to 25 times
            the power line frequency (50Hz or 60Hz).
        :param delay: Settling delay before each measurement. A value of -1
            automatically starts a measurement once the current is stable.
        :param pulsed: Select pulsed or continuous sweep. In a pulsed sweep, the voltage
            is always reset to zero between data points.
        :returns: Lists of voltages and currents measured during the sweep (in
            Volt and Ampere, respectively): ``(v_smui, i_smui, v_smuv,
            i_smuv)``.
        """

        with self._measurement_lock:

            if len(smui_sweeplist) != len(smuv_sweeplist):
                raise ValueError("Sweep lists must have equal lengths")

            # Define lists containing results.
            # If we abort early, we have something to return.
            v_smui, i_smui, v_smuv, i_smuv = [], [], [], []

            if self.abort_event.is_set():
                return v_smui, i_smui, v_smuv, i_smuv

            # Setup smui/smuv for sweep measurement.

            # setup smui and smuv to sweep through lists on trigger
            # send sweep_list over in chunks if too long
            if len(smui_sweeplist) > self.CHUNK_SIZE:
                self.create_lua_attr("python_driver_list", [])
                for num in smui_sweeplist:
                    self.table.insert(self.python_driver_list, num)
                smui.trigger.source.listi(self.python_driver_list)
                self.delete_lua_attr("python_driver_list")
            else:
                smui.trigger.source.listi(smui_sweeplist)

            if len(smuv_sweeplist) > self.CHUNK_SIZE:
                self.create_lua_attr("python_driver_list", [])
                for num in smuv_sweeplist:
                    self.table.insert(self.python_driver_list, num)
                smuv.trigger.source.listv(self.python_driver_list)
                self.delete_lua_attr("python_driver_list")
            else:
                smuv.trigger.source.listv(smuv_sweeplist)

            smui.trigger.source.action = smui.ENABLE
            smuv.trigger.source.action = smuv.ENABLE

            # CONFIGURE INTEGRATION TIME FOR EACH MEASUREMENT
            self.set_integration_time(smui, t_int)
            self.set_integration_time(smuv, t_int)

            # CONFIGURE SETTLING TIME FOR GATE VOLTAGE, I-LIMIT, ETC...
            smui.measure.delay = delay
            smuv.measure.delay = delay

            # enable autorange if not in high capacitance mode
            if smui.source.highc == smui.DISABLE: # JJ: not sure if this option is correct for current source
                smui.measure.autorangev = smui.AUTORANGE_ON
            if smuv.source.highc == smuv.DISABLE:
                smuv.measure.autorangei = smuv.AUTORANGE_ON

            # smui.trigger.source.limiti = 0.1
            # smuv.trigger.source.limiti = 0.1

            smui.source.func = smui.OUTPUT_DCAMPS
            smuv.source.func = smuv.OUTPUT_DCVOLTS

            # 2-wire measurement (use SENSE_REMOTE for 4-wire)
            # smui.sense = smui.SENSE_LOCAL
            # smuv.sense = smuv.SENSE_LOCAL

            # CLEAR BUFFERS
            for smu in [smui, smuv]:
                smu.nvbuffer1.clear()
                smu.nvbuffer2.clear()
                smu.nvbuffer1.clearcache()
                smu.nvbuffer2.clearcache()

            # display current values during measurement
            # for smu in (smui, smuv):
                # smu_name = self._get_smu_name(smu)
                # getattr(self.display, smu_name).measure.func = self.display.MEASURE_DCAMPS
            getattr(self.display, self._get_smu_name(smui)).measure.func = self.display.MEASURE_DCVOLTS
            getattr(self.display, self._get_smu_name(smuv)).measure.func = self.display.MEASURE_DCAMPS

            # SETUP TRIGGER ARM AND COUNTS
            # trigger count = number of data points in measurement
            # arm count = number of times the measurement is repeated (set to 1)

            npts = len(smui_sweeplist)

            smui.trigger.count = npts
            smuv.trigger.count = npts

            # SET THE MEASUREMENT TRIGGER ON BOTH SMU'S
            # Set measurement to trigger once a change in the gate value on
            # sweep smu is complete, i.e., a measurement will occur
            # after the voltage is stepped.
            # Both channels should be set to trigger on the sweep smu event
            # so the measurements occur at the same time.

            # enable smu
            smui.trigger.measure.action = smui.ENABLE
            smuv.trigger.measure.action = smuv.ENABLE

            # measure current and voltage on trigger, store in buffer of smu
            smui.trigger.measure.iv(smui.nvbuffer1, smui.nvbuffer2)
            smuv.trigger.measure.iv(smuv.nvbuffer1, smuv.nvbuffer2)

            # initiate measure trigger when source is complete
            smui.trigger.measure.stimulus = smui.trigger.SOURCE_COMPLETE_EVENT_ID
            smuv.trigger.measure.stimulus = smui.trigger.SOURCE_COMPLETE_EVENT_ID

            # SET THE ENDPULSE ACTION TO HOLD
            # Options are SOURCE_HOLD AND SOURCE_IDLE, hold maintains same voltage
            # throughout step in sweep (typical IV sweep behavior). idle will allow
            # pulsed IV sweeps.

            if pulsed:
                end_pulse_action = 0  # SOURCE_IDLE
            elif not pulsed:
                end_pulse_action = 1  # SOURCE_HOLD
            else:
                raise TypeError("'pulsed' must be of type 'bool'.")

            smui.trigger.endpulse.action = end_pulse_action
            smuv.trigger.endpulse.action = end_pulse_action

            # SET THE ENDSWEEP ACTION TO HOLD IF NOT PULSED
            # Output voltage will be held after sweep is done!

            smui.trigger.endsweep.action = end_pulse_action
            smuv.trigger.endsweep.action = end_pulse_action

            # SET THE EVENT TO TRIGGER THE SMU'S TO THE ARM LAYER
            # A typical measurement goes from idle -> arm -> trigger.
            # The 'trigger.event_id' option sets the transition arm -> trigger
            # to occur after sending *trg to the instrument.

            smui.trigger.arm.stimulus = self.trigger.EVENT_ID

            # Prepare an event blender (blender #1) that triggers when
            # the smua enters the trigger layer or reaches the end of a
            # single trigger layer cycle.

            # triggers when either of the stimuli are true ('or enable')
            self.trigger.blender[1].orenable = True
            self.trigger.blender[1].stimulus[1] = smui.trigger.ARMED_EVENT_ID
            self.trigger.blender[1].stimulus[2] = smui.trigger.PULSE_COMPLETE_EVENT_ID

            # SET THE smui SOURCE STIMULUS TO BE EVENT BLENDER #1
            # A source measure cycle within the trigger layer will occur when
            # either the trigger layer is entered (termed 'armed event') for the
            # first time or a single cycle of the trigger layer is complete (termed
            # 'pulse complete event').

            smui.trigger.source.stimulus = self.trigger.blender[1].EVENT_ID

            # PREPARE AN EVENT BLENDER (blender #2) THAT TRIGGERS WHEN BOTH SMU'S
            # HAVE COMPLETED A MEASUREMENT.
            # This is needed to prevent the next source measure cycle from occurring
            # before the measurement on both channels is complete.

            self.trigger.blender[2].orenable = False  # triggers when both stimuli are true
            self.trigger.blender[2].stimulus[1] = smui.trigger.MEASURE_COMPLETE_EVENT_ID
            self.trigger.blender[2].stimulus[2] = smuv.trigger.MEASURE_COMPLETE_EVENT_ID

            # SET THE smui ENDPULSE STIMULUS TO BE EVENT BLENDER #2
            smui.trigger.endpulse.stimulus = self.trigger.blender[2].EVENT_ID

            # TURN ON smui AND smuv
            smui.source.output = smui.OUTPUT_ON
            smuv.source.output = smuv.OUTPUT_ON

            # INITIATE MEASUREMENT
            # prepare SMUs to wait for trigger
            smui.trigger.initiate()
            smuv.trigger.initiate()

            # send trigger
            self.send_trigger()

            # CHECK STATUS BUFFER FOR MEASUREMENT TO FINISH
            # Possible return values:
            # 6 = smua and smub sweeping
            # 4 = only smub sweeping
            # 2 = only smua sweeping
            # 0 = neither smu sweeping

            # while loop that runs until the sweep begins
            while self.status.operation.sweeping.condition == 0:
                time.sleep(0.1)

            # while loop that runs until the sweep ends
            while self.status.operation.sweeping.condition > 0:
                time.sleep(0.1)

            # EXTRACT DATA FROM SMU BUFFERS
            i_smui = self.read_buffer(smui.nvbuffer1)
            v_smui = self.read_buffer(smui.nvbuffer2)
            i_smuv = self.read_buffer(smuv.nvbuffer1)
            v_smuv = self.read_buffer(smuv.nvbuffer2)

            # CLEAR BUFFERS
            for smu in [smui, smuv]:
                smu.nvbuffer1.clear()
                smu.nvbuffer2.clear()
                smu.nvbuffer1.clearcache()
                smu.nvbuffer2.clearcache()

            return v_smui, i_smui, v_smuv, i_smuv
    
    
    def transfer_measurement_i(
        self,
        smu_gate: KeithleyClass,
        smu_drain: KeithleyClass,
        vg_start: float,
        vg_stop: float,
        vg_step: float,
        id_list: Sequence[float],
        t_int: float,
        delay: float,
        pulsed: bool,
        callback = None
    ) -> DataFrame:
        """
        Records a transfer curve with forward and reverse gate sweeps for given drain currents and returns the results
        in a :class:`pandas.DataFrame` instance.

        :param smu_gate: Keithley smu attached to gate electrode.
        :param smu_drain: Keithley smu attached to drain electrode.
        :param vg_start: Start voltage of transfer sweep in Volt.
        :param vg_stop: End voltage of transfer sweep in Volt.
        :param vg_step: Voltage step size for transfer sweep in Volt.
        :param id_list: List of drain current steps in Amps. Can be a numpy array, list,
            tuple, range / xrange. 
        :param t_int: Integration time per data point. Must be between 0.001 to 25 times
            the power line frequency (50Hz or 60Hz).
        :param delay: Settling delay before each measurement. A value of -1
            automatically starts a measurement once the current is stable.
        :param bool pulsed: Select pulsed or continuous sweep. In a pulsed sweep, the
            voltage is always reset to zero between data points.
        :param method(idrain:float, df:DataFrame) callback: Callback method invoked after 
            each gate sweep. Argument: dataframe with transfer curve data.
        :returns: Transfer curve data.
        """

        vg_start = float(vg_start)
        vg_stop = float(vg_stop)
        vg_step = float(vg_step)
        id_list = np.array(id_list, dtype=np.double)
        
        with self._measurement_lock:

            self.abort_event.clear()

            msg = f"Recording transfer curve with Vg from {vg_start}V to {vg_stop}V, Id = {id_list}A."
            logger.info(msg)

            # create array with gate voltage steps, always include a step >= VgStop
            step = np.sign(vg_stop - vg_start) * abs(vg_step)
            sweeplist_gate_fwd = np.arange(vg_start, vg_stop + step, step)
            sweeplist_gate_rvs = np.flip(sweeplist_gate_fwd, 0)
            sweeplist_gate = np.append(sweeplist_gate_fwd, sweeplist_gate_rvs)
            
            direction_gate_fwd = np.append(
                np.ones_like(sweeplist_gate_fwd, dtype=int),
                np.zeros_like(sweeplist_gate_rvs, dtype=int)
            )

            rt = pd.DataFrame(columns=['Vg_SP', 'Id_SP', 'Vg', 'Ig', 'Vd', 'Id'])

            # record sweeps for every drain voltage step
            for idrain in id_list:
                logger.info(f"idrain = {idrain}")
                
                # check for abort event
                if self.abort_event.is_set():
                    self.reset()
                    self.beeper.beep(0.3, 2400)
                    return rt

                # create array with drain currents
                sweeplist_drain = np.full_like(sweeplist_gate, idrain)

                # conduct sweep
                v_d, i_d, v_g, i_g = self.current_voltage_sweep_dual_smu(
                    smu_drain,
                    smu_gate,
                    sweeplist_drain,
                    sweeplist_gate,
                    t_int,
                    delay,
                    pulsed,
                )

                if not self.abort_event.is_set():
                    d = {
                        'Vg_SP': sweeplist_gate, 
                        'Id_SP': sweeplist_drain, 
                        'Vg_dir_fwd': direction_gate_fwd,
                        'Vg':v_g, 
                        'Ig':i_g, 
                        'Vd':v_d, 
                        'Id':i_d
                    }
                    _df = pd.DataFrame(data=d)
                    rt = pd.concat([rt, _df], ignore_index=False)
                    
                    if callback is not None:
                        callback(idrain, _df)
                    
            self.reset()

            return rt


    def transfer_measurement_v(
        self,
        smu_gate: KeithleyClass,
        smu_drain: KeithleyClass,
        vg_start: float,
        vg_stop: float,
        vg_step: float,
        vd_list: Sequence[float],
        t_int: float,
        delay: float,
        pulsed: bool,
        callback = None
    ) -> DataFrame:
        """
        Records a transfer curve with forward and reverse gate voltage sweeps for given drain voltage and returns the results
        in a :class:`pandas.DataFrame` instance.

        :param smu_gate: Keithley smu attached to gate electrode.
        :param smu_drain: Keithley smu attached to drain electrode.
        :param vg_start: Start voltage of transfer sweep in Volt.
        :param vg_stop: End voltage of transfer sweep in Volt.
        :param vg_step: Voltage step size for transfer sweep in Volt.
        :param vd_list: List of drain voltage steps in Volt. Can be a numpy array, list,
            tuple, range / xrange. 
        :param t_int: Integration time per data point. Must be between 0.001 to 25 times
            the power line frequency (50Hz or 60Hz).
        :param delay: Settling delay before each measurement. A value of -1
            automatically starts a measurement once the current is stable.
        :param bool pulsed: Select pulsed or continuous sweep. In a pulsed sweep, the
            voltage is always reset to zero between data points.
        :param method(vdrain:float, df:DataFrame) callback: Callback method invoked after 
            each gate sweep. Argument: dataframe with transfer curve data.
        :returns: Transfer curve data.
        """

        vg_start = float(vg_start)
        vg_stop = float(vg_stop)
        vg_step = float(vg_step)
        vd_list = np.array(vd_list, dtype=np.double)
        
        with self._measurement_lock:

            self.abort_event.clear()

            msg = f"Recording transfer curve with Vg from {vg_start}V to {vg_stop}V, Vd = {vd_list}V."
            logger.info(msg)

            # create array with gate voltage steps, always include a step >= VgStop
            step = np.sign(vg_stop - vg_start) * abs(vg_step)
            sweeplist_gate_fwd = np.arange(vg_start, vg_stop + step, step)
            sweeplist_gate_rvs = np.flip(sweeplist_gate_fwd, 0)
            sweeplist_gate = np.append(sweeplist_gate_fwd, sweeplist_gate_rvs)

            rt = pd.DataFrame(columns=['Vg_SP', 'Vd_SP', 'Vg', 'Ig', 'Vd', 'Id'])

            # record sweeps for every drain voltage step
            for vdrain in vd_list:
                logger.info(f"vdrain = {vdrain}")
                
                # check for abort event
                if self.abort_event.is_set():
                    self.reset()
                    self.beeper.beep(0.3, 2400)
                    return rt

                # create array with drain currents
                sweeplist_drain = np.full_like(sweeplist_gate, vdrain)

                # conduct sweep
                v_d, i_d, v_g, i_g = self.voltage_sweep_dual_smu(
                    smu_drain,
                    smu_gate,
                    sweeplist_drain,
                    sweeplist_gate,
                    t_int,
                    delay,
                    pulsed,
                )

                if not self.abort_event.is_set():
                    d = {
                        'Vg_SP': sweeplist_gate, 'Vd_SP': sweeplist_drain, 
                        'Vg':v_g, 'Ig':i_g, 'Vd':v_d, 'Id':i_d
                    }
                    _df = pd.DataFrame(data=d)
                    rt = pd.concat([rt, _df], ignore_index=False)
                    
                    if callback is not None:
                        callback(vdrain, _df)
                    
            self.reset()

            return rt


    def iv_measurement_v(
        self,
        smu_gate: KeithleyClass,
        smu_drain: KeithleyClass,
        vd_start: float,
        vd_stop: float,
        vd_step: float,
        vg_list: Sequence[float],
        t_int: float,
        delay: float,
        pulsed: bool,
        callback = None
    ) -> DataFrame:
        """
        Records IV curves with drain voltage sweeps for given gate voltages and returns the results
        in a :class:`pandas.DataFrame` instance.

        :param smu_gate: Keithley smu attached to gate electrode.
        :param smu_drain: Keithley smu attached to drain electrode.
        :param vd_start: Start voltage of drain sweep in Volt.
        :param vd_stop: End voltage of drain sweep in Volt.
        :param vd_step: Voltage step size for drain sweep in Volt.
        :param vg_list: List of gate voltage steps in Volt. Can be a numpy array, list,
            tuple, range / xrange. 
        :param t_int: Integration time per data point. Must be between 0.001 to 25 times
            the power line frequency (50Hz or 60Hz).
        :param delay: Settling delay before each measurement. A value of -1
            automatically starts a measurement once the current is stable.
        :param bool pulsed: Select pulsed or continuous sweep. In a pulsed sweep, the
            voltage is always reset to zero between data points.
        :param method(vgate:float, df:DataFrame) callback: Callback method invoked after 
            each gate sweep. Argument: dataframe with transfer curve data.
        :returns: Transfer curve data.
        """

        vd_start = float(vd_start)
        vd_stop = float(vd_stop)
        vd_step = float(vd_step)
        vg_list = np.array(vg_list, dtype=np.double)
        
        with self._measurement_lock:

            self.abort_event.clear()

            msg = f"Recording iv curve with Vd from {vd_start}V to {vd_stop}V, Vg = {vg_list}V."
            logger.info(msg)

            # create array with gate voltage steps, always include a step >= VgStop
            step = np.sign(vd_stop - vd_start) * abs(vd_step)
            sweeplist_drain = np.arange(vd_start, vd_stop + step, step)

            rt = pd.DataFrame(columns=['Vg_SP', 'Vd_SP', 'Vg', 'Ig', 'Vd', 'Id'])

            # record sweeps for every gate voltage step
            for vgate in vg_list:
                logger.info(f"vgate = {vgate}")
                
                # check for abort event
                if self.abort_event.is_set():
                    self.reset()
                    self.beeper.beep(0.3, 2400)
                    return rt

                # create array with drain currents
                sweeplist_gate = np.full_like(sweeplist_drain, vgate)

                # conduct sweep
                v_d, i_d, v_g, i_g = self.voltage_sweep_dual_smu(
                    smu_drain,
                    smu_gate,
                    sweeplist_drain,
                    sweeplist_gate,
                    t_int,
                    delay,
                    pulsed,
                )

                if not self.abort_event.is_set():
                    d = {
                        'Vg_SP': sweeplist_gate, 'Vd_SP': sweeplist_drain, 
                        'Vg':v_g, 'Ig':i_g, 'Vd':v_d, 'Id':i_d
                    }
                    _df = pd.DataFrame(data=d)
                    rt = pd.concat([rt, _df], ignore_index=False)
                    
                    if callback is not None:
                        callback(vgate, _df)
                    
            self.reset()

            return rt


    def current_sweep_single_smu(
        self,
        smu: KeithleyClass,
        smu_sweeplist: Sequence[float],
        t_int: float,
        delay: float,
        pulsed: bool,
    ) -> Tuple[List[float], List[float]]:
        """
        Sweeps the current through the specified list of steps at the given
        SMU. Measures and returns the current and voltage during the sweep.

        :param smu: A keithley smu instance.
        :param smu_sweeplist: Currents to sweep through (can be a numpy array, list,
            tuple or any other iterable of numbers).
        :param t_int: Integration time per data point. Must be between 0.001 to 25 times
            the power line frequency (50Hz or 60Hz).
        :param delay: Settling delay before each measurement. A value of -1
            automatically starts a measurement once the current is stable.
        :param pulsed: Select pulsed or continuous sweep. In a pulsed sweep, the current
            is always reset to zero between data points.
        :returns: Lists of voltages and currents measured during the sweep (in
            Volt and Ampere, respectively): ``(v_smu, i_smu)``.
        """

        with self._measurement_lock:

            # Define lists containing results.
            # If we abort early, we have something to return.
            v_smu, i_smu = [], []

            if self.abort_event.is_set():
                return v_smu, i_smu

            # setup smu to sweep through list on trigger
            # send sweep_list over in chunks if too long
            if len(smu_sweeplist) > self.CHUNK_SIZE:
                self.create_lua_attr("python_driver_list", [])
                for num in smu_sweeplist:
                    self.table.insert(self.python_driver_list, num)
                smu.trigger.source.listi(self.python_driver_list)
                self.delete_lua_attr("python_driver_list")
            else:
                smu.trigger.source.listi(smu_sweeplist)

            smu.trigger.source.action = smu.ENABLE

            # CONFIGURE INTEGRATION TIME FOR EACH MEASUREMENT
            self.set_integration_time(smu, t_int)

            # CONFIGURE SETTLING TIME FOR GATE VOLTAGE, I-LIMIT, ETC...
            smu.measure.delay = delay

            # enable autorange if not in high capacitance mode
            if smu.source.highc == smu.DISABLE:
                smu.measure.autorangei = smu.AUTORANGE_ON

            # smu.trigger.source.limiti = 0.1

            smu.source.func = smu.OUTPUT_DCAMPS

            # 2-wire measurement (use SENSE_REMOTE for 4-wire)
            # smu.sense = smu.SENSE_LOCAL

            # clears SMU buffers
            smu.nvbuffer1.clear()
            smu.nvbuffer2.clear()

            smu.nvbuffer1.clearcache()
            smu.nvbuffer2.clearcache()

            # display current values during measurement
            smu_name = self._get_smu_name(smu)
            getattr(self.display, smu_name).measure.func = self.display.MEASURE_DCVOLTS

            # SETUP TRIGGER ARM AND COUNTS
            # trigger count = number of data points in measurement
            # arm count = number of times the measurement is repeated (set to 1)

            npts = len(smu_sweeplist)
            smu.trigger.count = npts

            # SET THE MEASUREMENT TRIGGER ON BOTH SMU'S
            # Set measurement to trigger once a change in the gate value on
            # sweep smu is complete, i.e., a measurement will occur
            # after the voltage is stepped.
            # Both channels should be set to trigger on the sweep smu event
            # so the measurements occur at the same time.

            # enable smu
            smu.trigger.measure.action = smu.ENABLE

            # measure current and voltage on trigger, store in buffer of smu
            smu.trigger.measure.iv(smu.nvbuffer1, smu.nvbuffer2)

            # initiate measure trigger when source is complete
            smu.trigger.measure.stimulus = smu.trigger.SOURCE_COMPLETE_EVENT_ID

            # SET THE ENDPULSE ACTION TO HOLD
            # Options are SOURCE_HOLD AND SOURCE_IDLE, hold maintains same voltage
            # throughout step in sweep (typical IV sweep behavior). idle will allow
            # pulsed IV sweeps.

            if pulsed:
                end_pulse_action = 0  # SOURCE_IDLE
            elif not pulsed:
                end_pulse_action = 1  # SOURCE_HOLD
            else:
                raise TypeError("'pulsed' must be of type 'bool'.")

            smu.trigger.endpulse.action = end_pulse_action

            # SET THE ENDSWEEP ACTION TO HOLD IF NOT PULSED
            # Output voltage will be held after sweep is done!

            smu.trigger.endsweep.action = end_pulse_action

            # SET THE EVENT TO TRIGGER THE SMU'S TO THE ARM LAYER
            # A typical measurement goes from idle -> arm -> trigger.
            # The 'trigger.event_id' option sets the transition arm -> trigger
            # to occur after sending *trg to the instrument.

            smu.trigger.arm.stimulus = self.trigger.EVENT_ID

            # Prepare an event blender (blender #1) that triggers when
            # the smua enters the trigger layer or reaches the end of a
            # single trigger layer cycle.

            # triggers when either of the stimuli are true ('or enable')
            self.trigger.blender[1].orenable = True
            self.trigger.blender[1].stimulus[1] = smu.trigger.ARMED_EVENT_ID
            self.trigger.blender[1].stimulus[2] = smu.trigger.PULSE_COMPLETE_EVENT_ID

            # SET THE smu SOURCE STIMULUS TO BE EVENT BLENDER #1
            # A source measure cycle within the trigger layer will occur when
            # either the trigger layer is entered (termed 'armed event') for the
            # first time or a single cycle of the trigger layer is complete (termed
            # 'pulse complete event').

            smu.trigger.source.stimulus = self.trigger.blender[1].EVENT_ID

            # PREPARE AN EVENT BLENDER (blender #2) THAT TRIGGERS WHEN BOTH SMU'S
            # HAVE COMPLETED A MEASUREMENT.
            # This is needed to prevent the next source measure cycle from occurring
            # before the measurement on both channels is complete.

            self.trigger.blender[2].orenable = True  # triggers when both stimuli are true
            self.trigger.blender[2].stimulus[1] = smu.trigger.MEASURE_COMPLETE_EVENT_ID

            # SET THE SMU ENDPULSE STIMULUS TO BE EVENT BLENDER #2
            smu.trigger.endpulse.stimulus = self.trigger.blender[2].EVENT_ID

            # TURN ON smu
            smu.source.output = smu.OUTPUT_ON

            # INITIATE MEASUREMENT
            # prepare SMUs to wait for trigger
            smu.trigger.initiate()

            # send trigger
            self.send_trigger()

            # CHECK STATUS BUFFER FOR MEASUREMENT TO FINISH
            # Possible return values:
            # 6 = smua and smub sweeping
            # 4 = only smub sweeping
            # 2 = only smua sweeping
            # 0 = neither smu sweeping

            # while loop that runs until the sweep begins
            while self.status.operation.sweeping.condition == 0:
                time.sleep(0.1)

            # while loop that runs until the sweep ends
            while self.status.operation.sweeping.condition > 0:
                time.sleep(0.1)

            # EXTRACT DATA FROM SMU BUFFERS
            i_smu = self.read_buffer(smu.nvbuffer1)
            v_smu = self.read_buffer(smu.nvbuffer2)

            smu.nvbuffer1.clear()
            smu.nvbuffer2.clear()

            smu.nvbuffer1.clearcache()
            smu.nvbuffer2.clearcache()

            return v_smu, i_smu
            

class Keithley2600ExtendFactory:

    _instances: Set[_Keithley2600Extend] = set()

    def __new__(cls, *args, **kwargs) -> _Keithley2600Extend:
        """
        Create new instance for a new visa_address, otherwise return existing instance.
        """
        address = args[0]

        for instance in cls._instances:
            if instance.visa_address == address:
                logger.debug("Returning existing instance with address '%s'.", address)
                return instance

        logger.debug("Creating new instance with address '%s'.", args[0])
        instance = _Keithley2600Extend(*args, **kwargs)
        cls._instances.add(instance)

        return instance
