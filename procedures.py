import logging

from time import sleep
from datetime import datetime
from pathlib import Path
import numpy as np

from settings import settings

from pymeasure.experiment import Procedure
from pymeasure.experiment import FloatParameter, BooleanParameter, Parameter, Metadata

if not settings['in_simulation']:
    from secop.client import SecopClient

from keithley2600_extend import Keithley2600ExtendFactory

###############################################################################
## does not work:
#
# out_dir = Path(settings['out_dir'])
# if not out_dir.exists():
    # out_dir.mkdir(parents=True)
# start_ts = datetime.now().strftime("%Y-%m-%d--%H-%M-%S.%f")
# log_file = out_dir / f'LOG-TBG_{start_ts}.log'
# logging.basicConfig(
    # level=logging.INFO,
    # format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    # handlers = [
        # logging.FileHandler(log_file, mode='w'),
        # logging.StreamHandler()
    # ],
    
# )
#
###############################################################################

log = logging.getLogger(__name__)

    

class ATBProcedure(Procedure):
    temperature_SP = FloatParameter('Temperature SP', units='K', default=300)
    temperature_ctrl_active = BooleanParameter('T control enable', default=not settings['in_simulation'])
    
    mag_field_SP = FloatParameter('Mag field SP', units='T', default=0)
    mag_field_ctrl_active = BooleanParameter('B control enable', default=not settings['in_simulation'])
    
    sample = Parameter('Sample', default='blank')
    
    starttime = Metadata('Start time', fget=datetime.now)
    
    secnode_connected = False  
    
    
    def startup(self):
        log.info(f"{str(self.__class__.__name__)}.startup()...")
        self.k_device = Keithley2600ExtendFactory(settings['keithley_address'], '')
        if not settings['in_simulation']:
            self.secnode = SecopClient(settings['secnode_address'])
        if self.temperature_ctrl_active or self.mag_field_ctrl_active:
            self.check_secnode()
            
             
    def shutdown(self):
        # nothing to do...
        pass
        
    ###########################################################################
    
    def check_secnode(self):
        if not self.secnode_connected:
            log.info("Connecting to secnode...")
            self.secnode.connect()
            self.secnode_connected = True
            log.info("... done")


    def get_temperature(self):
        if self.temperature_ctrl_active:
            self.check_secnode()
            return self.secnode.getParameter("tt", "value")[0]
        return -1.0
        
        
    def set_temperature(self, value):
        if self.temperature_ctrl_active:
            self.check_secnode()
            self.secnode.setParameter("tt", "target", value)
            log.info(f"T_SP = {value}K...")
        else:
            log.info("... T control disabled. Skipping setParameter()")
            
        
    def get_m_field(self):
        if self.mag_field_ctrl_active:
            self.check_secnode()
            return self.secnode.getParameter("mf", "value")[0]
        return -1.0
        
        
    def set_m_field(self, value):
        if self.mag_field_ctrl_active:
            self.check_secnode()
            self.secnode.setParameter("mf", "target", value)
            log.info(f"B_SP = {value}T...")
        else:
            log.info("... M control disabled. Skipping setParameter()")
        
        
    def is_temperature_stable(self):
        if self.temperature_ctrl_active:
            self.check_secnode()
            return (100 <= self.secnode.getParameter("tt", "status")[0][0] < 200)
        return True
        
        
    def is_m_field_stable(self):
        if self.mag_field_ctrl_active:
            self.check_secnode()
            return (100 <= self.secnode.getParameter("mf", "status")[0][0] < 200)
        return True
        
        
    def stabilize_TB(self):
        # stabilize T and B
        log.info(f"Trying to set T_SP = {self.temperature_SP}K...")
        self.set_temperature(self.temperature_SP)
        
        log.info(f"Trying to set B_SP = {self.mag_field_SP}T...")
        self.set_m_field(self.mag_field_SP)
        
        t_stable = False; m_stable = False
        while not (t_stable and m_stable):
            if self.should_stop():
                log.warning("Caught the stop flag in the procedure")
                return True
                
            sleep(1)
            if not t_stable:
                if self.is_temperature_stable():
                    log.info(f"Temperature stabilized ... T_SP = {self.temperature_SP}, T = {self.get_temperature()}")
                    t_stable = True
                else:
                    log.info(f"Waiting to stabilize temperature ... T_SP = {self.temperature_SP}, T = {self.get_temperature()}")
            if not m_stable:
                if self.is_m_field_stable():
                    log.info(f"Mag field stabilized ... B_SP = {self.mag_field_SP}, B = {self.get_m_field()}")
                    m_stable = True
                else:
                    log.info(f"Waiting to stabilize mag field ... B_SP = {self.mag_field_SP}, B = {self.get_m_field()}")
        
        if self.should_stop():
                log.warning("Caught the stop flag in the procedure")
                return True
                
        if self.temperature_ctrl_active or self.mag_field_ctrl_active:
            sleep_secs = np.max([settings['wait_after_stab_M'], settings['wait_after_stab_T']])
            log.info(f"Waiting another {sleep_secs}s...")
            sleep(sleep_secs)
        
        return False
        
###############################################################################
class TBGProcedure(ATBProcedure):
    drain_current_SP = FloatParameter('Id SP', units='A', default=10e-6)
    
    gate_voltage_min = FloatParameter('Vg min', units='V', default=0)
    gate_voltage_max = FloatParameter('Vg max', units='V', default=60)
    gate_voltage_step = FloatParameter('Vg step', units='V', default=0.25)
    
    DATA_COLUMNS = [
        'Index', 
        'Temperature SP', 
        'Mag field SP', 
        'Vg SP', 
        'Id SP', 
        'Temperature',
        'Mag field',
        'Vg direction fwd',
        'Vg',
        'Ig',
        'Vd',
        'Id',
        'Rds',
        'Sample'
    ]
    

    def execute(self):
        log.info(f"{str(self.__class__.__name__)}.execute()...")
        
        if self.stabilize_TB():
            return
        
        if self.should_stop():
            log.warning("Caught the stop flag in the procedure")
            return
                
        log.info(f"Starting Keithley measurement... B = {self.get_m_field()}, B_SP = {self.mag_field_SP}, T = {self.get_temperature()}, T_SP = {self.temperature_SP}")
        
        if settings['drain_channel'] == 'B':
            smu_gate = self.k_device.smua
            smu_drain = self.k_device.smub
        else:
            smu_gate = self.k_device.smub
            smu_drain = self.k_device.smua
        
        df = self.k_device.transfer_measurement_i(
            smu_gate = smu_gate,
            smu_drain = smu_drain,
            vg_start = self.gate_voltage_min,
            vg_stop = self.gate_voltage_max,
            vg_step = self.gate_voltage_step,
            id_list = [self.drain_current_SP],
            t_int = settings['keithley_integr_time'], # Must be between 0.001 to 25 times the power line frequency (50Hz or 60Hz).
            delay = -1, # automatically starts a measurement once the current is stable
            pulsed = False, # do not reset to zero between data points
        )
        df['Rds'] = df['Vd'] / df['Id']
        
        _t = self.get_temperature()
        _m = self.get_m_field()
        for index, row in df.iterrows():
            data = {
                'Index': index,
                'Temperature SP': self.temperature_SP, 
                'Mag field SP': self.mag_field_SP, 
                'Vg SP': row['Vg_SP'], 
                'Id SP': row['Id_SP'], 
                'Temperature': _t,
                'Mag field': _m,
                'Vg direction fwd': row['Vg_dir_fwd'],
                'Vg': row['Vg'],
                'Ig': row['Ig'],
                'Vd': row['Vd'],
                'Id': row['Id'],
                'Rds': row['Rds'],
                'Sample': self.sample
            }
            self.emit('results', data) # not possible to emit array...
            log.debug("Emitting results: %s" % data)
    
    
###############################################################################
class TBIVProcedure(ATBProcedure):
    drain_currennt_min = FloatParameter('Id min', units='A', default=0)
    drain_currennt_max = FloatParameter('Id max', units='A', default=100e-6)
    drain_currennt_step = FloatParameter('Id step', units='A', default=5e-6)
    
    DATA_COLUMNS = [
        'Index', 
        'Temperature SP', 
        'Mag field SP', 
        'Id SP', 
        'Temperature',
        'Mag field',
        'Vd',
        'Id',
        'Rds',
        'Sample'
    ]
    

    def execute(self):
        log.info(f"{str(self.__class__.__name__)}.execute()...")
        
        if self.stabilize_TB():
            return
        
        if self.should_stop():
            log.warning("Caught the stop flag in the procedure")
            return
                
        log.info(f"Starting Keithley measurement... B = {self.get_m_field()}, B_SP = {self.mag_field_SP}, T = {self.get_temperature()}, T_SP = {self.temperature_SP}")
        
        sweeplist_drain = np.arange(
            self.drain_currennt_min, 
            self.drain_currennt_max + self.drain_currennt_step, 
            self.drain_currennt_step
        )
        
        if settings['drain_channel'] == 'B':
            smu_drain = self.k_device.smub
        else:
            smu_drain = self.k_device.smua
        
        vdlist, idlist = self.k_device.current_sweep_single_smu(
            smu = smu_drain,
            smu_sweeplist = sweeplist_drain,
            t_int = settings['keithley_integr_time'], # Must be between 0.001 to 25 times the power line frequency (50Hz or 60Hz).
            delay = -1, # automatically starts a measurement once the current is stable
            pulsed = False, # do not reset to zero between data points
        )
        
        _t = self.get_temperature()
        _m = self.get_m_field()
        for index, (idsp, vd, id) in enum(zip(sweeplist_drain, vdlist, idlist)):
            data = {
                'Index': index,
                'Temperature SP': self.temperature_SP, 
                'Mag field SP': self.mag_field_SP, 
                'Id SP': idsp, 
                'Temperature': _t,
                'Mag field': _m,
                'Vd': vd,
                'Id': id,
                'Rds': vd / id,
                'Sample': self.sample
            }
            self.emit('results', data) # not possible to emit array...
            log.debug("Emitting results: %s" % data)
    
    