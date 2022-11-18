import logging
log = logging.getLogger(__name__)

import sys
from datetime import datetime
from pathlib import Path

from settings import settings

from pymeasure.display.Qt import QtWidgets
from pymeasure.display.windows import ManagedWindow
from pymeasure.experiment import Results, unique_filename

from procedures import TBIVProcedure

class MainWindow(ManagedWindow):
    def __init__(self):
        super().__init__(
            procedure_class=TBIVProcedure,
            inputs=[
                'sample',
                'temperature_SP',
                'temperature_ctrl_active',
                'mag_field_SP',
                'mag_field_ctrl_active',
                'drain_currennt_min',
                'drain_currennt_max',
                'drain_currennt_step'
            ],
            displays=[
                'temperature_SP',
                'mag_field_SP',
                'sample'
            ],
            x_axis='Id',
            y_axis='Vd',
            sequencer=True,
            sequencer_inputs=['temperature_SP', 'mag_field_SP'],
            sequence_file="TBIV.txt",
        )
        self.setWindowTitle('TBIV Experiment')


    def queue(self, procedure=None):
        if procedure is None:
            procedure = self.make_procedure()

        filename = unique_filename(
            directory=Path(settings['out_dir']), 
            procedure=procedure, 
            prefix='DATA-TBIV_{Sample}_T={Temperature SP}_B={Mag field SP}_',
        )
        
        log.info(f'Creating new experiment {procedure.__class__.__name__}. Output file: "{filename}"')
        results = Results(procedure, filename)
        experiment = self.new_experiment(results)

        self.manager.queue(experiment)


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
    