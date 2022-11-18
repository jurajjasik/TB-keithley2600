settings = {
    'out_dir': 'data',
       
    'keithley_address': 'TCPIP0::192.168.0.44::INSTR',
    'drain_channel': 'B', # either 'A' or 'B'.
    'keithley_integr_time': 20e-3,  # Seconds. Must be between 0.001 to 25 times 
                                    # the power line periode (20 ms for 50Hz).
    
    'secnode_address': 'kfes38.troja.mff.cuni.cz:5000',
    'wait_after_stab_M': 10, # Seconds
    'wait_after_stab_T': 10, # Seconds
    
    'in_simulation': True, # affects secop.client import, default values of 'T control active' and 'B control active'
}
