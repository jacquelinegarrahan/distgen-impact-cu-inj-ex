from lume_model.models import SurrogateModel
from impact import evaluate_impact_with_distgen
from impact.tools import isotime
from impact.evaluate import  default_impact_merit
#import matplotlib.pyplot as plt
import numpy as np
import json
import pandas as pd
from time import sleep, time
import logging
import os
import yaml
import sys
from pkg_resources import resource_filename
import numpy as np
from distgen import Generator


CU_INJ_IMPACT_FILE = resource_filename(
    "distgen_impact_cu_inj_ex.files", "cu_inj_impact.csv"
)

# Gets or creates a logger
logger = logging.getLogger(__name__)  

class ImpactModel(SurrogateModel):
    # move configuration file parsing into utility
    def __init__(self, *, configuration, input_variables, output_variables):

        self.input_variables = input_variables
        self.output_variables = output_variables

        self._configuration = configuration
        self._model_name = configuration["impact"].get("model")
        self._pv_mapping = pd.read_csv(CU_INJ_MAPPING)

        self._pv_mapping.set_index("impact_name")

        self._settings = {
            'distgen:n_particle': self._configuration["distgen"].get('distgen:n_particle'),   
            'timeout': self._configuration["impact"].get('timeout'),
            'header:Nx': self._configuration["impact"].get('header:Nx'),
            'header:Ny': self._configuration["impact"].get('header:Ny'),
            'header:Nz': self._configuration["impact"].get('header:Nz'),
            'stop': self._configuration["impact"].get('stop'),
            'numprocs': self._configuration["machine"].get('num_procs'),
            'mpi_run': self._configuration["machine"].get('mpi_run_cmd'),
            'workdir': self._configuration["machine"].get('workdir'),
            'command': self._configuration["machine"].get('command'),
            'command_mpi': self._configuration["machine"].get('command_mpi'),
            'distgen:t_dist:length:value': self._configuration["distgen"].get('distgen:t_dist:length:value'),
        }

        # Update settings with impact factor
        self._settings.update(dict(zip(self._pv_mapping['impact_name'], self._pv_mapping['impact_factor'])))

        self._archive_dir = self._configuration["machine"].get("archive_dir")
        self._plot_dir = self._configuration["machine"].get("plot_output_dir")
        self._summary_dir = self._configuration["machine"].get("summary_output_dir")
        self._distgen_laser_file = self._configuration["distgen"].get("distgen_laser_file")
        self._distgen_input_file = self._configuration["distgen"].get("distgen_input_file")
        self._impact_config = impact_config
        self._workdir = self._configuration["machine"].get('workdir')


        # kind of odd workaround
        with open(self._configuration["machine"].get('config_file'), "r") as stream:
            try:
                impact_config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

        impact_config['use_mpi'] = self._configuration["machine"].get('use_mpi')
        impact_config['workdir'] = self._configuration["machine"].get('workdir')
        
    
        self._dashboard_kwargs = self._configuration.get("dashboard")


"""
    def evaluate(self, input_variables, particles, gen_input):
        if settings:
            for key in settings:
                val = settings[key]
                if key.startswith('distgen:'):
                    key = key[len('distgen:'):]
                    if verbose:
                        print(f'Setting distgen {key} = {val}')
                    G[key] = val
                else:
                # Assume impact
                    if verbose:
                        print(f'Setting impact {key} = {val}')          
                    I[key] = val                
        
        # Attach particles
        I.initial_particles = particles
        
        # Attach distgen input. This is non-standard. 
        I.distgen_input = gen_input
        
        I.run()

        output = default_impact_merit(I)
        print(output)

        # feels like this should be separate task...
        archive_impact_with_distgen(I, G, archive_file=archive_file)   


        return I








        I = run_impact_with_distgen(settings=self._settings, 
                            distgen_input_file=self._distgen_input_file,
                            impact_config=self._impact_config,
                            workdir=self._workdir,
                            verbose=verbose)
            
        if merit_f:
            output = merit_f(I)
        else:
            output = default_impact_merit(I)
        
        if 'error' in output and output['error']:
            
            raise ValueError('run_impact_with_distgen returned error in output')

        #Recreate Generator object for fingerprint, proper archiving
        # TODO: make this cleaner
        G = Generator()
        G.input = I.distgen_input
        
        fingerprint = fingerprint_impact_with_distgen(I, G)
        output['fingerprint'] = fingerprint
        
        if archive_path:
            path = tools.full_path(archive_path)
            assert os.path.exists(path), f'archive path does not exist: {path}'
            archive_file = os.path.join(path, fingerprint+'.h5')
            output['archive'] = archive_file
            
            # Call the composite archive method
            archive_impact_with_distgen(I, G, archive_file=archive_file)   
            
        return output    









        self._settings['distgen:xy_dist:file'] = distgen_laser_file

        itime = isotime()
        input_variables = {input_var.name: input_var for input_var in input_variables}

        # convert IMAGE vars 
        if input_variables["vcc_array"].value.ptp() < 128:
            downcast = input_variables["vcc_array"].value.astype(np.int8) 
            input_variables["vcc_array"].value = downcast

        if input_variables["vcc_array"].value.ptp() == 0:
            raise ValueError(f'vcc_array has zero extent')

        # scale values by impact factor
        vals = {}
        for var in input_variables.values():
            if var.name in self._pv_mapping["impact_name"]:
                vals[var.name] = var.value * self._pv_mapping.loc[self._pv_mapping["impact_name"] == var.name, "impact_factor"].item()

        df = self._pv_mapping.copy()
        df['pv_value'] = [input_variables[k].value for k in input_variables if "vcc_" not in k]


        dat = {'isotime': itime, 
            'inputs': self._settings, 'config': self._impact_config, 'pv_mapping_dataframe': df.to_dict()}


        logger.info(f'Running evaluate_impact_with_distgen...')

        t0 = time()

        dat['outputs'] = evaluate_impact_with_distgen(self._settings,
                                        #    merit_f=lambda x: run_merit(x, itime, self._dashboard_kwargs),
                                            archive_path=self._archive_dir,
                                            **self._impact_config,
                                            verbose=False)



        def imact(particles, gen_input)


        I = Impact.from_yaml(impact_config)    
    else:
        I = Impact(**impact_config)

    if workdir:
        I.workdir = workdir
        I.configure() # again
        
    I.verbose=verbose
    
    if settings:
        for key in settings:
            val = settings[key]
            if not key.startswith('distgen:'):

                if verbose:
                    print(f'Setting impact {key} = {val}')          
                I[key] = val                
    
    # Attach particles
    I.initial_particles = particles
    
    # Attach distgen input. This is non-standard. 
    I.distgen_input = gen_input
    
    I.run()


        logger.info(f'...finished in {(time()-t0)/60:.1f} min')

        for var_name in dat['outputs']:
            if var_name in self.output_variables:
                self.output_variables[var_name].value = dat['outputs'][var_name]

        self.output_variables["isotime"].value = dat["isotime"]

        # write summary file
        fname = fname=f"{self._summary_dir}/{self._model_name}-{dat['isotime']}.json"
        json.dump(dat, open(fname, 'w'))
        logger.info(f'Output written: {fname}')

        return list(self.output_variables.values())

    @staticmethod
    def _run_merit():
        ...

    @staticmethod
    def archive_model(filename):




... 
    def archive(self, h5=None):

        if not h5:
            h5 = 'impact_'+self.fingerprint()+'.h5'


        archive_run(h5, self.inital_particles, self.input, self.output, self._units, self.group)




def archive_run(h5, initial_particles, impact_input, impact_output, units, group):
    if isinstance(h5, str):
        fname = os.path.expandvars(h5)
        g = h5py.File(fname, 'w')
    else:
        g = h5


    # Write basic attributes
    archive.impact_init(g)

    # NEED TO CHANGE THIS HANDLING ALSO
    if initial_particles:
        initial_particles.write(g, name='initial_particles')

     # All input
    archive.write_input_h5(g, impact_input, name='input')

    # All output
    archive.write_output_h5(g, impact_output, name='output', units=units)

    # Control groups
    if group:
            archive.write_control_groups_h5(g, group, name='control_groups')
"""