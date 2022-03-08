from lume_model.models import SurrogateModel
from impact import Impact
from impact.tools import isotime
from impact.evaluate import default_impact_merit

# import matplotlib.pyplot as plt
import numpy as np
import json
import pandas as pd
from pydantic import BaseSettings
from time import sleep, time
import logging
import os
import yaml
import sys
from pkg_resources import resource_filename
import numpy as np
from distgen import Generator
from distgen_impact_cu_inj_ex import MODEL_INPUT_VARIABLES, MODEL_OUTPUT_VARIABLES, CU_INJ_MAPPING_TABLE

# Gets or creates a logger
logger = logging.getLogger(__name__)




# .yaml low-level configs
class ImpactConfiguration(BaseSettings):
    command: str
    command_mpi: str
    use_mpi: str
    numprocs: str
    workdir: str
    mpi_run: str


class ImpactModel(SurrogateModel):
    # move configuration file parsing into utility

    input_variables = MODEL_INPUT_VARIABLES
    output_variables = MODEL_OUTPUT_VARIABLES

    def __init__(
        self, *, archive_file: str,configuration: ImpactConfiguration, base_settings:dict={},
    ):
        self._archive_file = archive_file
        self._configuration = configuration


        self._settings = base_settings
        
        """
        {
            "header:Nx": header_nx,
            "header:Ny": header_ny,
            "header:Nz": header_nz,
            "stop": stop,
            "numprocs": numprocs,
        }
        """

       # for key, val in mappings.items():
        #    self._settings[key] = val

        self._I = Impact(verbose=False)
        self._I.load_archive(archive_file)

        # Assign basic settings
        for key, val in self._settings.items():
            val = self._settings[key]
            self._I[key] = val


    def evaluate(self, input_variables, particles, settings={}):
        self._I.initial_particles = particles

        # Assign updated settings
        for key, val in settings.items():
            val = settings[key]
            self._I[key] = val

        # Assign input variable values
        for var in input_variables:
            self._I[var.name] = var.value


        # prepare
        itime = isotime()

        logger.info(f"Running evaluate_impact_with_distgen...")

        t0 = time()

        self._I.run()

        t1 = time()

        outputs = default_impact_merit(self._I)

        # format output variables
        for var_name in outputs:
            if var_name in self.output_variables:
                self.output_variables[var_name].value = dat["outputs"][var_name]

            else:
                print("OUTPUT NOT IN")
                print(var_name)

        self.output_variables["isotime"].value = t1-t0

        return list(self.output_variables.values())