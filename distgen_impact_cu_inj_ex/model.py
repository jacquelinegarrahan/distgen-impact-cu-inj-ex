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
from distgen_impact_cu_inj_ex import IMPACT_INPUT_VARIABLES, IMPACT_OUTPUT_VARIABLES, CU_INJ_MAPPING_TABLE

# Gets or creates a logger
logger = logging.getLogger(__name__)



class DistgenModel(SurrogateModel):

    def __init__(self, *, input_file, base_settings:dict={}):
        ...



    def evaluate():
        ...




# .yaml low-level configs
class ImpactConfiguration(BaseSettings):
    command: str
    command_mpi: str
    use_mpi: str
    workdir: str
    mpi_run: str


class ImpactModel(SurrogateModel):
    # move configuration file parsing into utility

    input_variables = IMPACT_INPUT_VARIABLES
    output_variables = IMPACT_OUTPUT_VARIABLES

    def __init__(
        self, *, archive_file: str, configuration: ImpactConfiguration, base_settings:dict={},
    ):
        self._archive_file = archive_file
        self._configuration = configuration.dict()
        self._settings = base_settings

        self._I = Impact(**self._configuration, verbose=False)
        self._I.load_archive(archive_file)

        # Assign basic settings
        for key, val in self._settings.items():
            val = self._settings[key]
            print(f"Setting {key}")
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
                self.output_variables[var_name].value = outputs[var_name]

        self.output_variables["isotime"].value = t1-t0

        return list(self.output_variables.values())