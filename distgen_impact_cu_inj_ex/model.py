from lume_model.models import SurrogateModel
from impact import Impact
from impact.tools import isotime
from impact.evaluate import default_impact_merit

# import matplotlib.pyplot as plt
import numpy as np
import json
import pandas as pd
from pydantic import BaseSettings
from time import time
import logging
from pkg_resources import resource_filename
import numpy as np
from distgen import Generator
from distgen_impact_cu_inj_ex import IMPACT_INPUT_VARIABLES, IMPACT_OUTPUT_VARIABLES, CU_INJ_MAPPING_TABLE, DISTGEN_INPUT_VARIABLES, DISTGEN_OUTPUT_VARIABLES
from distgen_impact_cu_inj_ex.utils import format_distgen_xy_dist, isolate_image
from typing import Optional

# Gets or creates a logger
logger = logging.getLogger(__name__)



# .yaml low-level configs
class LUMEConfiguration(BaseSettings):
    command: Optional[str]
    command_mpi: Optional[str]
    use_mpi: Optional[str]
    workdir: Optional[str]
    mpi_run: Optional[str]


class DistgenModel(SurrogateModel):

    input_variables = DISTGEN_INPUT_VARIABLES
    output_variables = DISTGEN_OUTPUT_VARIABLES

    def __init__(self, *, input_file, configuration, base_settings:dict=None, distgen_output_filename=None):
        self._input_file = input_file
        self._G = Generator(input_file, **configuration.dict())
        self._settings = base_settings
        self._configuration = configuration
        self._distgen_output_filename = distgen_output_filename

        if base_settings is not None:
            for setting, val in base_settings.items():
                self._G[setting] = val


    def evaluate(self, input_variables, settings:dict=None):

        # Assign updated settings
        if settings is not None:
            for key, val in settings.items():
                val = settings[key]
                self._G[key] = val


        image = input_variables["vcc_array"].value.reshape(input_variables["vcc_size_y"].value, input_variables["vcc_size_x"].value)
        

        cutimg = isolate_image(image, fclip=0.08)
        assert cutimg.ptp() > 0


        image_rep = format_distgen_xy_dist(cutimg,
            input_variables["vcc_resolution"].value,
            resolution_units=input_variables["vcc_resolution_units"].value)

        self._G['xy_dist']= image_rep


        self._G["total_charge:value"] = input_variables["total_charge"].value

        self._G.run()

        self.output_variables["x"].value = self._G.particles._data["x"]
        self.output_variables["px"].value = self._G.particles._data["px"]
        self.output_variables["y"].value = self._G.particles._data["y"]
        self.output_variables["py"].value = self._G.particles._data["py"]
        self.output_variables["z"].value = self._G.particles._data["z"]
        self.output_variables["pz"].value = self._G.particles._data["pz"]
        self.output_variables["t"].value = self._G.particles._data["t"]
        self.output_variables["status"].value = self._G.particles._data["status"]
        self.output_variables["weight"].value = self._G.particles._data["weight"]
        self.output_variables["species"].value = self._G.particles._data["species"]

        return list(self.output_variables.values())

    def get_particles(self):
        return self._G.particles

    @property
    def G(self):
        return self._G


class ImpactModel(SurrogateModel):
    # move configuration file parsing into utility

    input_variables = IMPACT_INPUT_VARIABLES
    output_variables = IMPACT_OUTPUT_VARIABLES

    def __init__(
        self, *, archive_file: str, configuration: LUMEConfiguration, base_settings:dict=None,
    ):
        self._archive_file = archive_file
        self._configuration = configuration.dict()
        self._settings = base_settings

        self._I = Impact(**self._configuration, verbose=False)
        self._I.load_archive(archive_file)

        # Assign basic settings
        if base_settings is not None:
            for key, val in self._settings.items():
                val = self._settings[key]
                self._I[key] = val


    def evaluate(self, input_variables, particles, settings:dict=None):
        self._I.initial_particles = particles

        # Assign updated settings
        if settings is not None:
            for key, val in settings.items():
                val = settings[key]
                self._I[key] = val

        # Assign input variable values
        for var in input_variables:
            self._I[var.name] = var.value

        logger.info(f"Running evaluate_impact_with_distgen...")

        t0 = time()

        self._I.run()

        logger.info(f"Completed execution in {(time()-t0)/60:.1f} min...")

        outputs = default_impact_merit(self._I)

        # format output variables
        for var_name in outputs:
            if var_name in self.output_variables:
                self.output_variables[var_name].value = outputs[var_name]

        return list(self.output_variables.values())

    
    @property
    def I(self):
        return self._I