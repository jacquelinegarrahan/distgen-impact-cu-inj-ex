from lume_model.models import SurrogateModel
from impact import Impact
from impact.tools import isotime
from impact.evaluate import default_impact_merit

# import matplotlib.pyplot as plt
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
from distgen_impact_cu_inj_ex import VARIABLE_FILE
from lume_model.utils import variables_from_yaml
from distgen_impact_cu_inj_ex import CU_INJ_MAPPING_TABLE

# Gets or creates a logger
logger = logging.getLogger(__name__)


class ImpactModel(SurrogateModel):
    # move configuration file parsing into utility
    def __init__(
        self,
        impact_config,
        model_name,
        timeout,
        header_nx,
        header_ny,
        header_nz,
        stop,
        numprocs,
        mpi_run,
        workdir,
        command,
        command_mpi,
        use_mpi
    ):
        with open(VARIABLE_FILE, "r") as f:
            self.input_variables, self.output_variables = variables_from_yaml(f)

        self._model_name = model_name
        self._mapping_table = CU_INJ_MAPPING_TABLE
        self._mapping_table.set_index("impact_name")

        self._settings = {
            "timeout": timeout,
            "header:Nx": header_nx,
            "header:Ny": header_ny,
            "header:Nz": header_nz,
            "stop": stop,
            "numprocs": numprocs,
            "mpi_run": mpi_run,
            "workdir": workdir,
            "command": command,
            "command_mpi": command_mpi,
        }


        mappings = dict(
            zip(self._mapping_table["impact_name"], self._mapping_table["impact_factor"])
        )

        for key, val in mappings.items():
            if "distgen" not in key:
                self._settings[key] = val

        impact_config["use_mpi"] = use_mpi
        impact_config["workdir"] = workdir

        self._impact_config = impact_config

        self._I = Impact(**self._impact_config)

    def evaluate(self, input_variables, particles, gen_input, debug=False):

        self._I = Impact(**self._impact_config, initial_particles=particles)

        for key, val in self._settings.items():
            val = self._settings[key]
            self._I[key] = val

        if debug:
            self._I.total_charge = 0


        # prepare
        itime = isotime()
        input_variables = {input_var.name: input_var for input_var in input_variables}

        # convert IMAGE vars
        if input_variables["vcc_array"].value.ptp() < 128:
            downcast = input_variables["vcc_array"].value.astype(np.int8)
            input_variables["vcc_array"].value = downcast

        if input_variables["vcc_array"].value.ptp() == 0:
            raise ValueError(f"vcc_array has zero extent")

        # scale values by impact factor
        vals = {}
        for var in input_variables.values():
            if self._mapping_table["impact_name"].str.contains(var.name, regex=False).any():
                val = var.value * self._mapping_table.loc[
                        self._mapping_table["impact_name"] == var.name, "impact_factor"
                    ].item()

                vals[var.name] = val
                if not "distgen" in var.name:
                    self._I[var.name] = val


        # create dat
        df = self._mapping_table.copy()
        df["pv_value"] = [
            input_variables[k].value for k in input_variables if "vcc_" not in k
        ]
        df["impact_value"] = vals.values()

        logger.info(f"Running evaluate_impact_with_distgen...")

        t0 = time()

        self._I.run()

        t1 = time()

        if not self._I.output.get("final_particles") and not self._I.output['run_info']['error']:
            logger.error("Beam loss in simulation likely preventing output.")

        outputs = default_impact_merit(self._I)

        dat = {
            "isotime": itime,
            "inputs": self._settings,
            "config": self._impact_config,
            "pv_mapping_dataframe": df.to_dict(),
            "outputs": outputs
        }

        for var_name in dat["outputs"]:
            if var_name in self.output_variables:
                self.output_variables[var_name].value = dat["outputs"][var_name]

        self.output_variables["isotime"].value = dat["isotime"]

        self._dat = dat

        return list(self.output_variables.values())

    def get_impact_objs(self):
        return self._I

    def get_dat(self):
        return self._dat

    @classmethod
    def from_archive(self, archive_file):
        I2 = Impact(verbose=False)
        I2.load_archive(afile)


     #   impact_config,
     #   model_name,
     #   timeout,
     #   header_nx,
     #   header_ny,
     #   header_nz,
     #   stop,
     #   numprocs,
     #   mpi_run,
     #   workdir,
     #   command,
     #   command_mpi,
     #   use_mpi




