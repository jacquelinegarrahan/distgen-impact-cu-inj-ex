import numpy as np
from distgen_impact_cu_inj_ex.utils import write_distgen_xy_dist, isolate_image
from distgen import Generator
import yaml
import os 
dir_path = os.path.dirname(os.path.realpath(__file__))



input_filename = f"{dir_path}/distgen_impact_cu_inj_ex/files/lcls_injector/distgen.yaml"
vcc_array = np.load(f"{dir_path}/distgen_impact_cu_inj_ex/files/default_vcc_array.npy").tolist()
vcc_size_y = 480
vcc_size_x = 640
output_filename = f"{dir_path}/distgen_impact_cu_inj_ex/files/output_file"
vcc_resolution = 9
vcc_resolution_units = "um"
n_particles = 1000
t_dist_len_value = 6.6

# Initialize distgen
vcc_array = np.array(vcc_array)
image = vcc_array.reshape(vcc_size_y, vcc_size_x)

# make units consistent
if vcc_resolution_units == "um/px":
    vcc_resolution_units = "um"

cutimg = isolate_image(image, fclip=0.08)
assert cutimg.ptp() > 0

write_distgen_xy_dist(
    output_filename, cutimg, vcc_resolution, resolution_units=vcc_resolution_units
)

G = Generator(input_filename)
G["total_charge:value"] =  2.5e-10 * 1.602176634e-07
G["t_dist:length:value"] = t_dist_len_value
G["n_particle"] = n_particles
G["t_dist:file"] = output_filename 
G.verbose = True

G.run()

from distgen_impact_cu_inj_ex import CU_INJ_MAPPING_TABLE

impact_settings_file = f"{dir_path}/distgen_impact_cu_inj_ex/files/lcls_injector/ImpactT.yaml"
workdir = f"{dir_path}/distgen_impact_cu_inj_ex/files/lcls_injector/output"
mpi_run="mpirun -n {nproc} --use-hwthread-cpus {command_mpi}"
command="/Users/jgarra/miniconda3/envs/lume-epics-impact/bin/ImpactTexe"
command_mpi="/Users/jgarra/miniconda3/envs/lume-epics-impact/bin/ImpactTexe-mpi"
numprocs=1
model_name="cu_inj"
timeout=10000
header_nx=32
header_ny=32
header_nz=32
stop=16.5
use_mpi=False
name="lume-impact-live-demo-cu-inj"



configuration = {
            "workdir": workdir,
            "mpi_run": mpi_run,
            "command": command,
            "command_mpi": command_mpi,
            "use_mpi": use_mpi,
            "timeout": timeout
        }


settings = {
        "header:Nx": header_nx,
        "header:Ny": header_ny,
        "header:Nz": header_nz,
        "stop": stop,
        "numprocs": numprocs,
    }



mappings = dict(
    zip(CU_INJ_MAPPING_TABLE["impact_name"], CU_INJ_MAPPING_TABLE["impact_factor"])
)

for key, val in mappings.items():
    if "distgen" not in key:
        settings[key] = val


from impact import Impact

I = Impact()
I = Impact(verbose=False)
I.load_archive("distgen_impact_cu_inj_ex/files/lcls_injector/archive1.h5")


I.initial_particles=G.particles

# ASSIGN SESTTINGS
for key, val in settings.items():
    I[key] = settings[key]


# ASSIGN SESTTINGS
for key, val in configuration.items():
    I[key] = configuration[key]



"""
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

        # REMOVE ALL DISTGEN
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

self._I.apply_settings(my_settings_dict)
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
"""