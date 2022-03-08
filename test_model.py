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


breakpoint()
from distgen_impact_cu_inj_ex.model import ImpactModel, ImpactConfiguration
from distgen_impact_cu_inj_ex import CU_INJ_MAPPING_TABLE



impact_config_filename = f"{dir_path}/distgen_impact_cu_inj_ex/files/lcls_injector/ImpactT.yaml"
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


impact_configuration = ImpactConfiguration(
    command=command, command_mpi=command_mpi, use_mpi=use_mpi, numprocs=numprocs, workdir=workdir, mpi_run=mpi_run
)


base_settings = {
    "header:Nx": header_nx,
    "header:Ny": header_ny,
    "header:Nz": header_nz,
    "stop": stop,
    "numprocs": numprocs,
}

archive_file="distgen_impact_cu_inj_ex/files/lcls_injector/archive.h5"

model = ImpactModel(archive_file=archive_file, configuration=impact_configuration, base_settings=base_settings)

input_variable_values = {"SOL1:solenoid_field_scale": 0.47235,
                        "CQ01:b1_gradient":  -0.00133705,
                        "SQ01:b1_gradient": 0.000769202,
                        "L0A_phase:dtheta0_deg": 0,
                        "L0B_phase:dtheta0_deg": -2.5,
                        "L0A_scale:voltage": 58,
                        "L0B_scale:voltage": 69.9586,
                        "QA01:b1_gradient": -3.25386,
                        "QA02:b1_gradient": 2.5843,
                        "QE01:b1_gradient": -1.54514,
                        "QE02:b1_gradient": -0.671809,
                        "QE03:b1_gradient": 3.22537,
                        "QE04:b1_gradient": -3.20496,
                        }


for var in input_variable_values:
     if CU_INJ_MAPPING_TABLE["impact_name"].str.contains(var, regex=False).any():
        val = input_variable_values[var] * CU_INJ_MAPPING_TABLE.loc[
                CU_INJ_MAPPING_TABLE["impact_name"] == var, "impact_factor"
            ].item()

        input_variable_values[var] = val


input_vars = model.input_variables
for var_name, var_value in input_variable_values.items():
    input_vars[var_name].value = var_value

output = model.evaluate(list(input_vars.values()), G.particles)
