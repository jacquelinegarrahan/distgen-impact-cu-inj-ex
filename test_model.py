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
n_particles = 100
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
G["distgen:t_dist:length:value"] = t_dist_len_value
G["n_particle"] = n_particles
#G["xy_dist:file"] = output_filename BROKEN
G.verbose = True

G.run()



from distgen_impact_cu_inj_ex.model import ImpactModel

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


with open(impact_config_filename, "r") as stream:
    try:
        impact_config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)


model = ImpactModel(
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
)

input_vars = model.input_variables
for var_name, var  in input_vars.items():
    input_vars[var_name].value = var.default

model.evaluate(list(input_vars.values()), G.particles, G.input)