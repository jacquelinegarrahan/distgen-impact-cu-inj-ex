from distgen_impact_cu_inj_ex.utils import write_distgen_xy_dist, isolate_image
from distgen_impact_cu_inj_ex.model import ImpactModel
import numpy as np
from prefect import Flow, task, Parameter
from prefect.storage import Docker
from distgen import Generator
from prefect.run_configs import KubernetesRun


if __name__ == "__main__":
    distgen_input_filename = "/Users/jgarra/sandbox/lume-orchestration-demo/examples/distgen-impact-cu-inj/distgen.yaml"
    vcc_array = np.load("/Users/jgarra/sandbox/distgen-impact-cu-inj-ex/distgen_impact_cu_inj_ex/files/default_vcc_array.npy").tolist()
    vcc_size_y = 480
    vcc_size_x = 640
    output_filename = "/Users/jgarra/sandbox/lume-orchestration-demo/examples/distgen-impact-cu-inj/output_file"
    vcc_resolution = 9
    vcc_resolution_units = "um"

    distgen_settings = {
        "n_particle": 1000,
        "t_dist:length:value": 6.6
    }

    impact_configuration = {
        "workdir":  "/Users/jgarra/sandbox/distgen-impact-cu-inj-ex/distgen_impact_cu_inj_ex/files/lcls_injector/output",
        "mpi_run": "mpirun -n {nproc} --use-hwthread-cpus {command_mpi}",
        "command": "/Users/jgarra/miniconda3/envs/lume-epics-impact/bin/ImpactTexe",
        "command_mpi": "/Users/jgarra/miniconda3/envs/lume-epics-impact/bin/ImpactTexe-mpi",
        "numprocs": 1,
        "use_mpi": False
    }

    impact_base_settings = {
        "header:Nx": header_nx,
        "header:Ny": header_ny,
        "header:Nz": header_nz,
        "stop": stop,
        "numprocs": numprocs,
    }

    impact_archive_file = "distgen_impact_cu_inj_ex/files/lcls_injector/archive.h5"

    pv_values = {"SOL1:solenoid_field_scale": 0.47235,
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
    pvname_to_input_map = {"SOL1:solenoid_field_scale": "SOL1:solenoid_field_scale",
                        "CQ01:b1_gradient": "CQ01:b1_gradient",
                        "SQ01:b1_gradient": "SQ01:b1_gradient",
                        "L0A_phase:dtheta0_deg": "L0A_phase:dtheta0_deg",
                        "L0B_phase:dtheta0_deg": "L0B_phase:dtheta0_deg",
                        "L0A_scale:voltage": "L0A_scale:voltage",
                        "L0B_scale:voltage": "L0B_scale:voltage",
                        "QA01:b1_gradient": "QA01:b1_gradient",
                        "QA02:b1_gradient": "QA02:b1_gradient",
                        "QE01:b1_gradient": "QE01:b1_gradient",
                        "QE02:b1_gradient": "QE02:b1_gradient",
                        "QE03:b1_gradient": "QE03:b1_gradient",
                        "QE04:b1_gradient": "QE04:b1_gradient",
                        }

    from slac_services import service_container
    scheduler=service_container.prefect_scheduler()

    flow_id = "380a75be-c057-4123-80e5-bfdc69a8b361"

    scheduler.schedule_run( flow_id, 
        {
            "vcc_array": vcc_array,
            "vcc_size_y": vcc_size_y,
            "vcc_size_x": vcc_size_x,
            "vcc_resolution": vcc_resolution,
            "vcc_resolution_units": vcc_resolution_units,
            "distgen_input_filename": input_filename,
            "distgen_settings": distgen_settings,
            "impact_configuration": impact_configuration,
            "impact_archive_file": impact_archive_file,
            "impact_base_settings": impact_base_settings,
            "pv_values": pv_values,
            "pvname_to_input_map": pvname_to_input_map,
        }
    )
