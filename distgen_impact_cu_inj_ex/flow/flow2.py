from distgen_impact_cu_inj_ex.utils import write_distgen_xy_dist, isolate_image
from distgen_impact_cu_inj_ex.model import ImpactModel, DistgenModel, LUMEConfiguration
from impact.impact_distgen import archive_impact_with_distgen
from impact.tools import isotime
import numpy as np
from prefect import Flow, task, Parameter
from prefect.storage import Docker
from distgen import Generator
from prefect.run_configs import KubernetesRun
import os
from distgen_impact_cu_inj_ex import (
    CU_INJ_MAPPING_TABLE,
    IMPACT_INPUT_VARIABLES,
    IMPACT_OUTPUT_VARIABLES,
    DISTGEN_INPUT_VARIABLES,
    DISTGEN_OUTPUT_VARIABLES,
)


@task
def format_distgen_epics_input(distgen_pv_values, distgen_pvname_to_input_map):
    input_variables = DISTGEN_INPUT_VARIABLES

    # scale all values w.r.t. impact factor
    for pv_name, value in distgen_pv_values.items():
        var_name = distgen_pvname_to_input_map[pv_name]

        # downcast
        if var_name == "vcc_array":
            value = np.array(value)
            value = value.astype(np.int8)

            if value.ptp() == 0:
                raise ValueError(f"EPICS get for vcc_array has zero extent")

        # make units consistent
        if var_name == "vcc_resolution_units":
            if value == "um/px":
                value = "um"

        if var_name == "total_charge":
            scaled_val = (
                value
                * CU_INJ_MAPPING_TABLE.loc[
                    CU_INJ_MAPPING_TABLE["impact_name"] == "distgen:total_charge:value", "impact_factor"
                ].item()
            )
            input_variables["total_charge"].value = scaled_val

        else:
            input_variables[var_name].value = value

    return input_variables


@task
def run_distgen(
    distgen_configuration,
    distgen_input_filename,
    distgen_settings,
    distgen_output_filename,
    distgen_input_variables,
):

    configuration = LUMEConfiguration(**distgen_configuration)
    distgen_model = DistgenModel(
        input_file=distgen_input_filename,
        configuration=configuration,
        base_settings=distgen_settings,
        distgen_output_filename=distgen_output_filename,
    )

    output_variables = distgen_model.evaluate(distgen_input_variables)

    return (distgen_model, output_variables)


@task
def format_impact_epics_input(impact_pv_values, impact_pvname_to_input_map):
    input_variables = IMPACT_INPUT_VARIABLES

    # scale all values w.r.t. impact factor
    for pv_name, value in impact_pv_values.items():
        var_name = impact_pvname_to_input_map[pv_name]

        if (
            CU_INJ_MAPPING_TABLE["impact_name"]
            .str.contains(var_name, regex=False)
            .any()
        ):
            scaled_val = (
                value
                * CU_INJ_MAPPING_TABLE.loc[
                    CU_INJ_MAPPING_TABLE["impact_name"] == var_name, "impact_factor"
                ].item()
            )

            input_variables[var_name].value = scaled_val

    return input_variables


@task
def run_impact(
    impact_archive_file,
    impact_configuration: dict,
    impact_settings: dict,
    input_variables,
    distgen_output: tuple,
):
    particles = distgen_output[0].get_particles()
    impact_configuration = LUMEConfiguration(**impact_configuration)

    model = ImpactModel(
        archive_file=impact_archive_file,
        configuration=impact_configuration,
        base_settings=impact_settings,
    )
    output_variables = model.evaluate(list(input_variables.values()), particles)

    return (model, output_variables)


@task
def archive(distgen_output, impact_output, archive_dir, model_id):
    # get fingerprint
    distgen_model = distgen_output[0]
    impact_model = impact_output[0]
 #   fingerprint = fingerprint_impact_with_distgen(impact_model.I, distgen_model.G)
    itime = isotime()

    archive_file = os.path.join(archive_dir, f"{str(model_id)}_{itime}.h5")

    assert os.path.exists(archive_dir), f'archive dir does not exist: {archive_dir}'

    archive_impact_with_distgen(impact_model.I, distgen_model.G, archive_file)

    return archive_file    
        

"""
# save summary file
@task
def summarize_impact(impact_settings, impact_configuration, impact_pv_values, impact_output, pv_collection_isotime, impact_model_name, summary_dir):
    skipping for now and saving to db instead
 
    df = CU_INJ_MAPPING_TABLE.copy()
   # df["pv_value"] = 

   # df["pv_value"] = [
   #     input_variables[k].value for k in input_variables if "vcc_" not in k
   # ]

   # df["impact_value"] = [impact_output_variables[""]]
    impact_output_variables = impact_output[1]

    settings = {
        var.name: var.value for var in impact_input_variables
    }.update(impact_settings)

    outputs = {}

    for var in impact_output_variables:
        outputs[var.name] =  var.value

    dat = {
        "isotime": pv_collection_isotime,
        "inputs": settings, 
        "config": impact_configuration,
        # "pv_mapping_dataframe": df.to_dict(),
        "outputs": outputs
    }

    fname = f"{summary_dir}/{impact_model_name}-{dat['isotime']}.json"

    # fname = fname = f"{self._summary_dir}/{self._model_name}-{dat['isotime']}.json"
    #json.dump(dat, open(fname, "w"))
    #logger.info(f"Output written: {fname}")

"""



with Flow(
    "distgen-impact-cu-inj",
) as flow:
    distgen_input_filename = Parameter("distgen_input_filename")
    distgen_output_filename = Parameter("distgen_output_filename")
    distgen_settings = Parameter("distgen_settings")
    distgen_configuration = Parameter("distgen_configuration")
    distgen_pv_values = Parameter("distgen_pv_values")
    distgen_pvname_to_input_map = Parameter("distgen_pvname_to_input_map")
    impact_configuration = Parameter("impact_configuration")
    impact_settings = Parameter("impact_settings")
    impact_pv_values = Parameter("impact_pv_values")
    impact_pvname_to_input_map = Parameter("impact_pvname_to_input_map")
    impact_archive_file = Parameter("impact_archive_file")
    archive_dir = Parameter("impact_archive_dir")
    model_id = Parameter("model_id")
  #  summary_dir = Parameter("impact_summary_dir")
  #  pv_collection_isotime = Parameter("pv_collection_isotime")


    distgen_input_variables = format_distgen_epics_input(distgen_pv_values, distgen_pvname_to_input_map)

    distgen_output = run_distgen(
        distgen_configuration,
        distgen_input_filename,
        distgen_settings,
        distgen_output_filename,
        distgen_input_variables
    )

    impact_input_variables = format_impact_epics_input(
        impact_pv_values, impact_pvname_to_input_map
    )
    impact_output = run_impact(
        impact_archive_file,
        impact_configuration,
        impact_settings,
        impact_input_variables,
        distgen_output
    )

    archive_file = archive(distgen_output, impact_output, archive_dir, model_id)

  #  summary_file = summarize_impact(impact_settings, impact_configuration, impact_pv_values, impact_output, pv_collection_isotime, impact_model_name, summary_dir)



if __name__ == "__main__":
    from impact.tools import isotime


    distgen_input_filename = f"/Users/jgarra/sandbox/distgen-impact-cu-inj-ex/distgen_impact_cu_inj_ex/files/lcls_injector/distgen.yaml"
    vcc_array = np.load(f"/Users/jgarra/sandbox/distgen-impact-cu-inj-ex/distgen_impact_cu_inj_ex/files/default_vcc_array.npy").tolist()
    distgen_output_filename = f"/Users/jgarra/sandbox/distgen-impact-cu-inj-ex/distgen_impact_cu_inj_ex/files/output_file.txt"


    distgen_pv_values = {
        "vcc_resolution" : 9,
        "vcc_resolution_units" : "um",
        "vcc_size_y" : 480,
        "vcc_size_x": 640,
        "vcc_array": np.array(vcc_array),
        "BPMS:IN20:221:TMIT": 1.51614e+09
    }

    distgen_configuration = {}
    distgen_settings = {
        'n_particle': 1000,
        "t_dist:length:value":  4 * 1.65   #  Inferred pulse stacker FWHM: 4 ps, converted to tukey length
    }

    distgen_pvname_to_input_map = {
        "vcc_resolution" : "vcc_resolution",
        "vcc_resolution_units" : "vcc_resolution_units",
        "vcc_size_y" : "vcc_size_y",
        "vcc_size_x": "vcc_size_x",
        "vcc_array": "vcc_array",
        "BPMS:IN20:221:TMIT":"total_charge"
    }


    workdir = "/Users/jgarra/sandbox/distgen-impact-cu-inj-ex/distgen_impact_cu_inj_ex/files/lcls_injector/output"
    mpi_run="mpirun -n {nproc} --use-hwthread-cpus {command_mpi}"
    command="/Users/jgarra/miniconda3/envs/lume-epics-impact/bin/ImpactTexe"
    command_mpi="/Users/jgarra/miniconda3/envs/lume-epics-impact/bin/ImpactTexe-mpi"
    use_mpi=False

    impact_configuration = {
        "command": command,
        "command_mpi": command_mpi,
        "use_mpi": use_mpi,
        "workdir": workdir,
        "mpi_run": mpi_run
    }

    impact_settings = {
        "header:Nx": 32,
        "header:Ny": 32,
        "header:Nz": 32,
      #  "stop": 16.5,
        "numprocs": 1,
       # "timeout": 10000
    }

    impact_archive_file="distgen_impact_cu_inj_ex/files/lcls_injector/archive.h5"

    impact_pv_values = {"SOL1:solenoid_field_scale": 0.47235,
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

    impact_pvname_to_input_map = {"SOL1:solenoid_field_scale": "SOL1:solenoid_field_scale",
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

    # FileOrCreate
    archive_dir = "/Users/jgarra/sandbox/distgen-impact-cu-inj-ex/output/archive"
    summary_dir = "/Users/jgarra/sandbox/distgen-impact-cu-inj-ex/output/summary"
    model_id = "1"


    data = {
        "distgen_input_filename": distgen_input_filename,
        "distgen_output_filename": distgen_output_filename,
        "distgen_settings": distgen_settings,
        "distgen_configuration": distgen_configuration,
        "distgen_pv_values": distgen_pv_values,
        "distgen_pvname_to_input_map": distgen_pvname_to_input_map,
        "impact_configuration": impact_configuration,
        "impact_pv_values": impact_pv_values,
        "impact_settings": impact_settings,
        "impact_pvname_to_input_map": impact_pvname_to_input_map,
        "impact_archive_file": impact_archive_file,
     #   "pv_collection_isotime": isotime(),
     #   "impact_model_name": "test",
        "impact_archive_dir": archive_dir,
     #   "impact_summary_dir": summary_dir
        "model_id": model_id
    }


    flow.run(**data)