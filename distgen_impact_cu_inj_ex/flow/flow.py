from distgen_impact_cu_inj_ex.utils import write_distgen_xy_dist, isolate_image
from distgen_impact_cu_inj_ex.model import ImpactModel, DistgenModel, LUMEConfiguration
from impact.impact_distgen import archive_impact_with_distgen
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
def archive(distgen_output, impact_output, archive_dir, model_id, pv_collection_isotime):
    # get fingerprint
    distgen_model = distgen_output[0]
    impact_model = impact_output[0]
 #   fingerprint = fingerprint_impact_with_distgen(impact_model.I, distgen_model.G)

    archive_file = os.path.join(archive_dir, f"{str(model_id)}_{pv_collection_isotime}.h5")

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


docker_storage = Docker(
    registry_url="jgarrahan",
    image_name="distgen-impact-cu-inj-ex",
    image_tag="latest",
    # path=os.path.dirname(__file__),
    build_kwargs={"nocache": True},
    dockerfile="Dockerfile",
    stored_as_script=True,
    path=f"/opt/prefect/flow.py",
)


with Flow(
    "distgen-impact-cu-inj",
    storage=docker_storage,
    run_config=KubernetesRun(
        image="jgarrahan/distgen-impact-cu-inj-ex", image_pull_policy="Always"
    ),
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
    pv_collection_isotime = Parameter("pv_collection_isotime")


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

    archive_file = archive(distgen_output, impact_output, archive_dir, model_id, pv_collection_isotime)

  #  summary_file = summarize_impact(impact_settings, impact_configuration, impact_pv_values, impact_output, pv_collection_isotime, impact_model_name, summary_dir)


docker_storage.add_flow(flow)

def get_flow():
    return flow



if __name__ == "__main__":

    import yaml
    from slac_services.services.scheduling import MountPoint
    from slac_services import service_container

    scheduler = service_container.prefect_scheduler()

    mount_point = MountPoint(
        name="fs-test", host_path="/Users/jgarra/sandbox", mount_type="Directory"
    )

    flow_id = scheduler.register_flow(
        flow,
        "examples",
        build=False,
        mount_points=[mount_point],
        lume_configuration_file="/Users/jgarra/sandbox/lume-orchestration-demo/examples/distgen-impact-cu-inj/config.yaml",
    )
    print(flow_id)
