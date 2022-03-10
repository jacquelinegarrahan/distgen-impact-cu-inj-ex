from distgen_impact_cu_inj_ex.utils import write_distgen_xy_dist, isolate_image
from distgen_impact_cu_inj_ex.model import ImpactModel, ImpactConfiguration
import numpy as np
from prefect import Flow, task, Parameter
from prefect.storage import Docker
from distgen import Generator
from prefect.run_configs import KubernetesRun
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
def run_distgen(
    vcc_array,
    vcc_size_y,
    vcc_size_x,
    vcc_resolution,
    vcc_resolution_units,
    distgen_input_filename,
    distgen_settings,
    distgen_output_filename,
):

    # Initialize distgen
    # vcc_array = distgen_input_variables["vcc_array"].value
    vcc_array = np.array(vcc_array)
    image = vcc_array.reshape(vcc_size_y, vcc_size_x)

    # make units consistent
    if vcc_resolution_units == "um/px":
        vcc_resolution_units = "um"

    cutimg = isolate_image(image, fclip=0.08)
    assert cutimg.ptp() > 0

    write_distgen_xy_dist(
        distgen_output_filename,
        cutimg,
        vcc_resolution,
        resolution_units=vcc_resolution_units,
    )

    # Run generator
    G = Generator(distgen_input_filename)

    G["t_dist:file"] = distgen_output_filename

    for setting, val in distgen_settings.items():
        G[setting] = val

    G.verbose = True

    G.run()

    return G


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
    G,
    archive_file,
    impact_configuration: dict,
    impact_base_settings: dict,
    input_variables,
):
    impact_configuration = ImpactConfiguration(**impact_configuration)

    model = ImpactModel(
        archive_file=archive_file,
        configuration=impact_configuration,
        base_settings=impact_base_settings,
    )
    output_variables = model.evaluate(list(input_variables.values()), G.particles)

    return model, output_variables


"""
def archive(G, I, output):
    # get fingerprint
    fingerprint = fingerprint_impact_with_distgen(I, G)
    output['fingerprint'] = fingerprint
        
    if archive_path:
        path = tools.full_path(archive_path)
        assert os.path.exists(path), f'archive path does not exist: {path}'
        archive_file = os.path.join(path, fingerprint+'.h5')
        output['archive'] = archive_file
        
        # Call the composite archive method
        archive_impact_with_distgen(I, G, archive_file=archive_file)   
"""


# save summary file
"""
# write summary file

    # build variable mapping dataframe
        # create dat
        df = self._mapping_table.copy()
        df["pv_value"] = [
            input_variables[k].value for k in input_variables if "vcc_" not in k
        ]
        df["impact_value"] = vals.values()


        dat = {
            "isotime": itime,
            "inputs": self._settings, 
            "config": self._impact_config,
            "pv_mapping_dataframe": df.to_dict(),
            "outputs": outputs
        }


# fname = fname = f"{self._summary_dir}/{self._model_name}-{dat['isotime']}.json"
#json.dump(dat, open(fname, "w"))
#logger.info(f"Output written: {fname}")

"""

"""
@task
def write_results()
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

    vcc_array = Parameter("vcc_array")
    vcc_size_y = Parameter("vcc_size_y")
    vcc_size_x = Parameter("vcc_size_x")
    vcc_resolution = Parameter("vcc_resolution")
    vcc_resolution_units = Parameter("vcc_resolution_units")
    distgen_input_filename = Parameter("distgen_input_filename")
    distgen_output_filename = Parameter("distgen_output_filename")
    distgen_settings = Parameter("distgen_settings")
    impact_configuration = Parameter("impact_configuration")
    impact_base_settings = Parameter("impact_base_settings")
    impact_pv_values = Parameter("impact_pv_values")
    impact_pvname_to_input_map = Parameter("impact_pvname_to_input_map")
    impact_archive_file = Parameter("impact_archive_file")

    g = run_distgen(
        vcc_array,
        vcc_size_y,
        vcc_size_x,
        vcc_resolution,
        vcc_resolution_units,
        distgen_input_filename,
        distgen_settings,
        distgen_output_filename,
    )

    input_variables = format_impact_epics_input(
        impact_pv_values, impact_pvname_to_input_map
    )
    run_impact(
        g,
        impact_archive_file,
        impact_configuration,
        impact_base_settings,
        input_variables,
    )


if __name__ == "__main__":

    import yaml
    from slac_services.services.scheduling import MountPoint
    from slac_services import service_container

    scheduler = service_container.prefect_scheduler()

    mount_point = MountPoint(name="fs-test", host_path="/Users/jgarra/sandbox", mount_type="Directory")

    flow_id = scheduler.register_flow(flow, "examples", build=True, mount_points=[mount_point], lume_configuration_file="/Users/jgarra/sandbox/lume-orchestration-demo/examples/distgen-impact-cu-inj/config.yaml")
    print(flow_id)
