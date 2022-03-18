from impact.impact_distgen import archive_impact_with_distgen
from slac_services.services.scheduling import MongoDBResult
from slac_services import service_container
import numpy as np
import os
from prefect import Flow, task, Parameter
from prefect.storage import Docker
from prefect.engine.results import LocalResult
from distgen_impact_cu_inj_ex.model import ImpactModel, DistgenModel, LUMEConfiguration
from distgen_impact_cu_inj_ex.dashboard import make_dashboard

from distgen_impact_cu_inj_ex import (
    CU_INJ_MAPPING_TABLE,
    IMPACT_INPUT_VARIABLES,
    DISTGEN_INPUT_VARIABLES,
    DISTGEN_INPUT_FILE,
    IMPACT_ARCHIVE_FILE
)



# DEFINE PARAMETERS GLOBALLY FOR USE IN DYNAMIC RESULT STORAGE STRINGS
distgen_input_filename = Parameter("distgen_input_filename", default=DISTGEN_INPUT_FILE)
distgen_output_filename = Parameter("distgen_output_filename", default="/tmp/laser.txt")
distgen_settings = Parameter("distgen_settings")
distgen_configuration = Parameter("distgen_configuration")
distgen_pv_values = Parameter("distgen_pv_values")
distgen_pvname_to_input_map = Parameter("distgen_pvname_to_input_map")
impact_configuration = Parameter("impact_configuration")
impact_settings = Parameter("impact_settings")
impact_pv_values = Parameter("impact_pv_values")
impact_pvname_to_input_map = Parameter("impact_pvname_to_input_map")
impact_archive_file = Parameter("impact_archive_file", default=IMPACT_ARCHIVE_FILE)
archive_dir = Parameter("impact_archive_dir")
#  summary_dir = Parameter("impact_summary_dir")
pv_collection_isotime = Parameter("pv_collection_isotime")
dashboard_dir = Parameter("dashboard_dir")


PREFECT__CONTEXT__FLOW_ID = os.environ.get("PREFECT__CONTEXT__FLOW_ID", "local")



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
def archive_impact(distgen_output, impact_output, archive_dir, pv_collection_isotime):
    # get fingerprint
    distgen_model = distgen_output[0]
    impact_model = impact_output[0]
 #   fingerprint = fingerprint_impact_with_distgen(impact_model.I, distgen_model.G)

    archive_file = os.path.join(archive_dir, f"{PREFECT__CONTEXT__FLOW_ID}_{pv_collection_isotime}.h5")

    assert os.path.exists(archive_dir), f'archive dir does not exist: {archive_dir}'

    archive_impact_with_distgen(impact_model.I, distgen_model.G, archive_file)

    return archive_file    



@task
def create_dashboard(pv_collection_isotime, dashboard_dir, impact_output):

    DASHBOARD_KWARGS = {'outpath': dashboard_dir,
                'screen1': 'YAG02',
                'screen2': 'YAG03',
                'screen3': 'OTR2',
                'ylim' : (0, 2e-6), # Emittance scale                        
                'name' : f"{PREFECT__CONTEXT__FLOW_ID}_{pv_collection_isotime}"
                }

    plot_file = make_dashboard(impact_output[0].I, itime=pv_collection_isotime, **DASHBOARD_KWARGS)

    return plot_file


@task(result=MongoDBResult(model_type="impact", results_db=service_container.results_db()))
def store_results(pv_collection_isotime, impact_settings, impact_input_variables, impact_configuration, impact_output, dashboard_file, archive_file):

    impact_settings.update(
        {var_name: var.value for var_name, var in impact_input_variables.items()}
    )


    # output vars second item in tuple
    impact_outputs = {var.name: var.value for var in impact_output[1]}


    dat = {
        "flow_id": PREFECT__CONTEXT__FLOW_ID,
        "isotime": pv_collection_isotime,
        "inputs": impact_settings, 
        "config": impact_configuration,
        "outputs": impact_outputs,
        "plot_file": dashboard_file,
        "archive": archive_file
    }
    return dat


docker_storage = Docker(
    registry_url="jgarrahan",
    image_name="distgen-impact-cu-inj-ex",
    # path=os.path.dirname(__file__),
    build_kwargs={"nocache": True},
    dockerfile="Dockerfile",
    stored_as_script=True,
    path=f"/opt/prefect/flow.py",
)


with Flow(
    "distgen-impact-cu-inj",
    storage=docker_storage,
    #result=MongoDB...
) as flow:


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


    dashboard_file = create_dashboard(pv_collection_isotime, dashboard_dir, impact_output)
    archive_file = archive_impact(distgen_output, impact_output, archive_dir, pv_collection_isotime)
    store_results(pv_collection_isotime, impact_settings, impact_input_variables, impact_configuration, impact_output, dashboard_file, archive_file)


docker_storage.add_flow(flow)

def get_flow():
    return flow

