from distgen_impact_cu_inj_ex.utils import write_distgen_xy_dist, isolate_image
from distgen_impact_cu_inj_ex.model import ImpactModel
import numpy as np
from prefect import Flow, task, Parameter
from prefect.storage import Docker
from distgen import Generator


@task
def run_distgen(
    vcc_array,
    vcc_size_y,
    vcc_size_x,
    vcc_resolution,
    vcc_resolution_units,
    input_filename,
    output_filename,
    t_dist_len_value,
    n_particles
):

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
    G["distgen:n_particle"] = n_particles
    G.verbose = True
    
    G.run()

    return G


# ... Save file?

@task
def run_impact(G, gen_input):
    print(G)
    ...


docker_storage = Docker(
    registry_url="jgarrahan", 
    image_name="distgen-impact-cu-inj-ex",
    image_tag="latest",
    # path=os.path.dirname(__file__),
    # build_kwargs = {"nocache": True},
    stored_as_script=True,
    path=f"/opt/prefect/flow.py",
)



with Flow(
        "test-distgen",
        storage = docker_storage,
        run_config=KubernetesRun(image="jgarrahan/distgen-impact-cu-inj-ex", image_pull_policy="Always"),
    ) as flow:


    vcc_array = Parameter("vcc_array")
    vcc_size_y = Parameter("vcc_size_y")
    vcc_size_x = Parameter("vcc_size_x")
    vcc_resolution = Parameter("vcc_resolution")
    vcc_resolution_units = Parameter("vcc_resolution_units")
    output_filename = Parameter("output_filename")
    input_filename = Parameter("input_filename")
    t_dist_len_value = Parameter("distgen:t_dist:length:value")
    n_particles = Parameter("distgen:n_particle")

    g = run_distgen(
        vcc_array,
        vcc_size_y,
        vcc_size_x,
        vcc_resolution,
        vcc_resolution_units,
        input_filename,
        output_filename,
        t_dist_len_value,
        n_particles
    )

    run_impact(g, "dummy")




def archive_result():
    ...


if __name__ == "__main__":
    input_filename = "/Users/jgarra/sandbox/lume-epics-impact/examples/local_lcls/lcls_injector/distgen.yaml"
    vcc_array = np.load("distgen_impact_cu_inj_ex/files/default_image_array.npy")
    vcc_size_y = 480
    vcc_size_x = 640
    output_filename = "output_file"
    vcc_resolution = 9
    vcc_resolution_units = "um"
    n_particles = 100
    t_dist_len_value = 6.6

    from slac_services import service_container

    scheduler = service_container.prefect_scheduler()
    scheduler.create_project("examples")
    scheduler.register_flow(flow, "examples")


    """

    flow.run(
        {
            "vcc_array": vcc_array,
            "vcc_size_y": vcc_size_y,
            "vcc_size_x": vcc_size_x,
            "vcc_resolution": vcc_resolution,
            "vcc_resolution_units": vcc_resolution_units,
            "input_filename": input_filename,
            "output_filename": output_filename,
            "distgen:t_dist:length:value": t_dist_len_value,
            "distgen:n_particle": n_particles
        }
    )
    """