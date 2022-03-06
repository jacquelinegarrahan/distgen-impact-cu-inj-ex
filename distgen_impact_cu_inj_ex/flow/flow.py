from distgen_impact_cu_inj_ex.utils import write_distgen_xy_dist, isolate_image
from distgen_impact_cu_inj_ex.model import ImpactModel
import numpy as np
from prefect import Flow, task, Parameter
from prefect.storage import Docker
from distgen import Generator
from prefect.run_configs import KubernetesRun
from distgen_impact_cu_inj_ex import CU_INJ_MAPPING_TABLE

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
    G["distgen:xy_dist:file"] = output_filename


    mappings = dict(
        zip(CU_INJ_MAPPING_TABLE["impact_name"], CU_INJ_MAPPING_TABLE["impact_factor"])
    )

    for key, val in mappings.items():
        if "distgen"  in key:
            self._settings[key] = val


    G.verbose = True
    
    G.run()

    return G


"""
def load_impact_config(impact_config_filename):
        # NEED TO GET impact config another way
        with open(impact_config_filename, "r") as stream:
            try:
                impact_config = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)

    return impact_config
"""

  #      self._archive_dir = self._configuration["machine"].get("archive_dir")
  #      self._plot_dir = self._configuration["machine"].get("plot_output_dir")
  #      self._summary_dir = self._configuration["machine"].get("summary_output_dir")

@task
def run_impact(G, gen_input):
    print(G)
    #    ...

  #  I = model.get_impact_obj()

    return "done"


# build dashboard

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
       # fname = fname = f"{self._summary_dir}/{self._model_name}-{dat['isotime']}.json"
        #json.dump(dat, open(fname, "w"))
        #logger.info(f"Output written: {fname}")

"""




docker_storage = Docker(
    registry_url="jgarrahan", 
    image_name="distgen-impact-cu-inj-ex",
    image_tag="latest",
    # path=os.path.dirname(__file__),
    # build_kwargs = {"nocache": True},
    dockerfile="Dockerfile",
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

if __name__ == "__main__":

    import yaml

    with open("/Users/jgarra/sandbox/lume-orchestration-demo/slac_services/files/kubernetes_job.yaml", "r") as stream:
        try:
            yaml_stream = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)


    print(yaml_stream)


    flow.run_config = KubernetesRun(image="jgarrahan/distgen-impact-cu-inj-ex", image_pull_policy="Always", job_template=yaml_stream)


    flow_id = flow.register(project_name="examples")
#print(flow_id)