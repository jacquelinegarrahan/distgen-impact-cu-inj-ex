from pkg_resources import resource_filename
from lume_model.utils import variables_from_yaml
import pandas as pd

IMPACT_VARIABLE_FILE =resource_filename(
    "distgen_impact_cu_inj_ex.files", "variables.yml"
)

DISTGEN_VARIABLE_FILE =resource_filename(
    "distgen_impact_cu_inj_ex.files", "distgen_variables.yaml"
)

DEFAULT_VCC_ARRAY =resource_filename(
    "distgen_impact_cu_inj_ex.files", "default_vcc_array.npy"
)


DISTGEN_INPUT_FILE = resource_filename(
    "distgen_impact_cu_inj_ex.files", "distgen.yaml"
)

IMPACT_ARCHIVE_FILE =  resource_filename(
    "distgen_impact_cu_inj_ex.files", "archive.h5"
)


CU_INJ_MAPPING = resource_filename(
    "distgen_impact_cu_inj_ex.files", "cu_inj_impact.csv"
)

CU_INJ_MAPPING_TABLE= pd.read_csv(CU_INJ_MAPPING)
CU_INJ_MAPPING_TABLE.set_index("impact_name")


with open(IMPACT_VARIABLE_FILE, "r") as f:
    IMPACT_INPUT_VARIABLES, IMPACT_OUTPUT_VARIABLES = variables_from_yaml(f)


with open(DISTGEN_VARIABLE_FILE, "r") as f:
    DISTGEN_INPUT_VARIABLES, DISTGEN_OUTPUT_VARIABLES = variables_from_yaml(f)


DISTGEN_INPUT_VARIABLES["vcc_array"].default = DEFAULT_VCC_ARRAY
from . import _version
__version__ = _version.get_versions()['version']
