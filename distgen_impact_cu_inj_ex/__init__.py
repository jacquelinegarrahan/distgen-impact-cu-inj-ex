from pkg_resources import resource_filename
from lume_model.utils import variables_from_yaml
import pandas as pd

VARIABLE_FILE =resource_filename(
    "distgen_impact_cu_inj_ex.files", "variables.yml"
)

CU_INJ_MAPPING = resource_filename(
    "distgen_impact_cu_inj_ex.files", "cu_inj_impact.csv"
)

CU_INJ_MAPPING_TABLE= pd.read_csv(CU_INJ_MAPPING)
CU_INJ_MAPPING_TABLE.set_index("impact_name")


with open(VARIABLE_FILE, "r") as f:
    MODEL_INPUT_VARIABLES, MODEL_OUTPUT_VARIABLES = variables_from_yaml(f)
