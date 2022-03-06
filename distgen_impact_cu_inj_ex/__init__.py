from pkg_resources import resource_filename
import pandas as pd

VARIABLE_FILE =resource_filename(
    "distgen_impact_cu_inj_ex.files", "variables.yml"
)

CU_INJ_MAPPING = resource_filename(
    "distgen_impact_cu_inj_ex.files", "cu_inj_impact.csv"
)
CU_INJ_MAPPING_TABLE= pd.read_csv(CU_INJ_MAPPING)
CU_INJ_MAPPING_TABLE.set_index("impact_name")