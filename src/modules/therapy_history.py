from src.common.constants import treatment_codes
from src.common.db import engine
import pandas as pd
from pandas import DataFrame
from input import *


# sp_df = pd.read_sql("select * from data_dev.sp_komodo_core_base", engine)

def generate_treatment_flag(row, treatment_codes: list[str], mx: DataFrame):
    mx_patient_id = row[claims_patient_id_column_in_core_table]

    if pd.isna(mx_patient_id):
        return 0, 0

    codes = [str(c) for c in treatment_codes]

    mx_filtered = mx.loc[
        (mx[claims_patient_id_in_claims] == mx_patient_id) &
        (mx[therapy_column_in_claims].astype(str).isin(codes))
    ]

    if mx_filtered.empty:
        return 0, 0

    return 1, int(len(mx_filtered))



