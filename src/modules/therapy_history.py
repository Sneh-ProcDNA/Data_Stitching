from src.common.constants import treatment_codes
from src.common.db import engine
import pandas as pd
from pandas import DataFrame



sp_df = pd.read_sql("select * from data_dev.sp_komodo_core_base", engine)

def generate_treatment_flag(row, treatment_codes: list[str], mx: DataFrame):
    mx_patient_id = row['matched_patient_id']

    if pd.isna(mx_patient_id):
        return 0, 0

    codes = [str(c) for c in treatment_codes]

    mx_filtered = mx.loc[
        (mx['patient_id'] == mx_patient_id) &
        (mx['procedure_code'].astype(str).isin(codes))
    ]

    if mx_filtered.empty:
        return 0, 0

    return 1, int(len(mx_filtered))



