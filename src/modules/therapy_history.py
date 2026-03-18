from src.common.constants import treatment_codes
from src.common.db import engine
import pandas as pd
from pandas import DataFrame
from input import *


# sp_df = pd.read_sql("select * from data_dev.sp_komodo_core_base", engine)

def generate_treatment_flag(row, treatment_codes: list[str], mx: DataFrame):
    """
    Original row-level function — retained for reference or single-row use.
    Do NOT use this inside .apply() over a large DataFrame; use
    generate_treatment_flags_vectorized() instead.
    """
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


def generate_treatment_flags_vectorized(sp_df: DataFrame, treatment_codes_map: dict, mx: DataFrame) -> DataFrame:
    """
    Vectorized replacement for the row-by-row apply(generate_treatment_flag) loop.

    OLD approach (slow):
        for treatment, codes in treatment_codes.items():
            sp_therapy_df[[f'{treatment}_flag', f'{treatment}_freq']] = sp_therapy_df.apply(
                generate_treatment_flag, axis=1, args=(codes, mx_df), result_type='expand'
            )

    WHY it was slow:
        - .apply() calls generate_treatment_flag once per row.
        - Inside each call, mx_df is fully scanned with .loc[] to find matching patient rows.
        - For N rows in sp_df and M rows in mx_df, this is O(N × M) — extremely slow at scale.

    NEW approach (fast):
        - Pre-filter mx once per treatment to only rows with matching procedure codes.
        - groupby patient_id to get per-patient counts — O(M) once.
        - merge result into sp_df — O(N log N).
        - Total: O(M + N log N) per treatment instead of O(N × M).
    """
    result_df = sp_df.copy()

    # Cast therapy column once outside the loop
    mx_therapy_col = mx[therapy_column_in_claims].astype(str)

    for treatment, codes in treatment_codes_map.items():
        codes_str = set(str(c) for c in codes)

        # Step 1: filter mx to only rows matching this treatment's codes — done ONCE
        mx_filtered = mx.loc[mx_therapy_col.isin(codes_str), [claims_patient_id_in_claims]].copy()

        # Step 2: count occurrences per patient — O(M) groupby
        freq_series = (
            mx_filtered
            .groupby(claims_patient_id_in_claims)
            .size()
            .rename(f'{treatment}_freq')
            .reset_index()
        )

        # Step 3: merge into result_df — O(N log N)
        result_df = result_df.merge(
            freq_series,
            how='left',
            left_on=claims_patient_id_column_in_core_table,
            right_on=claims_patient_id_in_claims
        )

        # Drop the duplicate patient_id column from the right side if it appeared
        if claims_patient_id_in_claims in result_df.columns and claims_patient_id_in_claims != claims_patient_id_column_in_core_table:
            result_df = result_df.drop(columns=[claims_patient_id_in_claims])

        # Step 4: derive flag from freq (0 freq or NaN → flag=0)
        result_df[f'{treatment}_freq'] = result_df[f'{treatment}_freq'].fillna(0).astype(int)
        result_df[f'{treatment}_flag'] = (result_df[f'{treatment}_freq'] > 0).astype(int)

    return result_df


