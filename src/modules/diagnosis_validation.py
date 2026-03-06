import re
from pandas import DataFrame
from datetime import datetime, timedelta
from src.common.db import *
from input import *

def get_icd_code_from_data(patient_id, sp_data):
    icd_codes_per_patient: list[str] = (
                                            sp_data.loc[sp_data[claims_patient_id_in_claims] == patient_id, 'primary_icd_code']
                                            .dropna()
                                            .unique()
                                        )

    for code in icd_codes_per_patient:
        # if code.startswith(parent_icd_code):
            return code.replace('.', '')




def get_icd_code_from_claims(row, claims):
    patient_id = row[claims_patient_id_column_in_core_table]
    sp_diag_code = row[sp_icd_code_column]

    if pd.isna(patient_id) or pd.isna(sp_diag_code):
        return None

    sp_diag_code = str(sp_diag_code)
    parent = sp_diag_code[:3] 

    dx_values = (
        claims.loc[claims[claims_patient_id_in_claims] == patient_id, claims_icd_code_column]
        .dropna()
        .unique()
    )

    for dx in dx_values:
        dx_str = str(dx)
        if sp_diag_code in dx_str:
            return sp_diag_code

    pattern = re.compile(rf"{re.escape(parent)}(?:\.\d+|\d+)?")
    for dx in dx_values:
        dx_str = str(dx)
        m = pattern.search(dx_str)
        if m:
            return m.group(0).replace('.', '')

    return None


def generate_diagnosis_flags(row):
    exact_sp_diag_code = row[sp_icd_code_column]
    exact_claims_diag_code = row[exact_claims_diag_code]

    if pd.isna(exact_claims_diag_code) or pd.isna(exact_sp_diag_code):
        return -1, -1

    parent_sp_diag_code = str(exact_sp_diag_code)[:3]
    parent_claims_diag_code = str(exact_claims_diag_code)[:3]

    exact_match = 1 if exact_sp_diag_code == exact_claims_diag_code else 0
    parent_match = 1 if parent_sp_diag_code == parent_claims_diag_code else 0

    return parent_match, exact_match


    

def calculate_diag_freq(row, mx_df):
    claims_patient_id = row[claims_patient_id_column_in_core_table]
    parent_diag_code_flag = row['parent_diag_code_flag']
    exact_diag_code_flag = row['exact_diag_code_flag']
    icd_code_claims = row[exact_claims_diag_code]

    freq = 0

    if parent_diag_code_flag == -1 or exact_diag_code_flag == -1 or pd.isna(icd_code_claims):
        return -1

    if exact_diag_code_flag == 1 and pd.notna(claims_patient_id) and pd.notna(icd_code_claims):
        filtered_df = mx_df.loc[mx_df[claims_patient_id_in_claims] == claims_patient_id]
        icd_codes = filtered_df[claims_icd_code_column].dropna().astype(str).values

        target = str(icd_code_claims)
        for code in icd_codes:
            if code.find(target) != -1:
                freq += 1

    return freq







def generate_diagnosis_lookback(row, sp_data: DataFrame, mx_df):
    sp_patient_id = row[sp_patient_id_column_in_core_table]
    claims_patient_id = row[claims_patient_id_column_in_core_table]
    icd_code = row[sp_icd_code_column]
    exact_diag_code_flag = row['exact_diag_code_flag']

    if exact_diag_code_flag == -1 or pd.isna(icd_code):
        return -1, -1, -1

    if exact_diag_code_flag != 1 or pd.isna(sp_patient_id) or pd.isna(claims_patient_id) or pd.isna(icd_code):
        return 0, 0, 0
    
    sp_ref_dates = pd.to_datetime(
        sp_data.loc[sp_data[claims_patient_id_in_claims] == sp_patient_id, 'referral_date'],
        errors='coerce'
    )

    max_referral_date = sp_ref_dates.max()

    if pd.isna(max_referral_date):
        return 0, 0, 0

    date_90_days_before = max_referral_date - timedelta(days=90)
    date_180_days_before = max_referral_date - timedelta(days=180)
    date_360_days_before = max_referral_date - timedelta(days=360)

    claims = mx_df.loc[mx_df[claims_patient_id_in_claims] == claims_patient_id].copy()
    claims['service_date'] = pd.to_datetime(claims['service_date'], errors='coerce')

    claims = claims.dropna(subset=['service_date'])

    claims_before_90_days = claims.loc[claims['service_date'] >= date_90_days_before]
    claims_before_180_days = claims.loc[claims['service_date'] >= date_180_days_before]
    claims_before_360_days = claims.loc[claims['service_date'] >= date_360_days_before]

    icd_code_str = str(icd_code)

    mask_90 = claims_before_90_days[claims_icd_code_column].astype(str).str.contains(icd_code_str, na=False)
    mask_180 = claims_before_180_days[claims_icd_code_column].astype(str).str.contains(icd_code_str, na=False)
    mask_360 = claims_before_360_days[claims_icd_code_column].astype(str).str.contains(icd_code_str, na=False)

    return int(mask_90.sum()), int(mask_180.sum()), int(mask_360.sum()) 



    



def generate_lookback_frequency(row, mx_df):
    icd_code = row.get(sp_icd_code_column)
    claims_patient_id = row.get(claims_patient_id_column_in_core_table)

    if pd.isna(icd_code) or pd.isna(claims_patient_id):
        return -1

    pattern = rf'(?:^|[^A-Z0-9]){re.escape(str(icd_code))}(?:[^A-Z0-9]|$)'

    claims_filtered = mx_df.loc[
        (mx_df[claims_patient_id_in_claims] == claims_patient_id) &
        (mx_df[claims_icd_code_column].astype(str).str.contains(pattern, na=False, regex=True))
    ].copy()

    if claims_filtered.empty:
        return -1

    service_dates = pd.to_datetime(claims_filtered['service_date'], errors='coerce').dropna()
    if service_dates.empty:
        return -1

    return int((service_dates.max() - service_dates.min()).days)






