from src.modules.diagnosis_validation import *
from src.common.constants import treatment_codes
from src.modules.therapy_history import *
from src.modules.dispense_and_utilization import *
from src.modules.payor_rules import *
from src.common.logger import get_logger
import time
from datetime import datetime
import os
import pandas as pd
from sqlalchemy import create_engine
from src.common.db import *
from input import *
from src.modules.scoring import *

logger = get_logger("pipeline")
pipeline_start = time.time()
logger.info(f"Starting Pipeline at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ─────────────────────────────────────────
# DIAGNOSIS VALIDATION
# ─────────────────────────────────────────
logger.info(f"Diagnosis Validation Section started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
sp_diagnosis_df = sp_core_df.copy()

logger.debug("Extracting ICD codes from SP data")
sp_diagnosis_df[sp_icd_code_column] = sp_diagnosis_df[sp_patient_id_column_in_core_table].apply(
    get_icd_code_from_data, args=(sp_data_df,)
)
logger.info(f"ICD codes from SP data populated for {sp_diagnosis_df[sp_icd_code_column].notna().sum()} records")

logger.debug("Extracting ICD codes from claims")
sp_diagnosis_df[exact_claims_diag_code] = sp_diagnosis_df.apply(
    get_icd_code_from_claims,
    axis=1,
    args=(mx_df,)
)
logger.info(f"ICD codes from claims populated for {sp_diagnosis_df[exact_claims_diag_code].notna().sum()} records")

logger.debug("Generating diagnosis flags (parent and exact)")
sp_diagnosis_df[['parent_diag_code_flag', 'exact_diag_code_flag']] = sp_diagnosis_df.apply(
    generate_diagnosis_flags,
    axis=1,
    result_type='expand'
)
logger.info(
    f"Diagnosis flags generated | parent_diag_code_flag=True: {sp_diagnosis_df['parent_diag_code_flag'].sum()} "
    f"| exact_diag_code_flag=True: {sp_diagnosis_df['exact_diag_code_flag'].sum()}"
)
logger.info(f"Diagnosis Validation Section completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ─────────────────────────────────────────
# THERAPY RULES  ← PRIMARY FIX
# ─────────────────────────────────────────
# BEFORE (slow): row-by-row .apply() scanning all of mx_df per row → O(N × M)
#
#   for treatment, codes in treatment_codes.items():
#       sp_therapy_df[[f'{treatment}_flag', f'{treatment}_freq']] = sp_therapy_df.apply(
#           generate_treatment_flag, axis=1, args=(codes, mx_df), result_type='expand'
#       )
#
# AFTER (fast): single merge+groupby per treatment → O(M + N log N)
# ─────────────────────────────────────────
logger.info(f"Therapy Rules Section started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

sp_therapy_df = generate_treatment_flags_vectorized(sp_core_df, treatment_codes, mx_df)

sp_therapy_df = sp_therapy_df.drop_duplicates(
    subset=[npi_column_in_core_table, sp_patient_id_column_in_core_table, claims_patient_id_column_in_core_table]
)

# ─────────────────────────────────────────
# CREATE PRIOR TREATMENT FLAG ✅ FIX
# ─────────────────────────────────────────

# Identify all *_flag columns from therapy
therapy_flag_cols = [c for c in sp_therapy_df.columns if c.endswith('_flag')]

# Create prior_treatment_flag = any therapy flag = 1
sp_therapy_df['prior_treatment_flag'] = (
    sp_therapy_df[therapy_flag_cols].sum(axis=1) > 0
).astype(int)

logger.info(f"prior_treatment_flag created | True count: {sp_therapy_df['prior_treatment_flag'].sum()}")
logger.info(f"Therapy Rules Section completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ─────────────────────────────────────────
# DISPENSE RULES
# ─────────────────────────────────────────
logger.info(f"Dispense Rules Section started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

sp_dispense_df = sp_data_df[[sp_patient_id_in_sp_data, prescription_written_date_sp, ship_date_sp]].copy()

# FIX: vectorized ship_date cleansing instead of row-by-row .apply(cleanse_ship_date)
#
# BEFORE (slow):
#   sp_dispense_df[ship_date_sp] = sp_dispense_df[ship_date_sp].apply(cleanse_ship_date)
#
# AFTER (fast): split into two cases — ISO strings vs Excel serial numbers
_sd = sp_dispense_df[ship_date_sp]
_is_str_with_dash = _sd.astype(str).str.contains('-', na=False)
sp_dispense_df[ship_date_sp] = pd.to_datetime(
    _sd.where(_is_str_with_dash),   # parse ISO strings directly
    errors='coerce'
).fillna(
    # Excel serial numbers: origin 1899-12-30
    pd.to_datetime(
        pd.to_numeric(_sd.where(~_is_str_with_dash), errors='coerce'),
        origin='1899-12-30',
        unit='D',
        errors='coerce'
    )
)

px_dispense = px_df[
    [claims_patient_id_in_claims, ship_date_claims, prescription_written_date_claims,
     'transaction_result', 'days_supply', 'quantity']
].copy()
px_dispense[ship_date_claims] = pd.to_datetime(px_dispense[ship_date_claims], errors='coerce')
px_dispense[prescription_written_date_claims] = pd.to_datetime(px_dispense[prescription_written_date_claims], errors='coerce')

sp_core_copy = sp_core_df.copy()

# Rename 'patient_id' in the right-side frames before merging to avoid column
# name collisions when sp_core_df itself already contains a 'patient_id' column.
# (When left and right both have 'patient_id', pandas suffixes them to
# 'patient_id_x'/'patient_id_y' and the subsequent .drop('patient_id') fails.)
sp_core_copy = sp_core_copy.merge(
    sp_dispense_df.rename(columns={'patient_id': '_sp_pid'}),
    how='left',
    left_on=sp_patient_id_column_in_core_table,
    right_on='_sp_pid'
).drop(columns=['_sp_pid'])

sp_core_copy = sp_core_copy.merge(
    px_dispense.rename(columns={claims_patient_id_in_claims: '_px_pid'}),
    how='left',
    left_on=claims_patient_id_column_in_core_table,
    right_on='_px_pid'
).drop(columns=['_px_pid'])

sp_core_copy[ship_date_claims] = pd.to_datetime(sp_core_copy[ship_date_claims], errors='coerce')
sp_core_copy[prescription_written_date_claims] = pd.to_datetime(sp_core_copy[prescription_written_date_claims], errors='coerce')

logger.debug("Generating prescription flags (rx_flag, exact_fill_flag, lag_fill_flag)")
sp_core_copy[[rx_date_match_flag, exact_fill_date_match_flag, lag_fill_date_match_flag]] = sp_core_copy.apply(
    generate_prescription_flag,
    axis=1,
    result_type='expand'
)

logger.info(
    f"Prescription flags | rx_flag=True: {(sp_core_copy[rx_date_match_flag] == 1).sum()} "
    f"| exact_fill_flag=True: {(sp_core_copy[exact_fill_date_match_flag] == 1).sum()} "
    f"| lag_fill_flag=True: {(sp_core_copy[lag_fill_date_match_flag] == 1).sum()}"
)

grp = [sp_patient_id_column_in_core_table, claims_patient_id_column_in_core_table]
sp_core_copy['rx_date_match_flag'] = sp_core_copy.groupby(grp)[rx_date_match_flag].transform(
    lambda x: 1 if (x == 1).any() else 0
)
sp_core_copy['exact_fill_date_flag'] = sp_core_copy.groupby(grp)[exact_fill_date_match_flag].transform(
    lambda x: 1 if (x == 1).any() else 0
)
sp_core_copy['lag_fill_date_flag'] = sp_core_copy.groupby(grp)[lag_fill_date_match_flag].transform(
    lambda x: 1 if (x == 1).any() else 0
)
sp_core_copy = sp_core_copy.drop(columns=[rx_date_match_flag, exact_fill_date_match_flag, lag_fill_date_match_flag])

# Vectorized replacement for calculate_days_between_dates apply()
# Casts both columns to datetime first to avoid "str - Timestamp" TypeError
_referral = pd.to_datetime(sp_core_copy['referral_date'], errors='coerce')
_service  = pd.to_datetime(sp_core_copy['service_date'],  errors='coerce')
sp_core_copy['diagnosis_days_lag'] = (_service - _referral).abs().dt.days.where(
    _referral.notna() & _service.notna(), other=float('inf')
)

sp_dispense_final_df = sp_core_copy.drop_duplicates(
    subset=[npi_column_in_core_table, sp_patient_id_column_in_core_table, claims_patient_id_column_in_core_table]
)
logger.info(f"Dispense Rules Section completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ─────────────────────────────────────────
# PAYOR RULES
# ─────────────────────────────────────────
logger.info(f"Payor Rules Section started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

px_payor_df = pd.merge(
    px_payor_df, plans_df,
    how='left', left_on='primary_kh_plan_id', right_on='kh_plan_id'
).drop(columns=['kh_plan_id'])

sp_payor_core_df = sp_core_df.copy()
logger.debug("Merging SP payor data with SP core on sp_patient_id")
sp_payor_core_df = pd.merge(
    sp_payor_core_df,
    sp_payor_df.rename(columns={'patient_id': '_sp_pid'}),
    how='left',
    left_on=sp_patient_id_column_in_core_table,
    right_on='_sp_pid'
).drop(columns=['_sp_pid'])
sp_payor_core_df = sp_payor_core_df.drop_duplicates(subset=sp_payor_core_df.columns.to_list())

logger.debug("Merging SP payor core with PX payor data on matched_patient_id")
sp_payor_core_df = pd.merge(
    sp_payor_core_df,
    px_payor_df.rename(columns={'patient_id': '_px_pid'}),
    how='left',
    left_on=claims_patient_id_column_in_core_table,
    right_on='_px_pid'
).drop(columns=['_px_pid'])
sp_payor_core_df = sp_payor_core_df.drop_duplicates(sp_payor_core_df.columns.to_list())

# FIX: vectorized payor flags instead of row-by-row .apply()
#
# BEFORE (slow):
#   sp_payor_core_df['payor_name_flag'] = sp_payor_core_df.apply(generate_payor_name_flag, axis=1, ...)
#
# AFTER (fast): direct string comparison on whole columns
logger.debug("Generating payor flags (vectorized)")
sp_payor_core_df['payor_name_flag'] = (
    sp_payor_core_df['payor_name'].astype(str).str.lower() ==
    sp_payor_core_df['payer_name'].astype(str).str.lower()
).astype(int)

sp_payor_core_df['payor_type_flag'] = (
    sp_payor_core_df['payor_type'].astype(str).str.lower() ==
    sp_payor_core_df['insurance_group'].astype(str).str.lower()
).astype(int)

sp_payor_core_df['pbm_flag'] = (
    sp_payor_core_df['pbm'].astype(str).str.lower() ==
    sp_payor_core_df['pbm_processor'].astype(str).str.lower()
).astype(int)

sp_payor_core_df = sp_payor_core_df.drop_duplicates(
    subset=[npi_column_in_core_table, sp_patient_id_column_in_core_table, claims_patient_id_column_in_core_table]
)
logger.info(
    f"Payor flags generated | payor_name_flag=True: {sp_payor_core_df['payor_name_flag'].sum()} "
    f"| payor_type_flag=True: {sp_payor_core_df['payor_type_flag'].sum()} "
    f"| pbm_flag=True: {sp_payor_core_df['pbm_flag'].sum()}"
)
logger.info(f"Payor Rules Section completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ─────────────────────────────────────────
# CREATE FINAL PAYOR FLAGS ✅ FIX
# ─────────────────────────────────────────

sp_payor_core_df['payor_name_final_flag'] = sp_payor_core_df['payor_name_flag']
sp_payor_core_df['payor_type_final_flag'] = sp_payor_core_df['payor_type_flag']
sp_payor_core_df['pbm_final_flag'] = sp_payor_core_df['pbm_flag']

logger.info("Final payor flags created")

# ─────────────────────────────────────────
# MERGE ALL FEATURES BEFORE SCORING
# ─────────────────────────────────────────

logger.info("Merging diagnosis + dispense + therapy into final dataset")

# Select only required columns to avoid explosion
diag_cols = [
    sp_patient_id_column_in_core_table,
    claims_patient_id_column_in_core_table,
    'parent_diag_code_flag',
    'exact_diag_code_flag'
]

disp_cols = [
    sp_patient_id_column_in_core_table,
    claims_patient_id_column_in_core_table,
    'rx_date_match_flag',
    'lag_fill_date_flag'
]

therapy_cols = [
    sp_patient_id_column_in_core_table,
    claims_patient_id_column_in_core_table,
    'prior_treatment_flag'
]

# Merge diagnosis
sp_final_df = sp_payor_core_df.merge(
    sp_diagnosis_df[diag_cols],
    how='left',
    on=[sp_patient_id_column_in_core_table, claims_patient_id_column_in_core_table]
)

# Merge dispense
sp_final_df = sp_final_df.merge(
    sp_dispense_final_df[disp_cols],
    how='left',
    on=[sp_patient_id_column_in_core_table, claims_patient_id_column_in_core_table]
)

# Merge therapy
sp_final_df = sp_final_df.merge(
    sp_therapy_df[therapy_cols],
    how='left',
    on=[sp_patient_id_column_in_core_table, claims_patient_id_column_in_core_table]
)

# Fill NA flags → 0
flag_cols = [
    'parent_diag_code_flag',
    'exact_diag_code_flag',
    'prior_treatment_flag',
    'rx_date_match_flag',
    'lag_fill_date_flag'
]

sp_final_df[flag_cols] = sp_final_df[flag_cols].fillna(0)

logger.info("All features merged successfully")
# ─────────────────────────────────────────
# SCORING
# ─────────────────────────────────────────
logger.info(f"Scoring started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
sp_final_df['confidence_score'] = sp_final_df.apply(generate_confidence_score, axis=1)
logger.info(f"Scoring completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ─────────────────────────────────────────
# EXPORT RESULTS TO EXCEL
# ─────────────────────────────────────────
logger.info("Exporting results to Excel")
os.makedirs("outputs", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
sp_final_df.to_excel(f"outputs/patient_matching_{timestamp}.xlsx", index=False)
logger.info(f"Results exported to outputs/: {len(sp_payor_core_df)} rows")


# ─────────────────────────────────────────
# PIPELINE COMPLETE
# ─────────────────────────────────────────
elapsed = time.time() - pipeline_start
mins, secs = divmod(elapsed, 60)
logger.info(f"Pipeline execution complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | total time: {int(mins)}m {secs:.2f}s")
logger.info(f"Total time taken for pipeline execution: {int(mins)}m {secs:.2f}s")