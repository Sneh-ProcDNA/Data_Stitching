from src.modules.diagnosis_validation import *
from src.common.constants import treatment_codes
from src.common.db import *
from src.modules.therapy_history import *
from src.modules.dispense_and_utilization import *
from src.modules.payor_rules import *
from src.common.logger import get_logger
import time
from datetime import datetime
import os
from datetime import datetime

logger = get_logger("pipeline")
pipeline_start = time.time()
logger.info(f"Starting Pipeline at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# ─────────────────────────────────────────
# DIAGNOSIS VALIDATION
# ─────────────────────────────────────────
logger.info(f"Diagnosis Validation Section started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
sp_diagnosis_df = sp_core_df.copy()

logger.debug("Extracting ICD codes from SP data")

sp_diagnosis_df['icd_code'] = sp_diagnosis_df['sp_patient_id'].apply(get_icd_code_from_data, args=(sp_data_df,))
logger.info(f"ICD codes from SP data populated for {sp_core_df['icd_code'].notna().sum()} records")

logger.debug("Extracting ICD codes from claims")
sp_diagnosis_df['icd_code_claims'] = sp_diagnosis_df.apply(
    get_icd_code_from_claims,
    axis=1,
    args=(mx_df,)
)
logger.info(f"ICD codes from claims populated for {sp_core_df['icd_code_claims'].notna().sum()} records")

logger.debug("Generating diagnosis flags (parent and exact)")
sp_diagnosis_df[['parent_diag_code_flag', 'exact_diag_code_flag']] = sp_diagnosis_df.apply(
    generate_diagnosis_flags,
    axis=1,
    result_type='expand'
)
logger.info(
    f"Diagnosis flags generated | parent_diag_code_flag=True: {sp_core_df['parent_diag_code_flag'].sum()} "
    f"| exact_diag_code_flag=True: {sp_core_df['exact_diag_code_flag'].sum()}"
)

logger.debug("Calculating diagnosis frequency per patient row")
freqs = []
for _, row in sp_diagnosis_df.iterrows():
    freqs.append(calculate_diag_freq(row, mx_df))

sp_diagnosis_df['diag_freq'] = freqs
logger.info(f"Diagnosis frequency calculated | mean={sp_core_df['diag_freq'].mean():.2f} | max={sp_core_df['diag_freq'].max()}")

logger.debug("Generating diagnosis lookback windows (90, 180, 360 days)")
sp_diagnosis_df[['90_days_lookback_freq', '180_days_lookback_freq', '360_days_lookback_freq']] = sp_diagnosis_df.apply(
    generate_diagnosis_lookback,
    axis=1,
    args=(sp_data_df, mx_df),
    result_type='expand'
)
logger.info("Diagnosis lookback windows populated successfully")

logger.debug("Generating first-last claim lookback frequency")
sp_diagnosis_df['first_last_claim_lookback'] = sp_diagnosis_df.apply(
    generate_lookback_frequency,
    axis=1,
    args=(mx_df,)
)

logger.info("First-last claim lookback frequency populated")
sp_diagnosis_df = sp_diagnosis_df.drop_duplicates(subset=['sp_prescriber_npi', 'sp_patient_id', 'matched_patient_id'])

logger.info(f"Diagnosis Validation Section completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ─────────────────────────────────────────
# THERAPY RULES
# ─────────────────────────────────────────

logger.info(f"Therapy Rules Section started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
sp_therapy_df = sp_core_df
for treatment, codes in treatment_codes.items():
    sp_therapy_df[[f'{treatment}_flag', f'{treatment}_freq']] = sp_therapy_df.apply(
        generate_treatment_flag,
        axis=1,
        args=(codes, mx_df),
        result_type='expand'
    )

sp_therapy_df = sp_therapy_df.drop_duplicates(subset=['sp_prescriber_npi', 'sp_patient_id', 'matched_patient_id'])


logger.info(f"Therapy Rules Section completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ─────────────────────────────────────────
# DISPENSE RULES
# ─────────────────────────────────────────
logger.info(f"Dispense Rules Section started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
px_dispense_df = sp_data_df
sp_dispense_df=sp_data_df

sp_dispense_df = sp_dispense_df[['patient_id', 'rx_written_date', 'ship_date', 'days_supply', 'dispense_quantity']]
sp_dispense_df['ship_date'] = sp_dispense_df['ship_date'].apply(
    cleanse_ship_date
)

sp_dispense_df['ship_date'] = pd.to_datetime(sp_dispense_df['ship_date'], errors='coerce')
px_dispense = px_df[['patient_id', 'fill_date', 'date_prescription_written', 'transaction_result', 'days_supply', 'quantity']].copy()
logger.debug("Cleansing and parsing ship_date in sp_dispense")
px_dispense['fill_date'] = pd.to_datetime(px_dispense['fill_date'])
px_dispense['date_prescription_written'] = pd.to_datetime(px_dispense['date_prescription_written'])
sp_core_copy = sp_core_df.copy()
sp_core_copy = sp_core_copy.merge(sp_dispense_df, how='left', left_on='sp_patient_id', right_on='patient_id').drop(columns=['patient_id'])
sp_core_copy = sp_core_copy.merge(px_dispense, how='left', left_on='matched_patient_id', right_on='patient_id').drop(columns=['patient_id'])
sp_core_copy[sp_core_copy['ship_date'] == sp_core_copy['fill_date']]
sp_core_copy['fill_date'] = pd.to_datetime(sp_core_copy['fill_date'], errors='coerce')
sp_core_copy['date_prescription_written'] = pd.to_datetime(sp_core_copy['date_prescription_written'], errors='coerce')


logger.debug("Loading PX data and filtering for MODEYSO / DORDAVIPRONE HCL")
logger.debug("Generating prescription flags (rx_flag, exact_fill_flag, lag_fill_flag)")
sp_core_copy[['rx_flag', 'exact_fill_flag', 'lag_fill_flag']] = sp_core_copy.apply(
    generate_prescription_flag,
    axis=1,
    result_type='expand'
)

sp_core_copy[sp_core_copy['fill_date'] == sp_core_copy['ship_date']]
sp_core_copy['days_supply_x'] = pd.to_numeric(sp_core_copy['days_supply_x'], errors='coerce')
sp_core_copy['dispense_quantity'] = pd.to_numeric(sp_core_copy['dispense_quantity'], errors='coerce')
sp_core_copy['days_supply_y'] = pd.to_numeric(sp_core_copy['days_supply_y'], errors='coerce')
sp_core_copy['quantity'] = pd.to_numeric(sp_core_copy['quantity'], errors='coerce')


logger.info(
    f"Prescription flags | rx_flag=True: {sp_core_copy['rx_flag'].sum()} "
    f"| exact_fill_flag=True: {sp_core_copy['exact_fill_flag'].sum()} "
    f"| lag_fill_flag=True: {sp_core_copy['lag_fill_flag'].sum()}"
)

logger.debug("Generating quantity dispensed flags (days_supply_flag, quantity_flag)")
sp_core_copy[['days_supply_flag', 'quantity_flag']] = sp_core_copy.apply(
    generate_quantity_dispensed_flag,
    axis=1,
    result_type='expand'
)
sp_core_copy['rx_date_match_flag'] = sp_core_copy.groupby(['sp_patient_id', 'matched_patient_id'])['rx_flag'].transform(lambda x: 1 if (x == 1).any() else 0)
sp_core_copy['exact_fill_date_flag'] = sp_core_copy.groupby(['sp_patient_id', 'matched_patient_id'])['exact_fill_flag'].transform(lambda x: 1 if (x == 1).any() else 0)
sp_core_copy['lag_fill_date_flag'] = sp_core_copy.groupby(['sp_patient_id', 'matched_patient_id'])['lag_fill_flag'].transform(lambda x: 1 if (x == 1).any() else 0)
sp_core_copy['days_supply_final_flag'] = sp_core_copy.groupby(['sp_patient_id', 'matched_patient_id'])['days_supply_flag'].transform(lambda x: 1 if (x == 1).any() else 0)
sp_core_copy['quantity_final_flag'] = sp_core_copy.groupby(['sp_patient_id', 'matched_patient_id'])['quantity_flag'].transform(lambda x: 1 if (x == 1).any() else 0)
sp_core_copy = sp_core_copy.drop(columns=['rx_flag', 'exact_fill_flag', 'lag_fill_flag', 'days_supply_flag', 'quantity_flag'])
logger.info(
    f"Quantity flags | days_supply_flag=True: {sp_core_copy['days_supply_flag'].sum()} "
    f"| quantity_flag=True: {sp_core_copy['quantity_flag'].sum()}"
)
sp_dispense_final_df = sp_core_copy.copy()
sp_dispense_final_df = sp_dispense_final_df.drop_duplicates(subset=['sp_prescriber_npi', 'sp_patient_id', 'matched_patient_id'])

logger.info(f"Dispense Rules Section completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ─────────────────────────────────────────
# PAYOR RULES
# ─────────────────────────────────────────
logger.info(f"Payor Rules Section started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

px_payor_df = pd.merge(px_payor_df, plans_df, how='left', left_on='primary_kh_plan_id', right_on='kh_plan_id').drop(columns=['kh_plan_id'])

sp_payor_core_df = sp_core_df.copy()
logger.debug("Merging SP payor data with SP core on sp_patient_id")
sp_payor_core_df = pd.merge(sp_payor_core_df, sp_payor_df, how='left', left_on='sp_patient_id', right_on='patient_id').drop(columns=['patient_id'])
sp_payor_core_df = sp_payor_core_df.drop_duplicates(subset=sp_payor_core_df.columns.to_list())

logger.debug("Merging SP payor core with PX payor data on matched_patient_id")
sp_payor_core_df = pd.merge(sp_payor_core_df, px_payor_df, how='left', left_on='matched_patient_id', right_on='patient_id').drop(columns='patient_id')
sp_payor_core_df = sp_payor_core_df.drop_duplicates(sp_payor_core_df.columns.to_list())

logger.debug("Generating payor name flag")
sp_payor_core_df['payor_name_flag'] = sp_payor_core_df.apply(
    generate_payor_name_flag,
    axis=1,
    result_type='expand'
)

logger.debug("Generating payor type flag")
sp_payor_core_df['payor_type_flag'] = sp_payor_core_df.apply(
    generate_payor_type_flag,
    axis=1,
    result_type='expand'
)

logger.debug("Generating PBM flag")
sp_payor_core_df['pbm_flag'] = sp_payor_core_df.apply(
    generate_pbm_flag,
    axis=1,
    result_type='expand'
)

sp_payor_core_df = sp_payor_core_df.drop_duplicates(subset=['sp_prescriber_npi', 'sp_patient_id', 'matched_patient_id'])
logger.info(
    f"Payor flags generated | payor_name_flag=True: {sp_payor_core_df['payor_name_flag'].sum()} "
    f"| payor_type_flag=True: {sp_payor_core_df['payor_type_flag'].sum()} "
    f"| pbm_flag=True: {sp_payor_core_df['pbm_flag'].sum()}"
)

logger.info(f"Payor Rules Section completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# ─────────────────────────────────────────
# EXPORT RESULTS TO EXCEL
# ─────────────────────────────────────────
logger.info("Exporting results to Excel")

os.makedirs("outputs", exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

sp_therapy_df.to_excel(f"outputs/therapy_rules_{timestamp}.xlsx", index=False)
sp_dispense_final_df.to_excel(f"outputs/dispense_rules_{timestamp}.xlsx", index=False)
sp_diagnosis_df.to_excel(f"outputs/diagnosis_validation_{timestamp}.xlsx", index=False)
sp_payor_core_df.to_excel(f"outputs/diagnosis_validation_{timestamp}.xlsx", index=False)

logger.info(f"Results exported to outputs/ | therapy_rules: {len(sp_therapy_df)} rows | dispense_rules: {len(sp_dispense_final_df)} rows | diagnosis_validation: {len(sp_diagnosis_df)} rows | payor_rules: {len(sp_payor_core_df)} rows")

# ─────────────────────────────────────────
# PIPELINE COMPLETE
# ─────────────────────────────────────────
elapsed = time.time() - pipeline_start
mins, secs = divmod(elapsed, 60)
logger.info(f"Pipeline execution complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | total time: {int(mins)}m {secs:.2f}s")
logger.info(f"Total time taken for pipeline execution: {int(mins)}m {secs:.2f}s")



