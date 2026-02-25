from src.modules.diagnosis_validation import *
from src.common.constants import treatment_codes
from src.modules.therapy_history import *
from src.modules.dispense_and_utilization import *
from src.modules.payor_rules import *
from src.common.logger import get_logger
import time
from datetime import datetime
import os
from datetime import datetime
import pandas as pd
import time
from sqlalchemy import create_engine
from src.common.logger import get_logger
from datetime import datetime

logger = get_logger("db")

# ─────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────
host = 'database-sparkathon.cm6vbnnsye5g.us-east-1.rds.amazonaws.com'
port = 5432
database = 'postgres'
user = 'postgres'
password = "1QCF03Azp9wUQ3EfYeqI"

logger.debug(f"Initializing database engine | host={host} | port={port} | database={database} | user={user}")
try:
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}",
        connect_args={"sslmode": "require"}
    )
    logger.info("Database engine created successfully")
except Exception as e:
    logger.exception(f"Failed to create database engine: {e}")
    raise

db_start = time.time()
logger.info(f"Starting Data Download at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
# ─────────────────────────────────────────
# LOAD SP CORE BASE
# ─────────────────────────────────────────
sp_core_base_query = """
select *
from data_dev.sp_komodo_core_base
"""

logger.debug("Executing query: sp_komodo_core_base")
try:
    t = time.time()
    sp_core_df = pd.read_sql(sp_core_base_query, engine)
    logger.info(f"sp_core_df loaded | rows: {len(sp_core_df)} | columns: {sp_core_df.shape[1]} | time: {time.time() - t:.2f}s")
except Exception as e:
    logger.exception(f"Failed to load sp_core_df: {e}")
    raise

# ─────────────────────────────────────────
# LOAD SP DATA
# ─────────────────────────────────────────
sp_query = """
select *
from data_dev.sp_data
"""

logger.debug("Executing query: sp_data")
try:
    t = time.time()
    sp_data_df = pd.read_sql(sp_query, engine)
    logger.info(f"sp_data_df loaded | rows: {len(sp_data_df)} | columns: {sp_data_df.shape[1]} | time: {time.time() - t:.2f}s")
except Exception as e:
    logger.exception(f"Failed to load sp_data_df: {e}")
    raise

# ─────────────────────────────────────────
# LOAD MX CORE TABLE
# ─────────────────────────────────────────
mx_query = """
select *
from data_dev.mx_core_table
"""

logger.debug("Executing query: mx_core_table")
try:
    t = time.time()
    mx_df = pd.read_sql(mx_query, engine)
    logger.info(f"mx_df loaded | rows: {len(mx_df)} | columns: {mx_df.shape[1]} | time: {time.time() - t:.2f}s")
except Exception as e:
    logger.exception(f"Failed to load mx_df: {e}")
    raise

# ─────────────────────────────────────────
# LOAD PX CORE TABLE
# ─────────────────────────────────────────
px_query = """
select *
from data_dev.px_core_table
"""

logger.debug("Executing query: px_core_table")
try:
    t = time.time()
    px_df = pd.read_sql(px_query, engine)
    logger.info(f"px_df loaded | rows: {len(px_df)} | columns: {px_df.shape[1]} | time: {time.time() - t:.2f}s")
except Exception as e:
    logger.exception(f"Failed to load px_df: {e}")
    raise

total_db_time = time.time() - db_start
mins, secs = divmod(total_db_time, 60)
logger.info(f"All database tables loaded successfully | total db load time: {int(mins)}m {secs:.2f}s")




# ─────────────────────────────────────────
# PAYOR QUERY
# ─────────────────────────────────────────
px_payor_query = """
select 
patient_id,
primary_kh_plan_id
from
data_dev.px_core_table
"""

px_payor_df = pd.read_sql(px_payor_query, engine)




# ─────────────────────────────────────────
# PLAN QUERY
# ─────────────────────────────────────────
plan_query = """
select
kh_plan_id,
payer_name,
pbm_processor,
insurance_group,
insurance_segment
from
data_dev.komodo_plans
"""

plans_df = pd.read_sql(plan_query, engine)

# ─────────────────────────────────────────
# SP PAYOR QUERY
# ─────────────────────────────────────────
sp_payor_query = """
select 
patient_id,
payor_type,
payor_name,
pbm,
plan_name
from
data_dev.sp_data
"""

sp_payor_df = pd.read_sql(sp_payor_query, engine)



# ─────────────────────────────────────────
# SP PRIOR TREATMENT QUERY
# ─────────────────────────────────────────

sp_prior_treatments_query = """
select patient_id,
prior_treatments_from_hcp
from 
data_dev.sp_data
"""

sp_prior_treatments_df = pd.read_sql(sp_prior_treatments_query, engine)
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
logger.info(f"ICD codes from SP data populated for {sp_diagnosis_df['icd_code'].notna().sum()} records")

logger.debug("Extracting ICD codes from claims")
sp_diagnosis_df['icd_code_claims'] = sp_diagnosis_df.apply(
    get_icd_code_from_claims,
    axis=1,
    args=(mx_df,)
)
logger.info(f"ICD codes from claims populated for {sp_diagnosis_df['icd_code_claims'].notna().sum()} records")

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

logger.debug("Calculating diagnosis frequency per patient row")
freqs = []
for _, row in sp_diagnosis_df.iterrows():
    freqs.append(calculate_diag_freq(row, mx_df))

sp_diagnosis_df['diag_freq'] = freqs
logger.info(f"Diagnosis frequency calculated | mean={sp_diagnosis_df['diag_freq'].mean():.2f} | max={sp_diagnosis_df['diag_freq'].max()}")

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
logger.info(
    f"Quantity flags | days_supply_flag=True: {sp_core_copy['days_supply_flag'].sum()} "
    f"| quantity_flag=True: {sp_core_copy['quantity_flag'].sum()}"
)
sp_core_copy = sp_core_copy.drop(columns=['rx_flag', 'exact_fill_flag', 'lag_fill_flag', 'days_supply_flag', 'quantity_flag'])

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
sp_payor_core_df.to_excel(f"outputs/payor_core_{timestamp}.xlsx", index=False)

logger.info(f"Results exported to outputs/ | therapy_rules: {len(sp_therapy_df)} rows | dispense_rules: {len(sp_dispense_final_df)} rows | diagnosis_validation: {len(sp_diagnosis_df)} rows | payor_rules: {len(sp_payor_core_df)} rows")

# ─────────────────────────────────────────
# PIPELINE COMPLETE
# ─────────────────────────────────────────
elapsed = time.time() - pipeline_start
mins, secs = divmod(elapsed, 60)
logger.info(f"Pipeline execution complete at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | total time: {int(mins)}m {secs:.2f}s")
logger.info(f"Total time taken for pipeline execution: {int(mins)}m {secs:.2f}s")



