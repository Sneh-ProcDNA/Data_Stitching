import psycopg2
import boto3
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