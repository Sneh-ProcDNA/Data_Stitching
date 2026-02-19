from src.modules.therapy_history import *
from src.common.db import *



def cleanse_ship_date(ship_date):
    if ship_date is None or pd.isna(ship_date):
        return pd.NaT
    
    ship_date = str(ship_date)

    if '-' in ship_date:
        return pd.to_datetime(ship_date).date()

    converted_date = pd.to_datetime(int(ship_date), origin='1899-12-30', unit='D').date()

    return converted_date


def generate_prescription_flag(row):
    transaction_result = row['transaction_result']
    sp_rx = row['rx_written_date']
    cl_rx = row['date_prescription_written']

    sp_fill = row['ship_date']
    cl_fill = row['fill_date']

    if pd.isna(sp_rx) or pd.isna(cl_rx):
        return -1, -1, -1

    rx_flag = 1 if sp_rx == cl_rx else 0

    if transaction_result != 'PAID':
        return rx_flag, -1, -1

    if pd.isna(sp_fill) or pd.isna(cl_fill):
        return rx_flag, -1, -1
    
    days_lag = (cl_fill - sp_fill).days

    if days_lag < 0:
        days_lag *= -1

    exact_fill_flag = 1 if sp_fill == cl_fill else 0
    lag_fill_flag = 1 if days_lag <= 7 else 0

    return rx_flag, exact_fill_flag, lag_fill_flag 

def generate_quantity_dispensed_flag(row):
    transaction_result = row['transaction_result']

    sp_days_supply = row['days_supply_x']
    sp_dispense_quantity = row['dispense_quantity']

    claims_days_supply = row['days_supply_y']
    claims_dispense_quantity = row['quantity']

    sp_fill = row['ship_date']
    cl_fill = row['fill_date']

    if transaction_result != 'PAID':
        return -1, -1

    #match date
    if (
        pd.isna(sp_days_supply) or pd.isna(claims_days_supply) or pd.isna(claims_days_supply) or pd.isna(claims_dispense_quantity) or pd.isna(sp_fill) or pd.isna(cl_fill)
    ):
        return -1, -1

    cl_fill = cl_fill.date()
    sp_fill = sp_fill.date()

    days_lag = abs((cl_fill - sp_fill).days)
    
    if days_lag <= 7:
        days_supply_flag = 1 if sp_days_supply == claims_days_supply else 0
        quantity_flag = 1 if sp_dispense_quantity == claims_dispense_quantity else 0

        return days_supply_flag, quantity_flag
    
    return 0, 0