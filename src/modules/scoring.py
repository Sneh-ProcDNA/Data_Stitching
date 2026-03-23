import numpy as np
import pandas as pd


def generate_confidence_score(row):
    parent_diag_code_flag = row['parent_diag_code_flag']
    exact_diag_code_flag = row['exact_diag_code_flag']
    prior_treatment_flag = row['prior_treatment_flag']
    rx_date_match_flag = row['rx_date_match_flag']
    lag_fill_date_flag = row['lag_fill_date_flag']
    payor_name_final_flag = row['payor_name_final_flag']
    payor_type_final_flag = row['payor_type_final_flag']
    pbm_final_flag = row['pbm_final_flag']

    diagnosis_score_parent = 0
    diagnosis_score_exact = 0

    if parent_diag_code_flag == 1:
        diagnosis_score_parent = 5
    
    if exact_diag_code_flag == 1:
        diagnosis_score_exact = 5

    final_dispense = 0

    if rx_date_match_flag == 1:
        if lag_fill_date_flag == 1 or lag_fill_date_flag == -1:
            final_dispense = 20

    therapy_score = 0

    if prior_treatment_flag == 1:
        therapy_score = 10

    payor_type_score = 0
    payor_name_score = 0
    pbm_score = 0

    if payor_name_final_flag == 1:
        payor_name_score = 2

    if payor_type_final_flag == 1:
        payor_type_score = 5 

    if pbm_final_flag == 1:
        pbm_score = 3

    confidence_score = diagnosis_score_parent + diagnosis_score_exact + final_dispense + therapy_score + payor_type_score + payor_name_score + pbm_score + 50

    return confidence_score





 
def generate_best_flag(group):
    if 'days_lag' not in group.columns:
        return pd.Index([])

    if np.isinf(group['days_lag']).all():
        return pd.Index([])

    max_conf = group['confidence_score'].max()
    top_conf = group[group['confidence_score'] == max_conf]

    if len(top_conf) == 1:
        return top_conf.index

    valid_lag = top_conf[~np.isinf(top_conf['days_lag'])]

    if valid_lag.empty:
        return pd.Index([])

    min_lag = valid_lag['days_lag'].min()
    winners = valid_lag[valid_lag['days_lag'] == min_lag]

    return winners.index
 

 
