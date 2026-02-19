import pandas as pd

def generate_payor_name_flag(row):
    sp_payor_name = row['payor_name']
    claims_payor_name = row['payer_name']

    sp_payor_name = str(sp_payor_name).lower()
    claims_payor_name = str(claims_payor_name).lower()

    return 1 if sp_payor_name == claims_payor_name else 0

def generate_payor_type_flag(row):
    sp_payor_type = row['payor_type']
    claims_payor_type = row['insurance_group']

    sp_payor_type = str(sp_payor_type).lower()
    claims_payor_type = str(claims_payor_type).lower()

    return 1 if sp_payor_type == claims_payor_type else 0

def generate_pbm_flag(row):
    sp_pbm = row['pbm']
    claims_pbm = row['pbm_processor']

    sp_pbm = str(sp_pbm).lower()
    claims_pbm = str(claims_pbm).lower()

    return 1 if sp_pbm == claims_pbm else 0


def generate_prior_flags(row):
    prior = str(row.get('prior_treatments_from_hcp', '')).strip().lower()
    rad_flag = row.get('radiation_flag', 0)
    chemo_flag = row.get('chemotherapy_flag', 0)
    surg_flag = row.get('surgery_flag', 0)

    if pd.isna(prior):
        return -1, -1, -1

    out = {
        "prior_radiation_treatment_flag": 0,
        "prior_chemotherapy_treatment_flag": 0,
        "prior_surgery_treatment_flag": 0,
    }

    if prior == "radiation" and rad_flag == 1:
        out["prior_radiation_treatment_flag"] = 1
    elif prior == "chemotherapy" and chemo_flag == 1:
        out["prior_chemotherapy_treatment_flag"] = 1
    elif prior == "surgery" and surg_flag == 1:
        out["prior_surgery_treatment_flag"] = 1
    else:
        # if you truly want -1 when no match:
        out = {k: -1 for k in out}

    return pd.Series(out)