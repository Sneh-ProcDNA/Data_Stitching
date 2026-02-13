# Patient Identification & Validation Pipeline

## Overview

The Patient Identification & Validation Pipeline is a modular, production-ready data processing framework designed to identify and validate patients using structured rule-based logic across SP and Komodo datasets.

The pipeline combines:

* **SQL** for base table creation and core patient matching
* **Python** for advanced validation and business rule implementation
* **YAML** configuration for centralized rule and environment management

This architecture ensures scalability, maintainability, and production readiness.

---

## Architecture Overview

The pipeline follows a layered validation framework:
```
Core Rules
  ↓
Diagnosis Validation
  ↓
Therapy History
  ↓
Dispense & Utilization
  ↓
Signal-Based Supporters
  ↓
Complementary Supporters
```

Each layer progressively refines the eligible patient population.

---

## Project Structure

**Root Folder:** `patient_identification`

### Directory Layout
```
patient_identification/
├── configs/
│   ├── config.yaml
│   ├── thresholds.yaml
│   └── logging.yaml
├── sql/
│   ├── base/
│   │   ├── sp_base.sql
│   │   └── komodo_base.sql
│   ├── core/
│   │   └── core_rules.sql
│   └── staging/
│       ├── diagnosis_stage.sql
│       └── therapy_stage.sql
├── src/
│   ├── common/
│   │   ├── config_loader.py
│   │   ├── db.py
│   │   ├── logger.py
│   │   └── constants.py
│   ├── modules/
│   │   ├── diagnosis_validation.py
│   │   ├── therapy_history.py
│   │   ├── dispense_utilization.py
│   │   ├── signal_supporters.py
│   │   └── complementary_supporters.py
│   └── pipeline/
│       └── main.py
├── tests/
│   ├── test_diagnosis_validation.py
│   ├── test_therapy_history.py
│   └── test_core_rules.py
├── .env
├── requirements.txt
└── README.md
```

---

## Pipeline Execution Flow

### Step 1: Base Table Creation (SQL)

* Standardizes SP and Komodo raw datasets
* Removes null identifiers
* Applies initial data cleaning

### Step 2: Core Rule Matching (SQL)

Matches patients based on:

* Prescriber NPI
* Age with ±1 tolerance
* Gender
* State

Creates eligible base population.

### Step 3: Diagnosis Validation (Python)

* Applies lookback window logic
* Checks diagnosis frequency threshold
* Evaluates persistence rules

### Step 4: Therapy History (Python)

* Identifies prior therapy presence
* Calculates therapy intensity
* Evaluates sequencing patterns

### Step 5: Dispense & Utilization (Python)

* Aggregates quantity dispensed
* Computes days of supply
* Evaluates utilization timelines

### Step 6: Signal-Based Supporters (Python)

* Applies disease-specific procedural signals
* Incorporates lab or diagnostic indicators

### Step 7: Complementary Supporters (Python)

* Integrates payer validation logic
* Applies secondary eligibility rules

### Step 8: Final Output

* Writes validated patient table to analytics schema

---

## Configuration Management

### 1. Environment Variables (`.env`)

Stores sensitive credentials and environment settings such as:

* `DB_HOST`
* `DB_NAME`
* `DB_USER`
* `DB_PASSWORD`
* `ENV` (dev or prod)

**⚠️ This file should never be committed to version control.**

### 2. `config.yaml`

Stores system-level configuration such as:

* Schema names
* Table names
* Lookback durations
* Pipeline parameters

### 3. `thresholds.yaml`

Stores business logic thresholds such as:

* Minimum diagnosis frequency
* Persistence duration
* Therapy intensity cutoffs
* Claim thresholds

## Setup and Execution

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Configure Environment

Create a `.env` file in the project root with database credentials:
```env
DB_HOST=your_database_host
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
ENV=dev
```

### Step 3: Run the Pipeline

Execute from the project root:
```bash
python -m src.pipeline.main
```

---

## Running Tests

### Run All Tests
```bash
pytest
```

### Run a Specific Module Test
```bash
pytest tests/test_diagnosis_validation.py
```

---

## Business Logic Layers Summary

| Layer | Description |
|-------|-------------|
| **Core Rules** | Initial patient matching |
| **Diagnosis Validation** | Clinical validation through frequency and persistence checks |
| **Therapy History** | Treatment pattern and sequencing evaluation |
| **Dispense & Utilization** | Medication usage and timeline analysis |
| **Signal Supporters** | Additional clinical signals and procedural indicators |
| **Complementary Supporters** | Payer-based and secondary validation logic |

---





