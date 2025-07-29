# Process Mining in Psychiatry

This repository contains all code, Jupyter notebooks, and SQL scripts associated with the paper:

**“Process Mining of Pharmacological Treatment Pathways in Treatment-Resistant Depression (TRD)”**

by Zeinab Soleimani et al.

---

## Overview

This project analyzes pharmacological treatment trajectories in patients with major depressive disorder (MDD), with a focus on identifying and characterizing treatment-resistant depression (TRD) using process mining techniques. All analyses are performed on electronic health record (EHR) data standardized to the OMOP Common Data Model (CDM).

Our pipeline includes:

- **Cohort selection** from OMOP CDM based on MDD and antidepressant exposure criteria
- **Event log generation** with semantic enrichment and TRD labeling
- **Process discovery and analysis** using PM4Py and related tools

For a detailed description of the methodology and results, please see the accompanying manuscript.

---

## Repository Structure

- `Discovery_Analysis.ipynb` — Process mining and analysis notebook
- `event_log_generation.ipynb` — Code for event log construction, data cleaning, TRD labeling, and abstraction
- `cohort_selection.sql` — SQL script for extracting the MDD cohort and drug exposures from OMOP CDM

---

## Getting Started

1. **Clone this repository:**
    ```bash
    git clone https://github.com/ZeinabSoleimani/Process-Mining-in-Psychiatry1.git
    ```
2. **Set up your Python environment:**  
   Required packages are listed in the notebook headers. We recommend using `conda` or `pip` to install:
    - `pm4py`
    - `pandas`
    - `notebook` (for Jupyter)
    - (others as needed for your environment)
3. **Prepare your data:**  
   The analysis assumes access to OMOP CDM-formatted EHR data with appropriate permissions. Example data structures and requirements are described in the notebooks.
4. **Run the notebooks in order:**
    - `cohort_selection.sql` (run via your database client)
    - `event_log_generation.ipynb`
    - `Discovery_Analysis.ipynb`

---

## Citation

If you use this code or methodology in your own work, please cite our paper:

