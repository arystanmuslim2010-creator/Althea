"""State synchronization for case management persistence."""
from __future__ import annotations

import time
from typing import Dict, List

import pandas as pd

from .. import config


def init_state(session_state):
    """
    Initialize session state variables if they don't exist.
    
    Args:
        session_state: Streamlit session_state object
    """
    if "cases" not in session_state:
        session_state["cases"] = {}
    if "case_counter" not in session_state:
        session_state["case_counter"] = 1
    if "audit_log" not in session_state:
        session_state["audit_log"] = []


def sync_df_with_cases(df: pd.DataFrame, session_state) -> pd.DataFrame:
    """
    Synchronize dataframe with case state from session_state.
    
    Rules:
    - If df does not have columns case_id or case_status, create them
    - Default for alerts not in any case: case_id = "", case_status = "NEW"
    - If alert belongs to a case:
      - df.case_id = that case_id
      - df.case_status = "IN_CASE" unless case status is CLOSED_TP/CLOSED_FP/ESCALATED
    
    Mapping:
    - case["status"] == "NEW" -> "IN_CASE"
    - "ESCALATED" -> "ESCALATED"
    - "CLOSED_TP" -> "CLOSED_TP"
    - "CLOSED_FP" -> "CLOSED_FP"
    
    Args:
        df: DataFrame with alert_id column
        session_state: Streamlit session_state with cases dict
        
    Returns:
        DataFrame with case_id and case_status synchronized
    """
    df = df.copy()
    
    # Ensure alert_id exists
    if "alert_id" not in df.columns:
        return df
    
    # Create case_id and case_status columns if they don't exist
    if "case_id" not in df.columns:
        df["case_id"] = ""
    if "case_status" not in df.columns:
        df["case_status"] = config.CASE_STATUS_NEW
    
    # Initialize with defaults (alerts not in any case)
    df["case_id"] = ""
    df["case_status"] = config.CASE_STATUS_NEW
    
    # Get cases from session_state
    cases = session_state.get("cases", {})
    
    # Map case status to alert case_status
    status_mapping = {
        "OPEN": config.CASE_STATUS_IN_CASE,
        "NEW": config.CASE_STATUS_IN_CASE,
        "ASSIGNED": config.CASE_STATUS_IN_CASE,
        "REOPENED": config.CASE_STATUS_IN_CASE,
        "ESCALATED": config.CASE_STATUS_ESCALATED,
        "CLOSED_TP": config.CASE_STATUS_CLOSED_TP,
        "CLOSED_FP": config.CASE_STATUS_CLOSED_FP,
    }
    
    # For each case, update df for all included alert_ids
    for case_id, case_data in cases.items():
        alert_ids = case_data.get("alert_ids", [])
        case_status = case_data.get("status", "NEW")
        
        # Map case status to alert case_status
        alert_case_status = status_mapping.get(case_status, config.CASE_STATUS_IN_CASE)
        
        # Update df for all alerts in this case
        mask = df["alert_id"].isin(alert_ids)
        df.loc[mask, "case_id"] = case_id
        df.loc[mask, "case_status"] = alert_case_status
    
    return df
