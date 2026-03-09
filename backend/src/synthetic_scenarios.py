"""
Deterministic synthetic scenario dataset for AML overlay demonstration.

⚠️  SYNTHETIC LABELS — NOT PRODUCTION GROUND TRUTH ⚠️
=======================================================
The ``synthetic_true_suspicious`` flag produced by this module is assigned
**programmatically** using rule-based pattern injection (structuring counts,
dormant-account bursts, rapid-withdrawal windows, flow-through ratios, etc.).
It is **NOT** sourced from real SAR filings, law-enforcement records, or
analyst review outcomes.

Implications:
- Model metrics (precision, recall, AUC) measure rule-consistency, not
  real-world AML detection capability.
- Scenario labels are fully deterministic and seeded for reproducibility;
  they carry no information about actual criminal behaviour.

Label evolution path toward production readiness:
  synthetic (this module)
      → analyst TP/FP disposition (OUTCOME_COLUMN pipeline)
          → SAR outcome feedback (strongest signal)
              → retraining on verified labels

See config.SYNTHETIC_LABEL_WARNING for the canonical warning string used
throughout the application.
"""
from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd

from . import config


def generate_customer_profiles(seed: int = 42) -> pd.DataFrame:
    """
    Generate deterministic customer profile table with 80-150 customers.
    
    Returns:
        DataFrame with columns: user_id, segment, income_band, expected_monthly_turnover,
        home_country, risk_flags, account_age_days, dormant_days
    """
    rng = np.random.default_rng(seed)
    
    # Total customers: 120
    n_customers = 120
    
    segments = ["retail_low", "retail_high", "smb", "corporate"]
    seg_probs = [0.45, 0.30, 0.20, 0.05]
    
    income_bands = ["low", "mid", "high"]
    income_probs = [0.40, 0.45, 0.15]
    
    countries = ["KZ", "TR", "AE", "GB", "US", "DE", "FR", "RU", "CN", "IN"]
    country_probs = [0.25, 0.15, 0.10, 0.10, 0.08, 0.08, 0.07, 0.07, 0.05, 0.05]
    
    risk_flag_options = [
        "",
        "student",
        "cash_intensive_business",
        "crypto_user",
        "student;crypto_user",
        "cash_intensive_business;crypto_user",
    ]
    risk_flag_probs = [0.60, 0.15, 0.10, 0.08, 0.04, 0.03]
    
    profiles = []
    for user_id in range(n_customers):
        seg_idx = rng.choice(len(segments), p=seg_probs)
        segment = segments[seg_idx]
        
        income_idx = rng.choice(len(income_bands), p=income_probs)
        income_band = income_bands[income_idx]
        
        country_idx = rng.choice(len(countries), p=country_probs)
        home_country = countries[country_idx]
        
        risk_flag_idx = rng.choice(len(risk_flag_options), p=risk_flag_probs)
        risk_flags = risk_flag_options[risk_flag_idx]
        
        # Expected monthly turnover based on segment and income
        if segment == "retail_low":
            if income_band == "low":
                expected_turnover = rng.uniform(200, 800)
            elif income_band == "mid":
                expected_turnover = rng.uniform(800, 2000)
            else:
                expected_turnover = rng.uniform(2000, 5000)
        elif segment == "retail_high":
            if income_band == "low":
                expected_turnover = rng.uniform(1000, 3000)
            elif income_band == "mid":
                expected_turnover = rng.uniform(3000, 8000)
            else:
                expected_turnover = rng.uniform(8000, 20000)
        elif segment == "smb":
            expected_turnover = rng.uniform(5000, 50000)
        else:  # corporate
            expected_turnover = rng.uniform(50000, 500000)
        
        account_age_days = rng.integers(30, 2000)
        dormant_days = rng.integers(0, 90) if rng.random() > 0.3 else rng.integers(30, 180)
        
        profiles.append({
            "user_id": user_id,
            "segment": segment,
            "income_band": income_band,
            "expected_monthly_turnover": round(expected_turnover, 2),
            "home_country": home_country,
            "risk_flags": risk_flags,
            "account_age_days": account_age_days,
            "dormant_days": dormant_days,
        })
    
    return pd.DataFrame(profiles)


def generate_scenario_transactions(profiles_df: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    """
    Generate deterministic transaction data with embedded AML scenarios.
    
    Args:
        profiles_df: Customer profiles DataFrame
        seed: Random seed for reproducibility
        
    Returns:
        DataFrame with transactions including all required columns and scenario markers
    """
    rng = np.random.default_rng(seed)
    
    now = pd.Timestamp.utcnow()
    transactions = []
    
    # Scenario assignments: map user_id to typology
    scenario_users = {
        # structuring_threshold_avoidance: 3 cases
        "structuring_threshold_avoidance": [10, 25, 45],
        # profile_change_before_large_tx: 3 cases
        "profile_change_before_large_tx": [15, 30, 50],
        # unusual_spending_pattern: 3 cases
        "unusual_spending_pattern": [5, 20, 35],
        # low_buyer_diversity: 3 cases (SMB segment)
        "low_buyer_diversity": [60, 70, 80],
        # flow_through_equal_in_out: 3 cases
        "flow_through_equal_in_out": [12, 28, 42],
        # high_risk_country_corridor: 3 cases
        "high_risk_country_corridor": [18, 33, 48],
        # immediate_withdrawal_private_wallet: 3 cases
        "immediate_withdrawal_private_wallet": [22, 38, 55],
        # large_cash_inconsistent_profile: 3 cases
        "large_cash_inconsistent_profile": [8, 17, 27],
        # dormant_account_reactivation: 3 cases
        "dormant_account_reactivation": [14, 29, 44],
        # frequent_crypto_fiat_conversions: 3 cases
        "frequent_crypto_fiat_conversions": [11, 26, 41],
    }
    
    # Generate baseline transactions for all users
    for _, profile in profiles_df.iterrows():
        user_id = int(profile["user_id"])
        segment = profile["segment"]
        expected_turnover = profile["expected_monthly_turnover"]
        home_country = profile["home_country"]
        risk_flags = profile["risk_flags"]
        dormant_days = profile["dormant_days"]
        
        # Determine if this user has a scenario
        user_typology = None
        for typ, users in scenario_users.items():
            if user_id in users:
                user_typology = typ
                break
        
        # Baseline transaction count
        if user_typology:
            tx_count = 50  # More transactions for scenario users
        else:
            tx_count = rng.integers(20, 40)
        
        # Generate baseline transactions
        base_amount = expected_turnover / 30.0  # Daily average
        base_gap = 24.0 if segment == "retail_low" else (12.0 if segment == "retail_high" else 6.0)
        
        start_time = now - pd.Timedelta(days=90)
        if dormant_days > 0:
            start_time = now - pd.Timedelta(days=dormant_days + 30)
        
        # Track profile update state across transactions
        profile_updated_recently = False
        
        for tx_idx in range(tx_count):
            # Time progression
            if tx_idx == 0:
                tx_time = start_time
            else:
                gap_hours = base_gap * (1 + rng.normal(0, 0.3))
                gap_hours = max(0.1, gap_hours)
                tx_time = tx_time + pd.Timedelta(hours=gap_hours)
            
            if tx_time > now:
                break
            
            # Baseline amount
            amount = base_amount * (1 + rng.normal(0, 0.4))
            amount = max(1.0, amount)
            
            # Default direction and channel
            direction = rng.choice(["in", "out"], p=[0.6, 0.4])
            channel = rng.choice(["card", "bank_transfer", "cash"], p=[0.5, 0.4, 0.1])
            
            # Default counterparty
            counterparty_country = home_country if rng.random() > 0.2 else rng.choice(["US", "GB", "DE"])
            counterparty_type = rng.choice(["individual", "merchant", "business"], p=[0.4, 0.4, 0.2])
            
            # Initialize scenario-specific fields
            customer_profile_update_recent = 1 if profile_updated_recently else 0
            is_dormant_reactivation = 0
            fx_conversion = 0
            cash_tx = 1 if channel == "cash" else 0
            merchant_id = None
            wallet_type = None
            
            # Apply scenario logic
            synthetic_true_suspicious = "No"
            typology = "none"
            rule_tag = None
            
            if user_typology == "structuring_threshold_avoidance":
                # Series of out transactions 9000-9999 within 60 days
                if direction == "out" and (now - tx_time).days <= 60:
                    amount = rng.uniform(9000, 9999)
                    channel = rng.choice(["bank_transfer", "cash"], p=[0.6, 0.4])
                    synthetic_true_suspicious = "Yes"
                    typology = "structuring_threshold_avoidance"
                    rule_tag = "STRUCT"
            
            elif user_typology == "profile_change_before_large_tx":
                # Profile update followed by large out transaction
                if tx_idx == tx_count - 2:
                    profile_updated_recently = True
                    customer_profile_update_recent = 1
                elif tx_idx == tx_count - 1 and profile_updated_recently:
                    direction = "out"
                    amount = expected_turnover * 3.0
                    channel = "bank_transfer"
                    customer_profile_update_recent = 1
                    synthetic_true_suspicious = "Yes"
                    typology = "profile_change_before_large_tx"
                    rule_tag = "PROF_CHG"
            
            elif user_typology == "unusual_spending_pattern":
                # Spending way above baseline
                if direction == "out" and tx_idx >= tx_count - 5:
                    amount = expected_turnover * 5.0
                    channel = rng.choice(["card", "bank_transfer"])
                    synthetic_true_suspicious = "Yes"
                    typology = "unusual_spending_pattern"
                    rule_tag = "UNUSUAL"
            
            elif user_typology == "low_buyer_diversity" and segment == "smb":
                # Merchant receives many payments from 1-2 buyers
                if direction == "in":
                    merchant_id = f"MERCHANT_{user_id}"
                    counterparty_type = "merchant"
                    # Only 1-2 unique counterparties
                    counterparty_id = f"CP_{user_id % 2}"
                    if tx_idx >= tx_count - 20:
                        synthetic_true_suspicious = "Yes"
                        typology = "low_buyer_diversity"
                        rule_tag = "LOW_DIV"
                else:
                    counterparty_id = f"CP_{rng.integers(0, 10)}"
            
            elif user_typology == "flow_through_equal_in_out":
                # Equal in/out in short period
                if tx_idx >= tx_count - 10:
                    if tx_idx % 2 == 0:
                        direction = "in"
                        amount = 10000
                    else:
                        direction = "out"
                        amount = 10000
                    channel = "bank_transfer"
                    synthetic_true_suspicious = "Yes"
                    typology = "flow_through_equal_in_out"
                    rule_tag = "FLOW"
            
            elif user_typology == "high_risk_country_corridor":
                # Transactions to/from high-risk countries
                if tx_idx >= tx_count - 8:
                    high_risk_countries = getattr(config, "HIGH_RISK_COUNTRIES", ["AE", "TR", "PA", "CY", "RU"])
                    counterparty_country = rng.choice(high_risk_countries)
                    direction = rng.choice(["in", "out"])
                    amount = base_amount * 2.0
                    synthetic_true_suspicious = "Yes"
                    typology = "high_risk_country_corridor"
                    rule_tag = "HR_COUNTRY"
            
            elif user_typology == "immediate_withdrawal_private_wallet":
                # In -> immediately out to private wallet
                if tx_idx >= tx_count - 4:
                    if tx_idx % 2 == 0:
                        direction = "in"
                        amount = 50000
                        channel = "bank_transfer"
                    else:
                        direction = "out"
                        amount = 50000
                        channel = "crypto"
                        wallet_type = "private_wallet"
                        # Adjust time for immediate withdrawal (within 30 minutes)
                        tx_time = tx_time + pd.Timedelta(minutes=rng.integers(5, 30))
                    synthetic_true_suspicious = "Yes"
                    typology = "immediate_withdrawal_private_wallet"
                    rule_tag = "IMM_WD"
            
            elif user_typology == "large_cash_inconsistent_profile":
                # Large cash transaction inconsistent with profile
                if "student" in risk_flags and direction == "out" and tx_idx >= tx_count - 3:
                    amount = 20000
                    channel = "cash"
                    cash_tx = 1
                    synthetic_true_suspicious = "Yes"
                    typology = "large_cash_inconsistent_profile"
                    rule_tag = "CASH_INCONS"
            
            elif user_typology == "dormant_account_reactivation":
                # Dormant account suddenly active
                if dormant_days > 30 and tx_idx >= tx_count - 10:
                    is_dormant_reactivation = 1
                    amount = base_amount * 3.0
                    direction = rng.choice(["in", "out"])
                    synthetic_true_suspicious = "Yes"
                    typology = "dormant_account_reactivation"
                    rule_tag = "DORMANT"
            
            elif user_typology == "frequent_crypto_fiat_conversions":
                # Many small crypto-fiat conversions
                if tx_idx >= tx_count - 15:
                    fx_conversion = 1
                    amount = rng.uniform(100, 500)
                    channel = rng.choice(["crypto", "bank_transfer"])
                    direction = rng.choice(["in", "out"])
                    synthetic_true_suspicious = "Yes"
                    typology = "frequent_crypto_fiat_conversions"
                    rule_tag = "FX_FREQ"
            
            # Compute time_gap and num_transactions (required fields)
            # This must be done after tx_time is finalized
            if tx_idx == 0:
                time_gap = base_gap
                num_transactions = 1
            else:
                # Find previous transaction for this user
                user_txs = [t for t in transactions if t.get("user_id") == user_id]
                if user_txs:
                    prev_time = user_txs[-1]["ts"]
                    gap_hours = (tx_time - prev_time).total_seconds() / 3600.0
                    time_gap = max(0.01, gap_hours)
                else:
                    time_gap = base_gap
                
                # Rolling 24h count for this user
                window_start = tx_time - pd.Timedelta(hours=24)
                user_recent = [t for t in transactions 
                              if t.get("user_id") == user_id and t.get("ts", pd.Timestamp.min) >= window_start]
                num_transactions = len(user_recent) + 1
            
            # Baseline level assignment (for evidence)
            baseline_level = "user" if tx_idx >= 20 else ("segment" if tx_idx >= 5 else "global")
            baseline_window = "30d"
            n_hist = tx_idx + 1 if baseline_level == "user" else (tx_idx + 1 if baseline_level == "segment" else 100)
            
            tx_record = {
                "user_id": user_id,
                "segment": segment,
                "amount": round(amount, 2),
                "time_gap": round(time_gap, 4),
                "num_transactions": num_transactions,
                "ts": tx_time,
                "direction": direction,
                "channel": channel,
                "counterparty_country": counterparty_country,
                "counterparty_type": counterparty_type,
                "merchant_id": merchant_id,
                "wallet_type": wallet_type,
                "customer_profile_update_recent": customer_profile_update_recent,
                "is_dormant_reactivation": is_dormant_reactivation,
                "fx_conversion": fx_conversion,
                "cash_tx": cash_tx,
                "typology": typology,
                "synthetic_true_suspicious": synthetic_true_suspicious,
                "rule_tag": rule_tag,
                "baseline_level": baseline_level,
                "baseline_window": baseline_window,
                "n_hist": n_hist,
            }
            
            # Ensure counterparty_id exists
            if "counterparty_id" not in locals():
                counterparty_id = f"CP_{rng.integers(0, 100)}"
            tx_record["counterparty_id"] = counterparty_id
            
            # Add country field for high_risk_country rule
            if user_typology == "high_risk_country_corridor":
                tx_record["country"] = counterparty_country
            elif "country" not in tx_record:
                tx_record["country"] = counterparty_country
            
            transactions.append(tx_record)
    
    df = pd.DataFrame(transactions)
    
    # Ensure required columns exist
    if "counterparty_id" not in df.columns:
        df["counterparty_id"] = None
    
    if "country" not in df.columns:
        df["country"] = df.get("counterparty_country", "UNKNOWN")
    
    # Fill missing values
    df["typology"] = df["typology"].fillna("none").replace("", "none")
    df["synthetic_true_suspicious"] = df["synthetic_true_suspicious"].fillna("No").replace("", "No")
    
    # Ensure baseline fields are present
    if "baseline_level" not in df.columns:
        df["baseline_level"] = "global"
    if "baseline_window" not in df.columns:
        df["baseline_window"] = "30d"
    if "n_hist" not in df.columns:
        df["n_hist"] = 0
    
    return df


def get_scenario_cases() -> List[Dict]:
    """
    Return list of scenario cases for demonstration.
    
    Returns:
        List of dicts with keys: case_name, typology, description, expected_user_ids
    """
    cases = [
        # structuring_threshold_avoidance
        {
            "case_name": "Structuring under 10k - retail_high #1",
            "typology": "structuring_threshold_avoidance",
            "description": "User 10: Series of out transactions 9000-9999 within 60 days to avoid reporting threshold",
            "expected_user_ids": [10],
        },
        {
            "case_name": "Structuring under 10k - smb #2",
            "typology": "structuring_threshold_avoidance",
            "description": "User 25: Multiple near-threshold out transfers in short period",
            "expected_user_ids": [25],
        },
        {
            "case_name": "Structuring under 10k - corporate #3",
            "typology": "structuring_threshold_avoidance",
            "description": "User 45: Corporate account with structuring pattern",
            "expected_user_ids": [45],
        },
        # profile_change_before_large_tx
        {
            "case_name": "Profile change + large transfer - retail #1",
            "typology": "profile_change_before_large_tx",
            "description": "User 15: Profile updated recently, then large outbound transfer",
            "expected_user_ids": [15],
        },
        {
            "case_name": "Profile change + large transfer - retail_high #2",
            "typology": "profile_change_before_large_tx",
            "description": "User 30: Suspicious profile update followed by large transaction",
            "expected_user_ids": [30],
        },
        {
            "case_name": "Profile change + large transfer - smb #3",
            "typology": "profile_change_before_large_tx",
            "description": "User 50: Business account with profile change pattern",
            "expected_user_ids": [50],
        },
        # unusual_spending_pattern
        {
            "case_name": "Unusual spending - retail_low #1",
            "typology": "unusual_spending_pattern",
            "description": "User 5: Spending 5x above expected baseline",
            "expected_user_ids": [5],
        },
        {
            "case_name": "Unusual spending - retail_high #2",
            "typology": "unusual_spending_pattern",
            "description": "User 20: High-value retail account with anomalous spending",
            "expected_user_ids": [20],
        },
        {
            "case_name": "Unusual spending - smb #3",
            "typology": "unusual_spending_pattern",
            "description": "User 35: SMB account with spending pattern deviation",
            "expected_user_ids": [35],
        },
        # low_buyer_diversity
        {
            "case_name": "Low buyer diversity - merchant #1",
            "typology": "low_buyer_diversity",
            "description": "User 60 (SMB): Merchant receiving payments from only 1-2 buyers",
            "expected_user_ids": [60],
        },
        {
            "case_name": "Low buyer diversity - merchant #2",
            "typology": "low_buyer_diversity",
            "description": "User 70 (SMB): Concentrated buyer pattern",
            "expected_user_ids": [70],
        },
        {
            "case_name": "Low buyer diversity - merchant #3",
            "typology": "low_buyer_diversity",
            "description": "User 80 (SMB): Suspicious merchant activity",
            "expected_user_ids": [80],
        },
        # flow_through_equal_in_out
        {
            "case_name": "Flow-through equal in/out - retail #1",
            "typology": "flow_through_equal_in_out",
            "description": "User 12: Equal in/out amounts in short window (layering pattern)",
            "expected_user_ids": [12],
        },
        {
            "case_name": "Flow-through equal in/out - retail_high #2",
            "typology": "flow_through_equal_in_out",
            "description": "User 28: Rapid equal-value transfers",
            "expected_user_ids": [28],
        },
        {
            "case_name": "Flow-through equal in/out - smb #3",
            "typology": "flow_through_equal_in_out",
            "description": "User 42: Business account with flow-through pattern",
            "expected_user_ids": [42],
        },
        # high_risk_country_corridor
        {
            "case_name": "High-risk country corridor - retail #1",
            "typology": "high_risk_country_corridor",
            "description": "User 18: Transactions to/from high-risk jurisdictions",
            "expected_user_ids": [18],
        },
        {
            "case_name": "High-risk country corridor - retail_high #2",
            "typology": "high_risk_country_corridor",
            "description": "User 33: Cross-border to sanctioned countries",
            "expected_user_ids": [33],
        },
        {
            "case_name": "High-risk country corridor - smb #3",
            "typology": "high_risk_country_corridor",
            "description": "User 48: Business transactions to high-risk countries",
            "expected_user_ids": [48],
        },
        # immediate_withdrawal_private_wallet
        {
            "case_name": "Immediate withdrawal to private wallet - retail #1",
            "typology": "immediate_withdrawal_private_wallet",
            "description": "User 22: Large in -> immediate out to private crypto wallet",
            "expected_user_ids": [22],
        },
        {
            "case_name": "Immediate withdrawal to private wallet - retail_high #2",
            "typology": "immediate_withdrawal_private_wallet",
            "description": "User 38: Rapid crypto withdrawal pattern",
            "expected_user_ids": [38],
        },
        {
            "case_name": "Immediate withdrawal to private wallet - smb #3",
            "typology": "immediate_withdrawal_private_wallet",
            "description": "User 55: Business account with immediate crypto withdrawal",
            "expected_user_ids": [55],
        },
        # large_cash_inconsistent_profile
        {
            "case_name": "Large cash - student profile #1",
            "typology": "large_cash_inconsistent_profile",
            "description": "User 8: Student profile with large cash transactions",
            "expected_user_ids": [8],
        },
        {
            "case_name": "Large cash - student profile #2",
            "typology": "large_cash_inconsistent_profile",
            "description": "User 17: Inconsistent cash activity for profile type",
            "expected_user_ids": [17],
        },
        {
            "case_name": "Large cash - student profile #3",
            "typology": "large_cash_inconsistent_profile",
            "description": "User 27: Profile mismatch with cash volume",
            "expected_user_ids": [27],
        },
        # dormant_account_reactivation
        {
            "case_name": "Dormant reactivation - retail #1",
            "typology": "dormant_account_reactivation",
            "description": "User 14: Account dormant 30+ days, then sudden activity burst",
            "expected_user_ids": [14],
        },
        {
            "case_name": "Dormant reactivation - retail_high #2",
            "typology": "dormant_account_reactivation",
            "description": "User 29: Long-dormant account reactivated with high activity",
            "expected_user_ids": [29],
        },
        {
            "case_name": "Dormant reactivation - smb #3",
            "typology": "dormant_account_reactivation",
            "description": "User 44: Business account reactivation pattern",
            "expected_user_ids": [44],
        },
        # frequent_crypto_fiat_conversions
        {
            "case_name": "Frequent crypto-fiat conversions - retail #1",
            "typology": "frequent_crypto_fiat_conversions",
            "description": "User 11: Many small crypto-fiat conversions (structuring)",
            "expected_user_ids": [11],
        },
        {
            "case_name": "Frequent crypto-fiat conversions - retail_high #2",
            "typology": "frequent_crypto_fiat_conversions",
            "description": "User 26: High-frequency FX conversion pattern",
            "expected_user_ids": [26],
        },
        {
            "case_name": "Frequent crypto-fiat conversions - smb #3",
            "typology": "frequent_crypto_fiat_conversions",
            "description": "User 41: Business account with frequent crypto conversions",
            "expected_user_ids": [41],
        },
    ]
    
    return cases
