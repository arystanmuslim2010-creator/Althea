"""
Production ML training entrypoint: time-based split, labels, LightGBM, calibration, product metrics.

Usage:
  From backend: python scripts/train_model.py [--data path/to/alerts.csv] [--config config/ml.yaml]
  Or: PYTHONPATH=. python scripts/train_model.py --data data/alerts.csv

Prints: dataset stats, split date ranges, class imbalance, PR-AUC, TP retention, suppression, Brier, ECE.
Saves: backend/artifacts/models/<timestamp>/
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure backend root on path
_backend = Path(__file__).resolve().parent.parent
if str(_backend) not in sys.path:
    sys.path.insert(0, str(_backend))


def main() -> int:
    parser = argparse.ArgumentParser(description="Train AML alert yield model (time split, LGBM, calibration)")
    parser.add_argument("--data", type=str, default="", help="Path to alerts CSV (required columns: time, label/disposition)")
    parser.add_argument("--config", type=str, default="", help="Path to ml.yaml (default: backend/config/ml.yaml)")
    parser.add_argument("--out-dir", type=str, default="", help="Artifact output dir (default: backend/artifacts/models/<timestamp>)")
    parser.add_argument("--target", type=str, default="y_sar", choices=["y_sar", "y_escalated"], help="Target column")
    args = parser.parse_args()

    import pandas as pd
    import yaml

    config_path = args.config or str(_backend / "config" / "ml.yaml")
    if not Path(config_path).exists():
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 1

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    col = (cfg.get("labels", {}) or {}).get("disposition_column") or "disposition"

    data_path = args.data
    if not data_path or not Path(data_path).exists():
        # Demo: create minimal synthetic data so script runs
        print("No --data path or file not found; generating minimal synthetic data for demo.", file=sys.stderr)
        n = 2000
        import numpy as np
        np.random.seed(cfg.get("random_seed", 42))
        ts = pd.date_range("2024-01-01", periods=n, freq="h")
        df = pd.DataFrame({
            "alert_created_at": ts,
            "alert_id": [f"A{i:06d}" for i in range(n)],
            "entity_id": np.random.choice([f"E{i}" for i in range(100)], n),
            "rule_id": np.random.choice(["R001", "R002", "R003"], n),
            "disposition": np.random.choice(["FP", "FP", "FP", "SAR", "ESCALATED"], n),
            "amount_dev": np.random.randn(n).astype(float),
            "velocity_dev": np.random.randn(n).astype(float),
            "activity_dev": np.random.randn(n).astype(float),
        })
        df["y_sar"] = (df["disposition"] == "SAR").astype(int)
        df["y_escalated"] = (df["disposition"].isin(["SAR", "ESCALATED"])).astype(int)
    else:
        df = pd.read_csv(data_path)
        if df.empty:
            print("Data is empty.", file=sys.stderr)
            return 1

    # Labels
    from src.ml.labels import compute_labels
    try:
        df, _ = compute_labels(df, config_path=config_path)
    except ValueError as e:
        if "disposition" in str(e).lower() or "label" in str(e).lower():
            print("Adding synthetic y_sar/y_escalated from disposition-like column for demo.", file=sys.stderr)
            if "synthetic_true_suspicious" in df.columns:
                df["y_sar"] = (df["synthetic_true_suspicious"].astype(str).str.strip().str.lower() == "yes").astype(int)
                df["y_escalated"] = df["y_sar"]
            else:
                print(e, file=sys.stderr)
                return 1
        else:
            raise

    time_cfg = cfg.get("time_split", {})
    time_col = time_cfg.get("time_column") or None
    candidates = time_cfg.get("time_column_candidates", ["alert_created_at", "alert_date", "created_at", "timestamp"])
    val_months = time_cfg.get("validation_window_months", 1)
    test_months = time_cfg.get("test_window_months", 1)

    # Time split
    from src.ml.split import time_split
    try:
        train_df, val_df, test_df = time_split(
            df,
            time_col=time_col,
            val_window=val_months,
            test_window=test_months,
            time_column_candidates=candidates,
        )
    except ValueError as e:
        print("Time split failed:", e, file=sys.stderr)
        return 1

    print("Dataset stats:")
    print("  total rows:", len(df))
    print("  train:", len(train_df), "| val:", len(val_df), "| test:", len(test_df))
    print("Split date ranges (approximate):")
    tcol = candidates[0] if candidates else "timestamp"
    for name, d in [("train", train_df), ("val", val_df), ("test", test_df)]:
        if tcol in d.columns and len(d) > 0:
            ts = pd.to_datetime(d[tcol], errors="coerce").dropna()
            if len(ts) > 0:
                print(f"  {name}: {ts.min()} to {ts.max()}")

    target = args.target
    y_train = train_df[target]
    y_val = val_df[target]
    y_test = test_df[target]
    pos = y_train.sum()
    neg = len(y_train) - pos
    print("Class imbalance (train):", "pos=", int(pos), "neg=", int(neg), "ratio(neg/pos)=", f"{neg/max(1,pos):.2f}")

    # Feature matrix (use numeric columns present in all splits)
    feature_cols = cfg.get("feature_columns")
    if not feature_cols:
        feature_cols = ["amount_dev", "velocity_dev", "activity_dev", "amount_z", "velocity_z", "activity_z"]
    available = [c for c in feature_cols if c in train_df.columns and c in val_df.columns and c in test_df.columns]
    if not available:
        available = [c for c in train_df.select_dtypes(include=["number"]).columns if c not in ("y_sar", "y_escalated")]
    if not available:
        print("No feature columns available. Add numeric features or set feature_columns in config.", file=sys.stderr)
        return 1

    X_train = train_df[available].fillna(0)
    X_val = val_df[available].fillna(0)
    X_test = test_df[available].fillna(0)

    from src.ml.imbalance import compute_scale_pos_weight
    scale_pos_weight = compute_scale_pos_weight(y_train) if cfg.get("imbalance", {}).get("use_scale_pos_weight", True) else 1.0

    model_cfg = cfg.get("model", {})
    monotonic = model_cfg.get("monotonic_constraints") or {}
    from src.ml.model import train_lgbm, predict_proba, save_artifact, get_shap_values, top_contributing_features
    from src.ml.calibration import fit_calibrator, apply_calibrator
    from src.ml.calibration_metrics import brier_score, ece
    from src.ml.metrics import (
        pr_auc,
        tp_retention_at_suppression,
        suppression_at_tp_retention,
        precision_at_k_percent,
        roc_auc_optional,
    )

    seed = cfg.get("random_seed", 42)
    model = train_lgbm(
        X_train,
        y_train,
        X_val=X_val,
        y_val=y_val,
        feature_names=available,
        scale_pos_weight=scale_pos_weight,
        monotonic_constraints=monotonic,
        random_state=seed,
        n_estimators=model_cfg.get("n_estimators", 200),
        max_depth=model_cfg.get("max_depth", 8),
    )

    # Raw scores on val and test
    val_scores = predict_proba(model, X_val)
    test_scores = predict_proba(model, X_test)

    # Calibrate on val only
    cal_method = cfg.get("calibration", {}).get("method", "isotonic")
    calibrator = fit_calibrator(val_scores, y_val, method=cal_method)
    test_scores_cal = apply_calibrator(calibrator, test_scores)

    # Metrics on test (calibrated)
    print("\nMetrics (test set, calibrated scores):")
    print("  PR-AUC:", f"{pr_auc(y_test, test_scores_cal):.4f}")
    roc = roc_auc_optional(y_test, test_scores_cal)
    if roc is not None:
        print("  ROC-AUC:", f"{roc:.4f}")
    for rate in cfg.get("metrics", {}).get("suppression_rates", [0.1, 0.2, 0.3]):
        ret = tp_retention_at_suppression(y_test, test_scores_cal, rate)
        print(f"  TP retention at suppression {rate:.0%}:", f"{ret:.4f}")
    target_ret = cfg.get("metrics", {}).get("tp_retention_target", 0.98)
    supp = suppression_at_tp_retention(y_test, test_scores_cal, retention_target=target_ret)
    print(f"  Suppression at TP retention {target_ret:.0%}:", f"{supp:.4f}")
    print("  Precision@10%:", f"{precision_at_k_percent(y_test, test_scores_cal, 0.1):.4f}")
    print("  Precision@20%:", f"{precision_at_k_percent(y_test, test_scores_cal, 0.2):.4f}")
    print("Calibration (test):")
    print("  Brier:", f"{brier_score(y_test, test_scores_cal):.4f}")
    print("  ECE:", f"{ece(y_test, test_scores_cal, n_bins=10):.4f}")

    # Artifacts
    out_dir = args.out_dir or str(_backend / "artifacts" / "models" / pd.Timestamp.now().strftime("%Y%m%d_%H%M%S"))
    out_dir_path = Path(out_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)
    feature_version = cfg.get("artifacts", {}).get("feature_version_prefix", "v1") + "_time_safe"
    save_artifact(
        model,
        out_dir_path,
        feature_names=available,
        feature_version=feature_version,
        monotonic_constraints=monotonic,
        metadata={
            "target": target,
            "calibration_method": cal_method,
            "brier": brier_score(y_test, test_scores_cal),
            "ece": ece(y_test, test_scores_cal, n_bins=10),
            "pr_auc": pr_auc(y_test, test_scores_cal),
            "suppression_at_tp_retention": supp,
        },
    )
    # Save calibrator (pickle) for serving
    if calibrator is not None:
        import pickle
        with open(out_dir_path / "calibrator.pkl", "wb") as f:
            pickle.dump(calibrator, f)

    # Save model as joblib for production scoring (_try_load_lgbm expects model_lgbm.joblib)
    import joblib as _joblib
    _joblib.dump(model, out_dir_path / "model_lgbm.joblib")
    if calibrator is not None:
        _joblib.dump(calibrator, out_dir_path / "calibrator.joblib")

    # Update backend/artifacts/models/latest symlink (or copy on Windows)
    latest_dir = _backend / "artifacts" / "models" / "latest"
    latest_dir.parent.mkdir(parents=True, exist_ok=True)
    import os as _os
    if latest_dir.is_symlink() or latest_dir.exists():
        if latest_dir.is_symlink():
            latest_dir.unlink()
        else:
            import shutil as _shutil
            _shutil.rmtree(latest_dir)
    try:
        latest_dir.symlink_to(out_dir_path.resolve())
    except (OSError, NotImplementedError):
        import shutil as _shutil
        _shutil.copytree(str(out_dir_path), str(latest_dir))
    print(f"  Latest artifacts symlinked to: {latest_dir}")

    # -- Two-stage cascade (optional, controlled by ml.yaml two_stage config) --
    two_stage_cfg = cfg.get("two_stage", {})
    if two_stage_cfg.get("enabled", False):
        print("\n-- Two-stage cascade training --")

        # Stage 1: Escalation model (trained on all data)
        from src.ml.labels import compute_labels

        df_labeled, _ = compute_labels(df, config_path=config_path, disposition_column=col)
        y_escalated_train = (
            df_labeled.loc[train_df.index, "y_escalated"] if "y_escalated" in df_labeled.columns else None
        )

        if y_escalated_train is not None and y_escalated_train.nunique() > 1:
            from src.ml.imbalance import compute_scale_pos_weight

            spw_stage1 = compute_scale_pos_weight(y_escalated_train)
            model_stage1 = train_lgbm(
                X_train,
                y_escalated_train,
                X_val=X_val,
                y_val=df_labeled.loc[val_df.index, "y_escalated"] if len(val_df) > 0 else None,
                feature_names=available,
                scale_pos_weight=spw_stage1,
                random_state=seed,
            )
            print(
                f"  Stage 1 (escalation): scale_pos_weight={spw_stage1:.2f}, "
                f"pos_rate={y_escalated_train.mean():.3f}"
            )

            # Stage 2: SAR model - trained only on escalated alerts
            if two_stage_cfg.get("stage2_train_on_escalated_only", True):
                escalated_mask_train = y_escalated_train >= 0.5
                if escalated_mask_train.sum() >= 50:
                    X_train_s2 = X_train.loc[escalated_mask_train]
                    y_sar_train_s2 = df_labeled.loc[train_df.index[escalated_mask_train], "y_sar"]
                    spw_stage2 = compute_scale_pos_weight(y_sar_train_s2)
                    model_stage2 = train_lgbm(
                        X_train_s2,
                        y_sar_train_s2,
                        feature_names=available,
                        scale_pos_weight=spw_stage2,
                        random_state=seed,
                    )
                    print(
                        f"  Stage 2 (SAR on escalated): n_train={escalated_mask_train.sum()}, "
                        f"scale_pos_weight={spw_stage2:.2f}"
                    )
                    # Save stage 2 artifact
                    import joblib

                    joblib.dump(model_stage2, out_dir_path / "model_stage2_sar.joblib")
                    print(f"  Stage 2 model saved to: {out_dir_path / 'model_stage2_sar.joblib'}")
                else:
                    print(
                        f"  Stage 2 skipped: only {escalated_mask_train.sum()} escalated "
                        f"training samples (need >= 50)"
                    )

            # Save stage 1 artifact
            import joblib

            joblib.dump(model_stage1, out_dir_path / "model_stage1_escalation.joblib")
            print(f"  Stage 1 model saved to: {out_dir_path / 'model_stage1_escalation.joblib'}")
        else:
            print("  Stage 1 skipped: y_escalated not available or single-class")
    else:
        print("\nTwo-stage cascade disabled (set two_stage.enabled: true in ml.yaml to enable)")
    print("\nArtifacts saved to:", out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
