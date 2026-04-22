from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

try:  # pragma: no cover - optional dependency
    import xgboost as xgb
except Exception:  # pragma: no cover
    xgb = None


@dataclass(slots=True)
class RankingFitResult:
    validation_scores: np.ndarray
    test_scores: np.ndarray
    metadata: dict[str, Any]
    model_object: Any


def _queue_groups(frame: pd.DataFrame) -> pd.Series:
    created = pd.to_datetime(frame["created_at"], utc=True, errors="coerce")
    return created.dt.strftime("%Y-%m-%d").fillna("unknown")


def _pairwise_training_data(
    frame: pd.DataFrame,
    feature_columns: list[str],
    *,
    max_negative_pairs_per_positive: int = 12,
) -> tuple[np.ndarray, np.ndarray]:
    groups = _queue_groups(frame)
    xs: list[np.ndarray] = []
    ys: list[int] = []
    rng = np.random.default_rng(42)
    for _, group in frame.groupby(groups, sort=False):
        positives = group[group["evaluation_label_is_sar"].astype(int) == 1]
        negatives = group[group["evaluation_label_is_sar"].astype(int) == 0]
        if positives.empty or negatives.empty:
            continue
        negative_scores = pd.to_numeric(negatives.get("total_amount_usd", 0.0), errors="coerce").fillna(0.0)
        negative_pool = negatives.assign(_sort_amount=negative_scores).sort_values("_sort_amount", ascending=False, kind="stable")
        for positive in positives.itertuples(index=False):
            take = min(len(negative_pool), max_negative_pairs_per_positive)
            if take <= 0:
                continue
            if take == len(negative_pool):
                sample = negative_pool
            else:
                head = negative_pool.head(max(1, take // 2))
                tail = negative_pool.iloc[len(head) :]
                random_take = min(len(tail), take - len(head))
                if random_take > 0:
                    sampled_idx = rng.choice(tail.index.to_numpy(), size=random_take, replace=False)
                    sample = pd.concat([head, tail.loc[sampled_idx]], axis=0)
                else:
                    sample = head
            positive_vector = np.asarray([float(getattr(positive, column)) for column in feature_columns], dtype=np.float32)
            for negative in sample.itertuples(index=False):
                negative_vector = np.asarray([float(getattr(negative, column)) for column in feature_columns], dtype=np.float32)
                diff = positive_vector - negative_vector
                xs.append(diff)
                ys.append(1)
                xs.append(-diff)
                ys.append(0)
    if not xs:
        return np.zeros((0, len(feature_columns)), dtype=np.float32), np.zeros(0, dtype=np.int32)
    return np.vstack(xs).astype(np.float32), np.asarray(ys, dtype=np.int32)


def fit_pairwise_ranker(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
) -> RankingFitResult:
    x_train, y_train = _pairwise_training_data(train_df, feature_columns)
    if len(y_train) == 0:
        raise ValueError("Pairwise ranker requires at least one positive/negative pair in the train split")
    model = Pipeline(
        steps=[
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=1200,
                    solver="lbfgs",
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )
    model.fit(x_train, y_train)
    validation_scores = model.decision_function(validation_df[feature_columns]).astype(np.float32)
    test_scores = model.decision_function(test_df[feature_columns]).astype(np.float32)
    return RankingFitResult(
        validation_scores=validation_scores,
        test_scores=test_scores,
        metadata={
            "family": "pairwise_logistic_ranker",
            "pairwise_examples": int(len(y_train)),
            "feature_columns": list(feature_columns),
        },
        model_object=model,
    )


def fit_lambdarank_candidate(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
) -> RankingFitResult | None:
    if xgb is None:
        return None
    train_groups = _queue_groups(train_df)
    validation_groups = _queue_groups(validation_df)
    group_sizes = train_groups.value_counts(sort=False).tolist()
    if not group_sizes:
        return None
    model = xgb.XGBRanker(
        objective="rank:pairwise",
        n_estimators=160,
        learning_rate=0.07,
        max_depth=5,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=0.3,
        random_state=42,
        tree_method="hist",
    )
    model.fit(
        train_df[feature_columns],
        train_df["evaluation_label_is_sar"].astype(int),
        group=group_sizes,
        eval_set=[(validation_df[feature_columns], validation_df["evaluation_label_is_sar"].astype(int))],
        eval_group=[validation_groups.value_counts(sort=False).tolist()],
        verbose=False,
    )
    validation_scores = np.asarray(model.predict(validation_df[feature_columns]), dtype=np.float32)
    test_scores = np.asarray(model.predict(test_df[feature_columns]), dtype=np.float32)
    return RankingFitResult(
        validation_scores=validation_scores,
        test_scores=test_scores,
        metadata={
            "family": "lambdarank",
            "feature_columns": list(feature_columns),
            "group_count_train": int(len(group_sizes)),
        },
        model_object=model,
    )


def fit_two_stage_reranker(
    train_df: pd.DataFrame,
    validation_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_columns: list[str],
    *,
    top_fraction: float = 0.25,
) -> RankingFitResult:
    stage1 = Pipeline(
        steps=[
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=1000,
                    solver="lbfgs",
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )
    stage1.fit(train_df[feature_columns], train_df["evaluation_label_is_sar"])
    train_stage1 = stage1.predict_proba(train_df[feature_columns])[:, 1]
    validation_stage1 = stage1.predict_proba(validation_df[feature_columns])[:, 1]
    test_stage1 = stage1.predict_proba(test_df[feature_columns])[:, 1]
    cutoff = float(np.quantile(train_stage1, max(0.0, min(1.0, 1.0 - top_fraction))))
    stage2_train = train_df.loc[train_stage1 >= cutoff, feature_columns].copy()
    stage2_labels = train_df.loc[train_stage1 >= cutoff, "evaluation_label_is_sar"].astype(int)
    if stage2_train.empty or stage2_labels.nunique() < 2:
        return RankingFitResult(
            validation_scores=validation_stage1.astype(np.float32),
            test_scores=test_stage1.astype(np.float32),
            metadata={
                "family": "two_stage_reranker",
                "fallback_to_stage1": True,
                "feature_columns": list(feature_columns),
            },
            model_object=stage1,
        )
    stage2 = Pipeline(
        steps=[
            ("scale", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    max_iter=1000,
                    solver="lbfgs",
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )
    stage2_train = stage2_train.assign(stage1_score=train_stage1[train_stage1 >= cutoff].astype(np.float32))
    stage2.fit(stage2_train, stage2_labels)

    def _rerank(frame: pd.DataFrame, stage1_scores: np.ndarray) -> np.ndarray:
        final = stage1_scores.astype(np.float32).copy()
        mask = stage1_scores >= cutoff
        if np.any(mask):
            stage2_frame = frame.loc[mask, feature_columns].copy()
            stage2_frame["stage1_score"] = stage1_scores[mask].astype(np.float32)
            final[mask] = final[mask] + 0.5 * stage2.predict_proba(stage2_frame)[:, 1].astype(np.float32)
        return final

    validation_scores = _rerank(validation_df, validation_stage1)
    test_scores = _rerank(test_df, test_stage1)
    return RankingFitResult(
        validation_scores=validation_scores,
        test_scores=test_scores,
        metadata={
            "family": "two_stage_reranker",
            "feature_columns": list(feature_columns),
            "stage2_cutoff_quantile": float(top_fraction),
            "stage2_train_rows": int(len(stage2_train)),
        },
        model_object={"stage1": stage1, "stage2": stage2, "cutoff": cutoff},
    )
