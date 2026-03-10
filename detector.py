#!/usr/bin/env python3
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest


class AnomalyDetector:
    def __init__(self, z_threshold: float = 3.0, contamination: float = 0.05):
        self.z_threshold = z_threshold
        self.contamination = contamination

    def zscore_flag(
        self,
        values: pd.Series,
        mean: float,
        std: float
    ) -> pd.Series:
        """
        Flag values more than z_threshold standard deviations from the
        established baseline mean. Returns a Series of z-scores.
        """
        if std is None or std <= 0:
            return pd.Series([0.0] * len(values), index=values.index)

        return (values - mean).abs() / std

    def isolation_forest_flag(self, df: pd.DataFrame, numeric_cols: list[str]) -> tuple[np.ndarray, np.ndarray]:
        """
        Multivariate anomaly detection across all numeric channels simultaneously.
        IsolationForest returns -1 for anomalies, 1 for normal points.
        Scores closer to -1 indicate stronger anomalies.
        """
        if not numeric_cols:
            raise ValueError("No numeric columns available for IsolationForest.")

        model = IsolationForest(
            contamination=self.contamination,
            random_state=42,
            n_estimators=100
        )

        X = df[numeric_cols].copy()

        for col in numeric_cols:
            X[col] = pd.to_numeric(X[col], errors="coerce")

        X = X.fillna(X.median(numeric_only=True))

        labels = model.fit_predict(X)
        scores = model.decision_function(X)

        return labels, scores

    def run(
        self,
        df: pd.DataFrame,
        numeric_cols: list[str],
        baseline: dict,
        method: str = "both"
    ) -> pd.DataFrame:
        result = df.copy()

        available_numeric_cols = [col for col in numeric_cols if col in df.columns]

        if not available_numeric_cols:
            raise ValueError("Input dataframe does not contain any configured numeric columns.")

        if method in ("zscore", "both"):
            for col in available_numeric_cols:
                stats = baseline.get(col)

                if stats and stats.get("count", 0) >= 30:
                    z_scores = self.zscore_flag(df[col], stats["mean"], stats.get("std", 0.0))
                    result[f"{col}_zscore"] = z_scores.round(4)
                    result[f"{col}_zscore_flag"] = z_scores > self.z_threshold
                else:
                    result[f"{col}_zscore"] = None
                    result[f"{col}_zscore_flag"] = None

        if method in ("isolation", "both"):
            labels, scores = self.isolation_forest_flag(df, available_numeric_cols)
            result["if_label"] = labels
            result["if_score"] = np.round(scores, 4)
            result["if_flag"] = labels == -1

        if method == "both":
            zscore_flag_columns = [
                f"{col}_zscore_flag"
                for col in available_numeric_cols
                if f"{col}_zscore_flag" in result.columns
            ]

            valid_zscore_cols = [
                col_name
                for col_name in zscore_flag_columns
                if result[col_name].notna().any()
            ]

            if valid_zscore_cols:
                any_zscore = result[valid_zscore_cols].fillna(False).any(axis=1)
                result["anomaly"] = any_zscore | result["if_flag"]
            else:
                result["anomaly"] = result["if_flag"]

        elif method == "zscore":
            zscore_flag_columns = [
                f"{col}_zscore_flag"
                for col in available_numeric_cols
                if f"{col}_zscore_flag" in result.columns
            ]

            valid_zscore_cols = [
                col_name
                for col_name in zscore_flag_columns
                if result[col_name].notna().any()
            ]

            if valid_zscore_cols:
                result["anomaly"] = result[valid_zscore_cols].fillna(False).any(axis=1)
            else:
                result["anomaly"] = False

        elif method == "isolation":
            result["anomaly"] = result["if_flag"]

        return result