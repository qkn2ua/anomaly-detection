#!/usr/bin/env python3
import json
import logging
import math
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger("anomaly_pipeline.baseline")
s3 = boto3.client("s3")


class BaselineManager:
    """
    Maintains a per-channel running baseline using Welford's online algorithm,
    which computes mean and variance incrementally without storing all past data.
    """

    def __init__(self, bucket: str, baseline_key: str = "state/baseline.json"):
        self.bucket = bucket
        self.baseline_key = baseline_key

    def load(self) -> dict:
        try:
            response = s3.get_object(Bucket=self.bucket, Key=self.baseline_key)
            baseline = json.loads(response["Body"].read())
            logger.info("Loaded baseline from s3://%s/%s", self.bucket, self.baseline_key)
            return baseline

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in {"NoSuchKey", "404"}:
                logger.info(
                    "Baseline file does not exist yet at s3://%s/%s. Starting fresh.",
                    self.bucket,
                    self.baseline_key,
                )
                return {}

            logger.exception(
                "Failed to load baseline from s3://%s/%s",
                self.bucket,
                self.baseline_key,
            )
            raise

        except json.JSONDecodeError:
            logger.exception(
                "Baseline file at s3://%s/%s is not valid JSON.",
                self.bucket,
                self.baseline_key,
            )
            raise

    def save(self, baseline: dict):
        try:
            baseline["last_updated"] = datetime.utcnow().isoformat()
            s3.put_object(
                Bucket=self.bucket,
                Key=self.baseline_key,
                Body=json.dumps(baseline, indent=2),
                ContentType="application/json",
            )
            logger.info("Saved baseline to s3://%s/%s", self.bucket, self.baseline_key)

        except Exception:
            logger.exception(
                "Failed to save baseline to s3://%s/%s",
                self.bucket,
                self.baseline_key,
            )
            raise

    def update(self, baseline: dict, channel: str, new_values: list[float]) -> dict:
        """
        Welford's online algorithm for numerically stable mean and variance.
        Each channel tracks: count, mean, M2 (sum of squared deviations).
        Variance = M2 / count, std = sqrt(variance).
        """
        if channel not in baseline:
            baseline[channel] = {"count": 0, "mean": 0.0, "M2": 0.0}

        state = baseline[channel]

        for value in new_values:
            state["count"] += 1
            delta = value - state["mean"]
            state["mean"] += delta / state["count"]
            delta2 = value - state["mean"]
            state["M2"] += delta * delta2

        if state["count"] >= 2:
            variance = state["M2"] / state["count"]
            state["std"] = math.sqrt(variance)
        else:
            state["std"] = 0.0

        baseline[channel] = state
        logger.info(
            "Updated baseline for channel=%s count=%s mean=%.4f std=%.4f",
            channel,
            state["count"],
            state["mean"],
            state["std"],
        )
        return baseline

    def get_stats(self, baseline: dict, channel: str) -> Optional[dict]:
        return baseline.get(channel)