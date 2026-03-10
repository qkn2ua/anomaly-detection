#!/usr/bin/env python3
import io
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd

from baseline import BaselineManager
from detector import AnomalyDetector

logger = logging.getLogger("anomaly_pipeline.processor")

s3 = boto3.client("s3")

APP_DIR = os.path.dirname(os.path.abspath(__file__))
APP_LOG_PATH = os.path.join(APP_DIR, "app.log")

NUMERIC_COLS = ["temperature", "humidity", "pressure", "wind_speed"]


def process_file(bucket: str, key: str):
    logger.info("Starting processing for s3://%s/%s", bucket, key)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        logger.info("Loaded %s rows from %s", len(df), key)

        available_numeric_cols = [col for col in NUMERIC_COLS if col in df.columns]
        if not available_numeric_cols:
            raise ValueError(
                f"No configured numeric columns found in file {key}. "
                f"Expected one or more of: {NUMERIC_COLS}"
            )

        logger.info("Available numeric columns in %s: %s", key, available_numeric_cols)

        baseline_mgr = BaselineManager(bucket=bucket)
        baseline = baseline_mgr.load()

        for col in available_numeric_cols:
            clean_series = pd.to_numeric(df[col], errors="coerce").dropna()
            clean_values = clean_series.tolist()

            if clean_values:
                baseline = baseline_mgr.update(baseline, col, clean_values)
            else:
                logger.info("Column %s had no valid numeric values in %s", col, key)

        detector = AnomalyDetector(z_threshold=3.0, contamination=0.05)
        scored_df = detector.run(df, available_numeric_cols, baseline, method="both")

        output_key = key.replace("raw/", "processed/", 1)
        csv_buffer = io.StringIO()
        scored_df.to_csv(csv_buffer, index=False)

        s3.put_object(
            Bucket=bucket,
            Key=output_key,
            Body=csv_buffer.getvalue(),
            ContentType="text/csv"
        )
        logger.info("Uploaded processed CSV to s3://%s/%s", bucket, output_key)

        baseline_mgr.save(baseline)

        if os.path.exists(APP_LOG_PATH):
            s3.upload_file(APP_LOG_PATH, bucket, "logs/app.log")
            logger.info("Synced local log file to s3://%s/logs/app.log", bucket)
        else:
            logger.warning("Local log file not found at %s; skipping log sync.", APP_LOG_PATH)

        anomaly_count = int(scored_df["anomaly"].fillna(False).sum()) if "anomaly" in scored_df else 0

        summary = {
            "source_key": key,
            "output_key": output_key,
            "processed_at": datetime.utcnow().isoformat(),
            "total_rows": len(df),
            "anomaly_count": anomaly_count,
            "anomaly_rate": round(anomaly_count / len(df), 4) if len(df) > 0 else 0,
            "baseline_observation_counts": {
                col: baseline.get(col, {}).get("count", 0) for col in available_numeric_cols
            }
        }

        summary_key = output_key.replace(".csv", "_summary.json")
        s3.put_object(
            Bucket=bucket,
            Key=summary_key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json"
        )
        logger.info("Uploaded summary JSON to s3://%s/%s", bucket, summary_key)
        logger.info("Finished processing %s: %s/%s anomalies flagged", key, anomaly_count, len(df))

        return summary

    except Exception:
        logger.exception("Failed processing file s3://%s/%s", bucket, key)
        raise