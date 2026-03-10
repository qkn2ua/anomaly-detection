# app.py
import io
import json
import logging
import os
from datetime import datetime

import boto3
import pandas as pd
import requests
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request

from baseline import BaselineManager
from processor import process_file

APP_DIR = os.path.dirname(os.path.abspath(__file__))
APP_LOG_PATH = os.path.join(APP_DIR, "app.log")


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("anomaly_pipeline")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s"
        )

        file_handler = logging.FileHandler(APP_LOG_PATH)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        logger.propagate = False

    return logger


logger = setup_logging()

app = FastAPI(title="Anomaly Detection Pipeline")

s3 = boto3.client("s3")

BUCKET_NAME = os.getenv("BUCKET_NAME")
if not BUCKET_NAME:
    raise RuntimeError("BUCKET_NAME environment variable is not set.")


@app.post("/notify")
async def handle_sns(request: Request, background_tasks: BackgroundTasks):
    """
    Handles SNS SubscriptionConfirmation and Notification messages.
    Notification messages contain an S3 event as a JSON string in body["Message"].
    """
    try:
        body = await request.json()
        msg_type = request.headers.get("x-amz-sns-message-type")

        logger.info("Received SNS request with message type: %s", msg_type)

        if msg_type == "SubscriptionConfirmation":
            confirm_url = body.get("SubscribeURL")
            if not confirm_url:
                logger.error("SubscriptionConfirmation missing SubscribeURL.")
                raise HTTPException(status_code=400, detail="Missing SubscribeURL.")

            logger.info("Confirming SNS subscription.")
            response = requests.get(confirm_url, timeout=10)
            response.raise_for_status()
            logger.info("SNS subscription confirmed successfully.")
            return {"status": "confirmed"}

        if msg_type == "Notification":
            raw_message = body.get("Message")
            if not raw_message:
                logger.warning("SNS notification missing Message field.")
                return {"status": "ignored", "reason": "missing message"}

            s3_event = json.loads(raw_message)
            records = s3_event.get("Records", [])

            queued = 0
            for record in records:
                key = (
                    record.get("s3", {})
                    .get("object", {})
                    .get("key")
                )

                if not key:
                    logger.warning("Skipping SNS record with missing S3 object key.")
                    continue

                logger.info("SNS notification references key: %s", key)

                if key.startswith("raw/") and key.endswith(".csv"):
                    background_tasks.add_task(process_file, BUCKET_NAME, key)
                    queued += 1
                    logger.info("Queued background processing for key: %s", key)
                else:
                    logger.info("Ignoring non-raw/non-csv key: %s", key)

            return {"status": "ok", "queued_files": queued}

        logger.warning("Unsupported SNS message type received: %s", msg_type)
        return {"status": "ignored", "reason": f"unsupported message type: {msg_type}"}

    except HTTPException:
        raise
    except json.JSONDecodeError:
        logger.exception("Failed to parse SNS message JSON.")
        raise HTTPException(status_code=400, detail="Invalid SNS JSON payload.")
    except requests.RequestException:
        logger.exception("Failed to confirm SNS subscription.")
        raise HTTPException(status_code=500, detail="SNS subscription confirmation failed.")
    except Exception:
        logger.exception("Unexpected error while handling SNS message.")
        raise HTTPException(status_code=500, detail="Unexpected SNS handler error.")


@app.get("/anomalies/recent")
def get_recent_anomalies(limit: int = 50):
    """
    Return rows flagged as anomalies across the 10 most recent processed files.
    """
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/")

        keys = sorted(
            [
                obj["Key"]
                for page in pages
                for obj in page.get("Contents", [])
                if obj["Key"].endswith(".csv")
            ],
            reverse=True,
        )[:10]

        all_anomalies = []
        for key in keys:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            df = pd.read_csv(io.BytesIO(response["Body"].read()))

            if "anomaly" in df.columns:
                flagged = df[df["anomaly"] == True].copy()
                if not flagged.empty:
                    flagged["source_file"] = key
                    all_anomalies.append(flagged)

        if not all_anomalies:
            return {"count": 0, "anomalies": []}

        combined = pd.concat(all_anomalies, ignore_index=True).head(limit)
        logger.info("Returning %s recent anomalies.", len(combined))

        return {"count": len(combined), "anomalies": combined.to_dict(orient="records")}

    except Exception:
        logger.exception("Failed to fetch recent anomalies.")
        raise HTTPException(status_code=500, detail="Failed to fetch recent anomalies.")


@app.get("/anomalies/summary")
def get_anomaly_summary():
    """
    Aggregate anomaly rates across all processed files using their summary JSONs.
    """
    try:
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix="processed/")

        summaries = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("_summary.json"):
                    response = s3.get_object(Bucket=BUCKET_NAME, Key=obj["Key"])
                    summaries.append(json.loads(response["Body"].read()))

        if not summaries:
            return {"message": "No processed files yet."}

        total_rows = sum(s.get("total_rows", 0) for s in summaries)
        total_anomalies = sum(s.get("anomaly_count", 0) for s in summaries)

        result = {
            "files_processed": len(summaries),
            "total_rows_scored": total_rows,
            "total_anomalies": total_anomalies,
            "overall_anomaly_rate": round(total_anomalies / total_rows, 4) if total_rows > 0 else 0,
            "most_recent": sorted(
                summaries,
                key=lambda x: x.get("processed_at", ""),
                reverse=True
            )[:5],
        }

        logger.info("Returning anomaly summary for %s processed files.", len(summaries))
        return result

    except Exception:
        logger.exception("Failed to generate anomaly summary.")
        raise HTTPException(status_code=500, detail="Failed to generate anomaly summary.")


@app.get("/baseline/current")
def get_current_baseline():
    """
    Show the current per-channel statistics the detector is working from.
    """
    try:
        baseline_mgr = BaselineManager(bucket=BUCKET_NAME)
        baseline = baseline_mgr.load()

        channels = {}
        for channel, stats in baseline.items():
            if channel == "last_updated":
                continue

            channels[channel] = {
                "observations": stats.get("count", 0),
                "mean": round(stats.get("mean", 0.0), 4),
                "std": round(stats.get("std", 0.0), 4),
                "baseline_mature": stats.get("count", 0) >= 30,
            }

        logger.info("Returning current baseline with %s channels.", len(channels))
        return {
            "last_updated": baseline.get("last_updated"),
            "channels": channels,
        }

    except Exception:
        logger.exception("Failed to fetch current baseline.")
        raise HTTPException(status_code=500, detail="Failed to fetch current baseline.")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "bucket": BUCKET_NAME,
        "timestamp": datetime.utcnow().isoformat()
    }