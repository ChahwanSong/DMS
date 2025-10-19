"""Structured logging utilities for DMS services."""
from __future__ import annotations

import json
import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional

from .config import LOG_TIME_FORMAT


class JsonFormatter(logging.Formatter):
    """Simple JSON formatter for DMS logs."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: Dict[str, Any] = {
            "timestamp": self.formatTime(record, LOG_TIME_FORMAT),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key.startswith("_dms_"):
                payload[key[5:]] = value
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(
    name: str,
    *,
    level: int = logging.INFO,
    log_file: Optional[str] = None,
    max_bytes: int = 20 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
    """Configure and return a logger with JSON formatting."""

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    formatter = JsonFormatter()
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file:
        rotating = RotatingFileHandler(log_file, maxBytes=max_bytes, backupCount=backup_count)
        rotating.setFormatter(formatter)
        logger.addHandler(rotating)

    return logger


def log_progress(
    logger: logging.Logger,
    *,
    request_id: str,
    agent_id: str,
    bytes_transferred: int,
    total_bytes: int,
    state: str,
    detail: Optional[str] = None,
) -> None:
    """Emit a structured progress log entry."""

    extra = {
        "_dms_request_id": request_id,
        "_dms_agent_id": agent_id,
        "_dms_bytes_transferred": bytes_transferred,
        "_dms_total_bytes": total_bytes,
        "_dms_state": state,
    }
    if detail:
        extra["_dms_detail"] = detail
    logger.info("progress", extra=extra)
