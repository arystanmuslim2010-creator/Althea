from __future__ import annotations

import logging
from typing import NoReturn

from fastapi import HTTPException


def safe_http_error(status_code: int, public_message: str) -> HTTPException:
    return HTTPException(status_code=int(status_code), detail=str(public_message or "Request failed"))


def log_and_raise_internal(
    logger: logging.Logger,
    exc: Exception,
    message: str = "Internal server error",
) -> NoReturn:
    logger.exception(message, exc_info=exc)
    raise safe_http_error(500, message)
