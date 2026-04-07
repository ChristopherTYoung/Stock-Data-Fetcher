"""Utilities for selecting tickers that require quarterly financial refresh."""

from datetime import datetime
import logging
import os
from typing import Any, Dict, List, Optional

from sqlalchemy import bindparam, create_engine, text

try:
    from .logging_config import setup_logging
except ImportError:
    from logging_config import setup_logging

logger = setup_logging("stock-orchestrator", level=logging.INFO)

DATABASE_URL = os.getenv("DATABASE_URL", "")


def _quarter_index(value: datetime) -> int:
    return (value.month - 1) // 3


def _requires_quarterly_refresh(
    last_updated: Optional[datetime],
    has_missing_quarterly_data: bool,
    now: datetime,
) -> bool:
    if has_missing_quarterly_data:
        return True

    if last_updated is None:
        return True

    last_quarter = _quarter_index(last_updated)
    current_quarter = _quarter_index(now)
    return (
        last_updated.year < now.year
        or (last_updated.year == now.year and last_quarter < current_quarter)
    )


def get_tickers_requiring_quarterly_update(tickers: List[str]) -> List[str]:
    """Return ticker symbols that should run quarterly-financial refresh this quarter."""
    if not tickers:
        return []

    if not DATABASE_URL:
        logger.warning(
            "DATABASE_URL not set in orchestrator; quarterly update queue will be empty"
        )
        return []

    symbols = [ticker.strip().upper() for ticker in tickers if ticker and ticker.strip()]
    if not symbols:
        return []

    query = (
        text(
            """
            SELECT
                symbol,
                quarterly_financials_updated_at,
                eps,
                revenue_per_share,
                outstanding_shares,
                total_revenue,
                debt_to_equity
            FROM incrementum.stock
            WHERE symbol IN :symbols
            """
        )
        .bindparams(bindparam("symbols", expanding=True))
    )

    try:
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            connect_args={"options": "-csearch_path=incrementum,public"},
        )

        with engine.connect() as conn:
            rows = conn.execute(query, {"symbols": symbols}).mappings().all()

        engine.dispose()

        rows_by_symbol: Dict[str, Dict[str, Any]] = {
            row["symbol"]: row for row in rows
        }

        now = datetime.utcnow()
        quarterly_tickers: List[str] = []
        for symbol in symbols:
            row = rows_by_symbol.get(symbol)
            last_updated = row["quarterly_financials_updated_at"] if row else None
            has_missing_quarterly_data = row is None or any(
                row[column] is None
                for column in (
                    "eps",
                    "revenue_per_share",
                    "outstanding_shares",
                    "total_revenue",
                    "debt_to_equity",
                )
            )
            if _requires_quarterly_refresh(last_updated, has_missing_quarterly_data, now):
                quarterly_tickers.append(symbol)

        logger.info(
            "Quarterly queue selection complete: %s of %s tickers need quarterly updates",
            len(quarterly_tickers),
            len(symbols),
        )
        return quarterly_tickers

    except Exception as exc:
        logger.error("Failed to compute quarterly update tickers: %s", exc)
        return []
