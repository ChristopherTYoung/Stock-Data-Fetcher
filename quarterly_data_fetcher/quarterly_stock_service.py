import logging
import os
import time
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf
from polygon import RESTClient
from sqlalchemy import select

from quarterly_data_fetcher.database import Stock, get_db
from quarterly_data_fetcher.logging_config import setup_logging

logger = setup_logging("quarterly-data-fetcher", level=logging.INFO)


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return max(float(raw), 0.0)
    except ValueError:
        logger.warning("Invalid %s value '%s'; using default %.2f", name, raw, default)
        return default


def _column_allowed(column_name: str) -> bool:
    return column_name in Stock.__table__.columns.keys()


def _to_builtin_number(value):
    if value is None:
        return None

    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, Decimal):
        return float(value)

    return value


def _to_two_decimal_numeric(value):
    numeric_value = _to_builtin_number(value)
    if numeric_value is None:
        return None
    return Decimal(str(numeric_value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _calculate_quarterly_metrics(
    ticker: str,
    yf_ticker_obj: Any,
    yf_info: Dict,
) -> Tuple[Optional[int], Optional[Decimal], Optional[Decimal], Optional[int], Optional[int], Optional[Decimal], Optional[datetime]]:
    try:
        quarterly_financials_updated_at = datetime.utcnow()
        eps_value = _to_builtin_number(yf_info.get("trailingEps"))
        previous_eps_value = None
        annual_eps_growth_rate = None
        outstanding_shares_value = _to_builtin_number(yf_info.get("sharesOutstanding"))
        total_revenue = _to_builtin_number(yf_info.get("totalRevenue"))
        revenue_per_share = None

        try:
            quarterly_financials = yf_ticker_obj.quarterly_financials
            if quarterly_financials is not None and not quarterly_financials.empty:
                if len(quarterly_financials.columns) >= 5:
                    quarters_ago = quarterly_financials.iloc[:, 4:5]
                    if "Net Income" in quarterly_financials.index:
                        net_income_4q_ago = quarters_ago.loc["Net Income"].values[0]
                        if net_income_4q_ago and net_income_4q_ago != 0:
                            quarterly_shares = _to_builtin_number(yf_info.get("sharesOutstanding"))
                            if quarterly_shares and quarterly_shares != 0:
                                previous_eps_value = Decimal(
                                    str(round(float(net_income_4q_ago) / float(quarterly_shares), 4))
                                )
                                logger.info(
                                    "[%s] calculated EPS from 4 quarters ago: %s",
                                    ticker,
                                    previous_eps_value,
                                )
        except Exception as error:
            logger.info("[%s] could not get quarterly EPS data: %s", ticker, error)

        if eps_value is not None and previous_eps_value not in (None, 0):
            try:
                annual_eps_growth_rate = ((float(eps_value) / float(previous_eps_value)) - 1.0) * 100.0
                logger.info("[%s] calculated annual_eps_growth_rate=%s", ticker, annual_eps_growth_rate)
            except Exception:
                annual_eps_growth_rate = None
                logger.warning("[%s] failed calculating annual_eps_growth_rate", ticker, exc_info=True)

        if total_revenue is not None and outstanding_shares_value not in (None, 0):
            try:
                shares_decimal = Decimal(str(outstanding_shares_value))
                if shares_decimal != 0:
                    revenue_per_share = (Decimal(str(total_revenue)) / shares_decimal).quantize(
                        Decimal("0.01"),
                        rounding=ROUND_HALF_UP,
                    )
                    logger.info("[%s] calculated revenue_per_share=%s", ticker, revenue_per_share)
            except Exception as error:
                logger.warning("[%s] failed calculating revenue_per_share: %s", ticker, error)

        debt_to_equity_value = _to_builtin_number(yf_info.get("debtToEquity"))

        return (
            int(round(_to_builtin_number(annual_eps_growth_rate))) if annual_eps_growth_rate is not None else None,
            _to_two_decimal_numeric(eps_value) if eps_value else None,
            revenue_per_share,
            outstanding_shares_value,
            total_revenue,
            _to_two_decimal_numeric(debt_to_equity_value) if debt_to_equity_value else None,
            quarterly_financials_updated_at,
        )

    except Exception as error:
        logger.warning("[%s] error in quarterly calculations: %s", ticker, error)
        return (None, None, None, None, None, None, None)


def update_quarterly_metrics_for_tickers(
    tickers: List[str],
    status_dict: Optional[Dict[str, int]] = None,
) -> int:
    if not tickers:
        return 0

    api_key = os.environ.get("POLYGON_API_KEY")
    if not api_key:
        logger.error("POLYGON_API_KEY not set, cannot fetch quarterly metrics")
        return 0

    stock_data = [
        {"symbol": ticker.strip().upper()}
        for ticker in tickers
        if isinstance(ticker, str) and ticker.strip()
    ]

    if not stock_data:
        return 0

    logger.info("[QUARTERLY] Starting to fetch quarterly metrics for %s stocks", len(stock_data))

    if status_dict:
        status_dict["total"] = len(stock_data)
        status_dict["progress"] = 0

    saved_count = 0
    error_count = 0
    stock_delay_seconds = _env_float("QUARTERLY_STOCK_DELAY_SECONDS", 5.0)

    logger.info(
        "[QUARTERLY] Sequential processing enabled with delay=%ss between stocks",
        stock_delay_seconds,
    )

    client = RESTClient(api_key)

    for idx, entry in enumerate(stock_data):
        ticker = entry.get("symbol")
        if not ticker:
            if status_dict:
                status_dict["progress"] = idx + 1
            continue

        try:
            yf_ticker = yf.Ticker(ticker)
            yf_info = yf_ticker.info
            (
                annual_eps_growth_rate,
                eps_value,
                revenue_per_share,
                outstanding_shares,
                total_revenue,
                debt_to_equity,
                quarterly_financials_updated_at,
            ) = _calculate_quarterly_metrics(
                ticker,
                yf_ticker,
                yf_info,
            )

            if quarterly_financials_updated_at is None:
                error_count += 1
            else:
                try:
                    details = client.get_ticker_details(ticker)
                except Exception as error:
                    logger.warning("[QUARTERLY] Error fetching details for %s: %s", ticker, error)
                    details = None

                company_name = (getattr(details, "name", ticker) or ticker) if details else ticker
                if isinstance(company_name, str) and len(company_name) > 100:
                    company_name = company_name[:100]

                defaults = {
                    "company_name": company_name,
                    "updated_at": datetime.now(),
                    "annual_eps_growth_rate": annual_eps_growth_rate,
                    "quarterly_financials_updated_at": quarterly_financials_updated_at,
                    "eps": eps_value,
                    "revenue_per_share": revenue_per_share,
                    "outstanding_shares": outstanding_shares,
                    "total_revenue": total_revenue,
                    "debt_to_equity": debt_to_equity,
                }

                with get_db() as db:
                    existing = db.execute(select(Stock).where(Stock.symbol == ticker)).first()

                    if existing:
                        db.execute(Stock.__table__.update().where(Stock.symbol == ticker).values(**defaults))
                    else:
                        insert_payload = {k: v for k, v in defaults.items() if _column_allowed(k)}
                        insert_payload["symbol"] = ticker
                        db.execute(Stock.__table__.insert().values(**insert_payload))
                    db.commit()

                saved_count += 1
                logger.info("[QUARTERLY] Saved quarterly metrics for %s", ticker)

        except Exception as error:
            error_count += 1
            if error_count <= 10:
                logger.error("[QUARTERLY] Error processing %s: %s", ticker, error)

        if status_dict:
            status_dict["progress"] = idx + 1

        if stock_delay_seconds > 0 and idx < len(stock_data) - 1:
            logger.debug("[QUARTERLY] Sleeping %.2fs before next stock", stock_delay_seconds)
            time.sleep(stock_delay_seconds)

    logger.info("[QUARTERLY] COMPLETE: Saved %s stocks with quarterly metrics", saved_count)
    logger.info("[QUARTERLY] Errors: %s", error_count)

    if status_dict:
        status_dict["progress"] = len(stock_data)
        status_dict["saved"] = saved_count
        status_dict["errors"] = error_count

    return saved_count
