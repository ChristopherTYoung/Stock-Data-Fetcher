import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional
from types import SimpleNamespace
import pandas as pd

from polygon import RESTClient
from stock_data_calculator.database import get_db, Stock, StockHistory
from stock_data_calculator.logging_config import setup_logging
from sqlalchemy import select
from stock_data_calculator.stock_calculator import StockCalculator

logger = setup_logging("stock-data-calculator", level=logging.INFO)


def fetch_new_stocks_from_polygon() -> List[Dict[str, str]]:
    api_key = os.environ.get('POLYGON_API_KEY')
    if not api_key or api_key.strip() == '':
        logger.error('POLYGON_API_KEY not set in environment')
        return []

    try:
        client = RESTClient(api_key)
        tickers = []

        for ticker in client.list_tickers(market='stocks', limit=1000):
            tickers.append({
                'symbol': ticker.ticker,
                'name': getattr(ticker, 'name', ticker.ticker),
            })

        logger.info("Fetched %s tickers from Polygon", len(tickers))
        return tickers

    except Exception as e:
        logger.exception("Error fetching tickers from Polygon: %s", e)
        return []


def _column_allowed(column_name: str) -> bool:
    # Only set attributes that exist on the SQLAlchemy model
    return column_name in Stock.__table__.columns.keys()


def _to_builtin_number(value):
    """Convert numpy/Decimal scalar values to plain Python numeric types."""
    if value is None:
        return None
    # numpy scalars expose item()
    if hasattr(value, 'item'):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, Decimal):
        return float(value)
    return value


def _to_cents(value):
    """Convert a dollar-denominated numeric value to integer cents."""
    numeric_value = _to_builtin_number(value)
    if numeric_value is None:
        return None
    return int(round(float(numeric_value) * 100))


def _to_percent_hundredths(value):
    """Convert a percentage value to integer hundredths of a percent."""
    numeric_value = _to_builtin_number(value)
    if numeric_value is None:
        return None
    return int(round(float(numeric_value) * 100))


def _to_two_decimal_numeric(value):
    """Convert a numeric value to Decimal rounded to two decimal places."""
    numeric_value = _to_builtin_number(value)
    if numeric_value is None:
        return None
    return Decimal(str(numeric_value)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def _load_history_dataframe(db, ticker: str, lookback_days: int = 370) -> pd.DataFrame:
    """Load the most recent stock history rows for a ticker into a DataFrame."""
    ticker = ticker.upper()
    start_date = datetime.utcnow() - timedelta(days=lookback_days)

    query = (
        select(StockHistory)
        .where(
            StockHistory.stock_symbol == ticker,
            StockHistory.day_and_time >= start_date,
        )
        .order_by(StockHistory.day_and_time.asc(), StockHistory.is_hourly.asc())
    )

    history_rows = db.execute(query).scalars().all()
    if not isinstance(history_rows, list):
        try:
            history_rows = list(history_rows)
        except TypeError:
            history_rows = []

    if not history_rows:
        history_rows = (
            db.execute(
                select(StockHistory)
                .where(StockHistory.stock_symbol == ticker)
                .order_by(StockHistory.day_and_time.asc(), StockHistory.is_hourly.asc())
            )
            .scalars()
            .all()
        )
        if not isinstance(history_rows, list):
            try:
                history_rows = list(history_rows)
            except TypeError:
                history_rows = []

    if not history_rows:
        return pd.DataFrame()

    history_df = pd.DataFrame(
        [
            {
                'timestamp': row.day_and_time,
                'open': _to_builtin_number(row.open_price) / 100.0,
                'high': _to_builtin_number(row.high) / 100.0,
                'low': _to_builtin_number(row.low) / 100.0,
                'close': _to_builtin_number(row.close_price) / 100.0,
                'volume': _to_builtin_number(row.volume),
            }
            for row in history_rows
        ]
    )
    history_df['timestamp'] = pd.to_datetime(history_df['timestamp'])
    history_df.set_index('timestamp', inplace=True)
    return history_df


def update_stocks_in_db_from_polygon(
    stock_data: List[Dict[str, Any]],
    status_dict: Optional[Dict[str, int]] = None,
) -> int:
    """
    Update stocks in database with regular (non-quarterly) metrics.
    This function updates: price, percent_change, high52, low52, P/E, PEG, P/S, debt_to_equity.
    Does NOT calculate quarterly-specific metrics like annual_eps_growth_rate.
    """
    api_key = os.environ.get('POLYGON_API_KEY')
    if not api_key:
        logger.error('POLYGON_API_KEY not set, cannot fetch metadata')
        return 0

    if not stock_data:
        logger.error('No stock data provided')
        return 0

    logger.info("[REGULAR] Starting to fetch metadata for %s stocks", len(stock_data))

    if status_dict:
        status_dict['total'] = len(stock_data)
        status_dict['progress'] = 0

    client = RESTClient(api_key)
    saved_count = 0
    error_count = 0

    for idx, entry in enumerate(stock_data):
        ticker = entry.get('symbol')
        if not ticker:
            continue

        try:
            # Get ticker details from Polygon
            details = client.get_ticker_details(ticker)

            list_date = None
            if hasattr(details, 'list_date') and details.list_date:
                try:
                    list_date = datetime.strptime(details.list_date, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    list_date = None

            company_name = getattr(details, 'name', ticker) or ticker
            if isinstance(company_name, str) and len(company_name) > 100:
                company_name = company_name[:100]

            defaults = {
                'company_name': company_name,
                'updated_at': datetime.now(),
            }

            optional_map = {
                'description': getattr(details, 'description', None),
                'market_cap': getattr(details, 'market_cap', None),
                'primary_exchange': getattr(details, 'primary_exchange', None),
                'type': getattr(details, 'type', None),
                'currency_name': getattr(details, 'currency_name', None),
                'cik': getattr(details, 'cik', None),
                'composite_figi': getattr(details, 'composite_figi', None),
                'share_class_figi': getattr(details, 'share_class_figi', None),
                'homepage_url': getattr(details, 'homepage_url', None),
                'total_employees': getattr(details, 'total_employees', None),
                'list_date': list_date,
                'locale': getattr(details, 'locale', None),
                'sic_code': getattr(details, 'sic_code', None),
                'sic_description': getattr(details, 'sic_description', None),
            }

            for k, v in optional_map.items():
                if _column_allowed(k):
                    defaults[k] = v

            # Upsert the stock row
            with get_db() as db:
                existing = db.execute(select(Stock).where(Stock.symbol == ticker)).first()

                history_df = _load_history_dataframe(db, ticker)

                if existing is not None:
                    stock_for_calc = existing[0]
                else:
                    stock_for_calc = SimpleNamespace(symbol=ticker, price=None, eps=None)

                calculated_price = StockCalculator.calculate_price(history_df, stock_for_calc)
                calculated_high52 = StockCalculator.calculate_high52(history_df, stock_for_calc)
                calculated_low52 = StockCalculator.calculate_low52(history_df, stock_for_calc)

                logger.info(
                    "[%s] market calculations: price=%s, high52=%s, low52=%s",
                    ticker,
                    calculated_price,
                    calculated_high52,
                    calculated_low52,
                )

                stock_for_percent_change = SimpleNamespace(
                    symbol=ticker,
                    price=_to_builtin_number(calculated_price),
                )
                calculated_percent_change = StockCalculator.calculate_percent_change(
                    history_df,
                    stock_for_percent_change,
                )
                logger.info("[%s] calculated percent_change=%s", ticker, calculated_percent_change)

                # Keep valuation calculations in dollar units even though the DB stores cents.
                current_price = _to_builtin_number(calculated_price)
                
                # Query database for existing financial data
                db_eps = None
                db_revenue_per_share = None
                db_annual_eps_growth_rate = None
                
                if existing is not None:
                    stock_obj = existing[0]
                    db_eps = _to_builtin_number(getattr(stock_obj, 'eps', None))
                    db_revenue_per_share = _to_builtin_number(getattr(stock_obj, 'revenue_per_share', None))
                    db_annual_eps_growth_rate = _to_builtin_number(getattr(stock_obj, 'annual_eps_growth_rate', None))
                
                # Calculate P/E using database EPS if available
                price_per_earnings_value = None
                if current_price is not None and db_eps not in (None, 0):
                    try:
                        price_per_earnings_value = Decimal(str(current_price)) / Decimal(str(db_eps))
                        logger.info(
                            "[%s] calculated price_per_earnings=%s using price=%s and database EPS=%s",
                            ticker,
                            price_per_earnings_value,
                            current_price,
                            db_eps,
                        )
                    except Exception as e:
                        logger.warning("[%s] failed calculating price_per_earnings: %s", ticker, e)
                else:
                    if db_eps is None:
                        logger.info("[%s] skipped price_per_earnings: no EPS in database", ticker)
                
                # Calculate P/S using database revenue_per_share if available
                price_per_sales_value = None
                if current_price is not None and db_revenue_per_share not in (None, 0):
                    try:
                        revenue_per_share_decimal = Decimal(str(db_revenue_per_share))
                        if revenue_per_share_decimal != 0:
                            price_per_sales_value = (
                                Decimal(str(current_price)) / revenue_per_share_decimal
                            ).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                            logger.info(
                                "[%s] calculated price_per_sales=%s using price=%s and database revenue_per_share=%s",
                                ticker,
                                price_per_sales_value,
                                current_price,
                                db_revenue_per_share,
                            )
                    except Exception as e:
                        logger.warning("[%s] failed calculating price_per_sales: %s", ticker, e)
                else:
                    if db_revenue_per_share is None:
                        logger.info("[%s] skipped price_per_sales: no revenue_per_share in database", ticker)
                
                # Calculate PEG using database annual_eps_growth_rate if available
                pe_per_growth_value = None
                if price_per_earnings_value is not None and db_annual_eps_growth_rate not in (None, 0):
                    try:
                        peg_ratio = (float(price_per_earnings_value) / float(db_annual_eps_growth_rate)) * 100
                        pe_per_growth_value = Decimal(str(peg_ratio))
                        logger.info(
                            "[%s] calculated PEG=%s using P/E=%s and growth_rate=%s",
                            ticker,
                            pe_per_growth_value,
                            price_per_earnings_value,
                            db_annual_eps_growth_rate,
                        )
                    except Exception as e:
                        logger.warning("[%s] failed calculating PEG: %s", ticker, e)
                else:
                    if db_annual_eps_growth_rate is None:
                        logger.info("[%s] skipped PEG: no annual_eps_growth_rate in database", ticker)

                defaults['price'] = _to_cents(calculated_price)
                defaults['high52'] = _to_cents(calculated_high52)
                defaults['low52'] = _to_cents(calculated_low52)
                defaults['percent_change'] = _to_percent_hundredths(calculated_percent_change)
                defaults['price_per_sales'] = _to_two_decimal_numeric(price_per_sales_value)
                defaults['price_per_earnings'] = _to_percent_hundredths(price_per_earnings_value) if price_per_earnings_value else None
                defaults['pe_per_growth'] = _to_percent_hundredths(pe_per_growth_value) if pe_per_growth_value else None
                
                # Detect candlestick pattern for the last candle
                last_candle_pattern = StockCalculator.calculate_last_candlestick_pattern(history_df, stock_for_calc)
                if last_candle_pattern and _column_allowed('last_candle'):
                    defaults['last_candle'] = last_candle_pattern
                    logger.info("[%s] detected candlestick pattern: %s", ticker, last_candle_pattern)

                if existing:
                    # Update only allowed columns
                    db.execute(
                        Stock.__table__.update().where(Stock.symbol == ticker).values(**defaults)
                    )
                else:
                    insert_payload = {k: v for k, v in defaults.items() if _column_allowed(k)}
                    insert_payload['symbol'] = ticker
                    db.execute(Stock.__table__.insert().values(**insert_payload))
                db.commit()

            saved_count += 1

        except Exception as e:
            error_count += 1
            if error_count <= 10:
                logger.error("[REGULAR] Error fetching %s: %s", ticker, e)

        if (idx + 1) % 100 == 0:
            progress_msg = (
                f"[REGULAR] Processed {idx + 1}/{len(stock_data)} stocks... "
                f"(Saved: {saved_count}, Errors: {error_count})"
            )
            logger.info(progress_msg)
            if status_dict:
                status_dict['progress'] = idx + 1
                status_dict['saved'] = saved_count
                status_dict['errors'] = error_count

    logger.info("[REGULAR] COMPLETE: Saved %s stocks to database", saved_count)
    logger.info("[REGULAR] Errors: %s", error_count)

    if status_dict:
        status_dict['progress'] = len(stock_data)
        status_dict['saved'] = saved_count
        status_dict['errors'] = error_count

    return saved_count


def fetch_and_update_symbols() -> int:
    data = fetch_new_stocks_from_polygon()
    if not data:
        logger.error("No data fetched from Polygon")
        return 0

    saved = update_stocks_in_db_from_polygon(data)
    return saved


def update_metadata_for_tickers(
    tickers: List[str],
    status_dict: Optional[Dict[str, int]] = None,
) -> int:
    """Update regular (non-quarterly) metadata for tickers."""
    if not tickers:
        return 0

    stock_data = [
        {'symbol': ticker.strip().upper()}
        for ticker in tickers
        if isinstance(ticker, str) and ticker.strip()
    ]

    if not stock_data:
        return 0

    return update_stocks_in_db_from_polygon(
        stock_data,
        status_dict=status_dict,
    )


def update_quarterly_metrics_for_tickers(
    tickers: List[str],
    status_dict: Optional[Dict[str, int]] = None,
) -> int:
    """Compatibility wrapper delegated to the quarterly data fetcher service logic."""
    from quarterly_data_fetcher.quarterly_stock_service import (
        update_quarterly_metrics_for_tickers as _quarterly_update_impl,
    )

    return _quarterly_update_impl(tickers=tickers, status_dict=status_dict)
