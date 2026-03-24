import os
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional
from types import SimpleNamespace
import pandas as pd
import yfinance as yf

from polygon import RESTClient
from stock_data_calculator.database import get_db, Stock
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


def update_stocks_in_db_from_polygon(stock_data: List[Dict[str, Any]], status_dict: Optional[Dict[str, int]] = None) -> int:
    api_key = os.environ.get('POLYGON_API_KEY')
    if not api_key:
        logger.error('POLYGON_API_KEY not set, cannot fetch metadata')
        return 0

    if not stock_data:
        logger.error('No stock data provided')
        return 0

    logger.info("Starting to fetch metadata for %s stocks", len(stock_data))
    logger.info("Metadata fetch started")

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

            # Get historical data from Polygon for price calculations
            history_df = pd.DataFrame()
            try:
                end_date = datetime.utcnow().date()
                start_date = end_date - timedelta(days=370)
                bars = client.list_aggs(
                    ticker=ticker,
                    multiplier=1,
                    timespan='day',
                    from_=start_date.strftime('%Y-%m-%d'),
                    to=end_date.strftime('%Y-%m-%d'),
                    adjusted=True,
                    sort='asc',
                )
                history_rows = []
                for bar in bars:
                    history_rows.append({
                        'timestamp': datetime.fromtimestamp(bar.timestamp / 1000),
                        'open': bar.open,
                        'high': bar.high,
                        'low': bar.low,
                        'close': bar.close,
                        'volume': bar.volume,
                    })
                if history_rows:
                    history_df = pd.DataFrame(history_rows)
                    history_df['timestamp'] = pd.to_datetime(history_df['timestamp'])
                    history_df.set_index('timestamp', inplace=True)
            except Exception:
                history_df = pd.DataFrame()

            list_date = None
            if hasattr(details, 'list_date') and details.list_date:
                try:
                    list_date = datetime.strptime(details.list_date, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    list_date = None

            # Get financial metrics from yfinance
            yf_ticker = yf.Ticker(ticker)
            yf_info = yf_ticker.info
            
            eps_value = None
            annual_eps_growth_rate = None
            revenue_per_share_value = None
            price_per_sales_value = None
            price_per_earnings_value = None
            pe_per_growth_value = None
            debt_to_equity_value = None
            
            outstanding_shares_value = _to_builtin_number(yf_info.get('sharesOutstanding'))
            eps_value = _to_builtin_number(yf_info.get('trailingEps'))
            total_revenue = _to_builtin_number(yf_info.get('totalRevenue'))
            price_per_earnings_value = _to_builtin_number(yf_info.get('trailingPE'))
            price_per_sales_value = _to_builtin_number(yf_info.get('priceToSalesTrailing12Months'))
            pe_per_growth_value = _to_builtin_number(yf_info.get('pegRatio'))
            debt_to_equity_value = _to_builtin_number(yf_info.get('debtToEquity'))

            previous_eps_value = None
            try:
                quarterly_financials = yf_ticker.quarterly_financials
                if quarterly_financials is not None and not quarterly_financials.empty:
                    if len(quarterly_financials.columns) >= 5:
                        quarters_ago = quarterly_financials.iloc[:, 4:5]
                        if 'Net Income' in quarterly_financials.index:
                            net_income_4q_ago = quarters_ago.loc['Net Income'].values[0]
                            if net_income_4q_ago and net_income_4q_ago != 0:
                                quarterly_shares = _to_builtin_number(yf_info.get('sharesOutstanding'))
                                if quarterly_shares and quarterly_shares != 0:
                                    previous_eps_value = Decimal(str(round(float(net_income_4q_ago) / float(quarterly_shares), 4)))
                                    logger.info(
                                        "[%s] calculated EPS from 4 quarters ago: %s",
                                        ticker,
                                        previous_eps_value,
                                    )
            except Exception as e:
                logger.info("[%s] could not get quarterly EPS data: %s", ticker, e)

            annual_eps_growth_rate = None
            if eps_value is not None and previous_eps_value not in (None, 0):
                try:
                    annual_eps_growth_rate = ((float(eps_value) / float(previous_eps_value)) - 1.0) * 100.0
                    logger.info(
                        "[%s] calculated annual_eps_growth_rate=%s using trailing_eps=%s and previous_year_eps=%s",
                        ticker,
                        annual_eps_growth_rate,
                        eps_value,
                        previous_eps_value,
                    )
                except Exception:
                    annual_eps_growth_rate = None
                    logger.warning("[%s] failed calculating annual_eps_growth_rate", ticker, exc_info=True)
            
            if total_revenue is not None and outstanding_shares_value not in (None, 0):
                try:
                    shares_decimal = Decimal(str(outstanding_shares_value))
                    if shares_decimal != 0:
                        revenue_per_share_value = (Decimal(str(total_revenue)) / shares_decimal).quantize(
                            Decimal('0.01'),
                            rounding=ROUND_HALF_UP,
                        )
                        logger.info(
                            "[%s] calculated revenue_per_share=%s using total_revenue=%s and outstanding_shares=%s",
                            ticker,
                            revenue_per_share_value,
                            total_revenue,
                            outstanding_shares_value,
                        )
                except Exception as e:
                    revenue_per_share_value = None
                    logger.warning("[%s] failed calculating revenue_per_share: %s", ticker, e)
            else:
                logger.info(
                    "[%s] skipped revenue_per_share calculation: total_revenue=%s, outstanding_shares=%s",
                    ticker,
                    total_revenue,
                    outstanding_shares_value,
                )

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
                'outstanding_shares': outstanding_shares_value,
                'eps': _to_two_decimal_numeric(eps_value) if eps_value else None,
                'revenue_per_share': revenue_per_share_value,
                'homepage_url': getattr(details, 'homepage_url', None),
                'total_employees': getattr(details, 'total_employees', None),
                'list_date': list_date,
                'locale': getattr(details, 'locale', None),
                'sic_code': getattr(details, 'sic_code', None),
                'sic_description': getattr(details, 'sic_description', None),
                'price_per_earnings': price_per_earnings_value,
                'pe_per_growth': pe_per_growth_value,
                'annual_eps_growth_rate': int(round(annual_eps_growth_rate)) if annual_eps_growth_rate is not None else None,
                'debt_to_equity': _to_two_decimal_numeric(debt_to_equity_value) if debt_to_equity_value else None,
            }

            for k, v in optional_map.items():
                if _column_allowed(k):
                    defaults[k] = v

            # Upsert the stock row
            with get_db() as db:
                existing = db.execute(select(Stock).where(Stock.symbol == ticker)).first()

                if existing is not None:
                    stock_for_calc = existing[0]
                else:
                    stock_for_calc = SimpleNamespace(symbol=ticker, price=None, eps=eps_value)

                if getattr(stock_for_calc, 'eps', None) is None and eps_value is not None:
                    stock_for_calc.eps = eps_value

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

                if current_price is not None and revenue_per_share_value not in (None, 0):
                    try:
                        revenue_per_share_decimal = Decimal(str(revenue_per_share_value))
                        if revenue_per_share_decimal != 0 and price_per_sales_value is None:
                            price_per_sales_value = (
                                Decimal(str(current_price)) / revenue_per_share_decimal
                            ).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                            logger.info(
                                "[%s] calculated price_per_sales=%s using price=%s and revenue_per_share=%s",
                                ticker,
                                price_per_sales_value,
                                current_price,
                                revenue_per_share_value,
                            )
                    except Exception:
                        logger.warning("[%s] failed calculating price_per_sales", ticker, exc_info=True)
                else:
                    logger.info(
                        "[%s] skipped price_per_sales calculation: price=%s, revenue_per_share=%s",
                        ticker,
                        current_price,
                        revenue_per_share_value,
                    )

                defaults['price'] = _to_cents(calculated_price)
                defaults['high52'] = _to_cents(calculated_high52)
                defaults['low52'] = _to_cents(calculated_low52)
                defaults['percent_change'] = _to_percent_hundredths(calculated_percent_change)
                defaults['eps'] = _to_two_decimal_numeric(eps_value) if eps_value else None
                defaults['revenue_per_share'] = _to_two_decimal_numeric(revenue_per_share_value)
                defaults['price_per_sales'] = _to_two_decimal_numeric(price_per_sales_value)
                defaults['price_per_earnings'] = _to_percent_hundredths(price_per_earnings_value)
                defaults['pe_per_growth'] = _to_percent_hundredths(pe_per_growth_value)
                defaults['annual_eps_growth_rate'] = int(round(_to_builtin_number(annual_eps_growth_rate))) if annual_eps_growth_rate is not None else None
                defaults['debt_to_equity'] = _to_two_decimal_numeric(debt_to_equity_value) if debt_to_equity_value else None

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
                logger.error("Error fetching %s: %s", ticker, e)

        if (idx + 1) % 100 == 0:
            progress_msg = (
                f"Processed {idx + 1}/{len(stock_data)} stocks... "
                f"(Saved: {saved_count}, Errors: {error_count})"
            )
            logger.info(progress_msg)
            if status_dict:
                status_dict['progress'] = idx + 1
                status_dict['saved'] = saved_count
                status_dict['errors'] = error_count

    logger.info("COMPLETE: Saved %s stocks to database", saved_count)
    logger.info("Errors: %s", error_count)

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


def update_metadata_for_tickers(tickers: List[str], status_dict: Optional[Dict[str, int]] = None) -> int:
    if not tickers:
        return 0

    stock_data = [
        {'symbol': ticker.strip().upper()}
        for ticker in tickers
        if isinstance(ticker, str) and ticker.strip()
    ]

    if not stock_data:
        return 0

    return update_stocks_in_db_from_polygon(stock_data, status_dict=status_dict)
