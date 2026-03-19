import os
import traceback
import logging
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional
from types import SimpleNamespace
import pandas as pd

from polygon import RESTClient

from database import get_db, Stock
from sqlalchemy import select
from stock_calculator import StockCalculator

logger = logging.getLogger(__name__)


def _report_identity(report) -> str:
    if report is None:
        return "none"

    fiscal_year = getattr(report, 'fiscal_year', None)
    end_date = getattr(report, 'end_date', None)
    filing_date = getattr(report, 'filing_date', None)
    return f"fiscal_year={fiscal_year}, end_date={end_date}, filing_date={filing_date}"


def fetch_new_stocks_from_polygon() -> List[Dict[str, str]]:
    api_key = os.environ.get('POLYGON_API_KEY')
    if not api_key or api_key.strip() == '':
        print('ERROR: POLYGON_API_KEY not set in environment')
        return []

    try:
        client = RESTClient(api_key)
        tickers = []

        for ticker in client.list_tickers(market='stocks', limit=1000):
            tickers.append({
                'symbol': ticker.ticker,
                'name': getattr(ticker, 'name', ticker.ticker),
            })

        print(f"Fetched {len(tickers)} tickers from Polygon")
        return tickers

    except Exception as e:
        print(f"ERROR fetching tickers from Polygon: {e}")
        traceback.print_exc()
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
        print('ERROR: POLYGON_API_KEY not set, cannot fetch metadata')
        return 0

    if not stock_data:
        print('ERROR: No stock data provided')
        return 0

    print(f"Starting to fetch metadata for {len(stock_data)} stocks...")
    print("This will take a while...")

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
            details = client.get_ticker_details(ticker)

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

            def _dp_value(dp):
                if dp is None:
                    return None
                return getattr(dp, 'value', dp)

            outstanding_shares_value = _to_builtin_number(
                getattr(details, 'weighted_shares_outstanding', None)
            )
            eps_value = None
            revenue_per_share_value = None
            price_per_sales_value = None
            previous_eps_value = None
            annual_eps_growth_rate = None
            price_per_earnings_value = None
            pe_per_growth_value = None
            debt_to_equity_value = None
            try:
                reports = list(
                    client.vx.list_stock_financials(
                        ticker=ticker,
                        limit=2,
                    )
                )

                def _report_sort_key(report):
                    end_date_str = getattr(report, 'end_date', None)
                    if end_date_str:
                        try:
                            return datetime.strptime(end_date_str[:10], '%Y-%m-%d')
                        except Exception:
                            pass

                    filing_date_str = getattr(report, 'filing_date', None)
                    if filing_date_str:
                        try:
                            return datetime.strptime(filing_date_str[:10], '%Y-%m-%d')
                        except Exception:
                            pass

                    fiscal_year = getattr(report, 'fiscal_year', None)
                    if fiscal_year is not None:
                        try:
                            return datetime(int(fiscal_year), 12, 31)
                        except Exception:
                            pass

                    return datetime.min

                sorted_reports = sorted(reports, key=_report_sort_key, reverse=True)
                target = sorted_reports[0] if sorted_reports else None
                previous_report = sorted_reports[1] if len(sorted_reports) > 1 else None

                logger.info(
                    "[%s] financial reports selected: current={%s}, previous={%s}",
                    ticker,
                    _report_identity(target),
                    _report_identity(previous_report),
                )

                def _extract_basic_eps(report):
                    if report is None:
                        return None

                    fin = getattr(report, 'financials', None)
                    income = getattr(fin, 'income_statement', None) if fin is not None else None
                    basic_eps = None
                    if income is not None:
                        basic_eps = _dp_value(getattr(income, 'basic_earnings_per_share', None))
                    if basic_eps is None:
                        return None
                    try:
                        return Decimal(str(round(float(basic_eps), 4)))
                    except Exception:
                        return None

                def _extract_total_revenue(report):
                    if report is None:
                        return None

                    fin = getattr(report, 'financials', None)
                    income = getattr(fin, 'income_statement', None) if fin is not None else None
                    if income is None:
                        return None

                    for field_name in ('total_revenue', 'revenues', 'sales_revenue_net'):
                        revenue = _dp_value(getattr(income, field_name, None))
                        if revenue is None:
                            continue
                        try:
                            return Decimal(str(revenue))
                        except Exception:
                            return None

                    return None

                if target is not None:
                    eps_value = _extract_basic_eps(target)
                    previous_eps_value = _extract_basic_eps(previous_report)
                    total_revenue_value = _extract_total_revenue(target)

                    logger.info(
                        "[%s] extracted financial inputs: eps=%s from {%s}, previous_eps=%s from {%s}, total_revenue=%s from {%s}",
                        ticker,
                        eps_value,
                        _report_identity(target),
                        previous_eps_value,
                        _report_identity(previous_report),
                        total_revenue_value,
                        _report_identity(target),
                    )

                    if total_revenue_value is not None and outstanding_shares_value not in (None, 0):
                        try:
                            shares_decimal = Decimal(str(outstanding_shares_value))
                            if shares_decimal != 0:
                                revenue_per_share_value = (total_revenue_value / shares_decimal).quantize(
                                    Decimal('0.01'),
                                    rounding=ROUND_HALF_UP,
                                )
                                logger.info(
                                    "[%s] calculated revenue_per_share=%s using total_revenue=%s from {%s} and outstanding_shares=%s",
                                    ticker,
                                    revenue_per_share_value,
                                    total_revenue_value,
                                    _report_identity(target),
                                    outstanding_shares_value,
                                )
                        except Exception:
                            revenue_per_share_value = None
                            logger.warning("[%s] failed calculating revenue_per_share", ticker, exc_info=True)
                    else:
                        logger.info(
                            "[%s] skipped revenue_per_share calculation: total_revenue=%s, outstanding_shares=%s",
                            ticker,
                            total_revenue_value,
                            outstanding_shares_value,
                        )

                    fin = getattr(target, 'financials', None)
                    if eps_value is not None and previous_eps_value not in (None, 0):
                        try:
                            annual_eps_growth_rate = ((float(eps_value) / float(previous_eps_value)) - 1.0) * 100.0
                            logger.info(
                                "[%s] calculated annual_eps_growth_rate=%s using eps=%s from {%s} and previous_eps=%s from {%s}",
                                ticker,
                                annual_eps_growth_rate,
                                eps_value,
                                _report_identity(target),
                                previous_eps_value,
                                _report_identity(previous_report),
                            )
                        except Exception:
                            annual_eps_growth_rate = None
                            logger.warning("[%s] failed calculating annual_eps_growth_rate", ticker, exc_info=True)
                    else:
                        logger.info(
                            "[%s] skipped annual_eps_growth_rate calculation: eps=%s, previous_eps=%s",
                            ticker,
                            eps_value,
                            previous_eps_value,
                        )

                    balance_sheet = getattr(fin, 'balance_sheet', None) if fin is not None else None
                    if balance_sheet is not None:
                        # Use long-term debt (interest-bearing obligations only) for a
                        # standard D/E ratio.  Fall back to noncurrent_liabilities if the
                        # filing doesn't include a discrete long_term_debt entry.
                        debt = _dp_value(getattr(balance_sheet, 'long_term_debt', None))
                        if debt is None:
                            debt = _dp_value(getattr(balance_sheet, 'noncurrent_liabilities', None))
                        equity = _dp_value(getattr(balance_sheet, 'equity', None))
                        if debt is not None and equity is not None:
                            try:
                                equity_f = float(equity)
                                if equity_f != 0:
                                    debt_to_equity_value = Decimal(str(round(float(debt) / equity_f, 4)))
                                    logger.info(
                                        "[%s] calculated debt_to_equity=%s from {%s}",
                                        ticker,
                                        debt_to_equity_value,
                                        _report_identity(target),
                                    )
                            except Exception:
                                debt_to_equity_value = None
                                logger.warning("[%s] failed calculating debt_to_equity", ticker, exc_info=True)
                        else:
                            logger.info(
                                "[%s] skipped debt_to_equity calculation: debt=%s, equity=%s",
                                ticker,
                                debt,
                                equity,
                            )
            except Exception:
                eps_value = None
                revenue_per_share_value = None
                price_per_sales_value = None
                previous_eps_value = None
                annual_eps_growth_rate = None
                price_per_earnings_value = None
                pe_per_growth_value = None
                debt_to_equity_value = None

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
                'eps': eps_value,
                'revenue_per_share': revenue_per_share_value,
                'homepage_url': getattr(details, 'homepage_url', None),
                'total_employees': getattr(details, 'total_employees', None),
                'list_date': list_date,
                'locale': getattr(details, 'locale', None),
                'sic_code': getattr(details, 'sic_code', None),
                'sic_description': getattr(details, 'sic_description', None),
                'annual_eps_growth_rate': int(round(annual_eps_growth_rate)) if annual_eps_growth_rate is not None else None,
                'price_per_earnings': price_per_earnings_value,
                'pe_per_growth': pe_per_growth_value,
                'debt_to_equity': debt_to_equity_value,
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

                defaults['price'] = _to_cents(calculated_price)
                defaults['high52'] = _to_cents(calculated_high52)
                defaults['low52'] = _to_cents(calculated_low52)
                defaults['percent_change'] = _to_percent_hundredths(calculated_percent_change)

                # Keep valuation calculations in dollar units even though the DB stores cents.
                current_price = _to_builtin_number(calculated_price)

                if current_price is not None and revenue_per_share_value not in (None, 0):
                    try:
                        revenue_per_share_decimal = Decimal(str(revenue_per_share_value))
                        if revenue_per_share_decimal != 0:
                            price_per_sales_value = (
                                Decimal(str(current_price)) / revenue_per_share_decimal
                            ).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)
                            logger.info(
                                "[%s] calculated price_per_sales=%s using price=%s and revenue_per_share=%s (revenue from {%s})",
                                ticker,
                                price_per_sales_value,
                                current_price,
                                revenue_per_share_value,
                                _report_identity(target),
                            )
                    except Exception:
                        price_per_sales_value = None
                        logger.warning("[%s] failed calculating price_per_sales", ticker, exc_info=True)
                else:
                    logger.info(
                        "[%s] skipped price_per_sales calculation: price=%s, revenue_per_share=%s",
                        ticker,
                        current_price,
                        revenue_per_share_value,
                    )

                if current_price is not None and eps_value not in (None, 0):
                    price_per_earnings_value = StockCalculator.calculate_pe(stock_for_calc, current_price)
                    logger.info(
                        "[%s] calculated price_per_earnings=%s using price=%s and eps=%s from {%s}",
                        ticker,
                        price_per_earnings_value,
                        current_price,
                        eps_value,
                        _report_identity(target),
                    )
                else:
                    logger.info(
                        "[%s] skipped price_per_earnings calculation: price=%s, eps=%s",
                        ticker,
                        current_price,
                        eps_value,
                    )

                # PEG = P/E / annual growth rate
                if price_per_earnings_value is not None and annual_eps_growth_rate not in (None, 0):
                    try:
                        pe_per_growth_value = float(price_per_earnings_value) / float(annual_eps_growth_rate)
                        logger.info(
                            "[%s] calculated pe_per_growth=%s using pe=%s and annual_eps_growth_rate=%s",
                            ticker,
                            pe_per_growth_value,
                            price_per_earnings_value,
                            annual_eps_growth_rate,
                        )
                    except Exception:
                        pe_per_growth_value = None
                        logger.warning("[%s] failed calculating pe_per_growth", ticker, exc_info=True)
                else:
                    logger.info(
                        "[%s] skipped pe_per_growth calculation: pe=%s, annual_eps_growth_rate=%s",
                        ticker,
                        price_per_earnings_value,
                        annual_eps_growth_rate,
                    )

                defaults['annual_eps_growth_rate'] = int(round(_to_builtin_number(annual_eps_growth_rate))) if annual_eps_growth_rate is not None else None
                defaults['revenue_per_share'] = _to_two_decimal_numeric(revenue_per_share_value)
                defaults['price_per_sales'] = _to_two_decimal_numeric(price_per_sales_value)
                defaults['price_per_earnings'] = _to_percent_hundredths(price_per_earnings_value)
                defaults['pe_per_growth'] = _to_percent_hundredths(pe_per_growth_value)

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
                print(f"Error fetching {ticker}: {e}")

        if (idx + 1) % 100 == 0:
            progress_msg = (
                f"Processed {idx + 1}/{len(stock_data)} stocks... "
                f"(Saved: {saved_count}, Errors: {error_count})"
            )
            print(progress_msg)
            if status_dict:
                status_dict['progress'] = idx + 1
                status_dict['saved'] = saved_count
                status_dict['errors'] = error_count

    print(f"COMPLETE: Saved {saved_count} stocks to database")
    print(f"Errors: {error_count}")

    if status_dict:
        status_dict['progress'] = len(stock_data)
        status_dict['saved'] = saved_count
        status_dict['errors'] = error_count

    return saved_count


def fetch_and_update_symbols() -> int:
    data = fetch_new_stocks_from_polygon()
    if not data:
        print("ERROR: No data fetched from Polygon")
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
