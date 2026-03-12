import os
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional
from types import SimpleNamespace
import pandas as pd

from polygon import RESTClient

from database import get_db, Stock
from sqlalchemy import select
from stock_calculator import StockCalculator


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

            eps_value = None
            annual_eps_growth_rate = None
            price_per_earnings_value = None
            pe_per_growth_value = None
            debt_to_equity_value = None
            try:
                current_year = datetime.now().year
                last_year = current_year - 1

                reports = client.vx.list_stock_financials(
                    ticker=ticker,
                    timeframe='annual',
                    limit=5,
                )
                target = None
                current_report = None
                prior_year_report = None
                for rpt in reports:
                    fiscal_year = getattr(rpt, 'fiscal_year', None)
                    if fiscal_year is not None:
                        try:
                            fy = int(fiscal_year)
                            if fy == int(current_year) and current_report is None:
                                current_report = rpt
                            if fy == int(last_year) and prior_year_report is None:
                                prior_year_report = rpt
                            if target is None and fy == int(last_year):
                                target = rpt
                        except Exception:
                            pass
                    end_date_str = getattr(rpt, 'end_date', None)
                    if end_date_str:
                        try:
                            end_dt = datetime.strptime(end_date_str[:10], '%Y-%m-%d').date()
                            if end_dt.year == current_year and current_report is None:
                                current_report = rpt
                            if end_dt.year == last_year and prior_year_report is None:
                                prior_year_report = rpt
                            if target is None and end_dt.year == last_year:
                                target = rpt
                        except Exception:
                            pass

                if target is None:
                    target = next(
                        iter(
                            client.vx.list_stock_financials(
                                ticker=ticker,
                                timeframe='annual',
                                limit=1,
                            )
                        ),
                        None,
                    )

                if target is not None:
                    fin = getattr(target, 'financials', None)
                    income = getattr(fin, 'income_statement', None) if fin is not None else None
                    beps = None
                    if income is not None:
                        beps = _dp_value(getattr(income, 'basic_earnings_per_share', None))
                    if beps is not None:
                        try:
                            eps_value = Decimal(str(round(float(beps), 4)))
                        except Exception:
                            eps_value = None

                    # Annual EPS growth rate formula:
                    if current_report is not None and prior_year_report is not None:
                        try:
                            current_fin = getattr(current_report, 'financials', None)
                            prior_fin = getattr(prior_year_report, 'financials', None)
                            current_income = getattr(current_fin, 'income_statement', None) if current_fin is not None else None
                            prior_income = getattr(prior_fin, 'income_statement', None) if prior_fin is not None else None

                            current_beps = _dp_value(getattr(current_income, 'basic_earnings_per_share', None)) if current_income is not None else None
                            prior_beps = _dp_value(getattr(prior_income, 'basic_earnings_per_share', None)) if prior_income is not None else None

                            if current_beps is not None and prior_beps not in (None, 0):
                                annual_eps_growth_rate = ((float(current_beps) / float(prior_beps)) - 1.0) * 100.0
                        except Exception:
                            annual_eps_growth_rate = None

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
                            except Exception:
                                debt_to_equity_value = None
            except Exception:
                eps_value = None
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
                'outstanding_shares': getattr(details, 'weighted_shares_outstanding', None),
                'eps': eps_value,
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
                calculated_percent_change = StockCalculator.calculate_percent_change(history_df, stock_for_calc)

                defaults['price'] = int(round(_to_builtin_number(calculated_price))) if calculated_price is not None else None
                defaults['high52'] = int(round(_to_builtin_number(calculated_high52))) if calculated_high52 is not None else None
                defaults['low52'] = int(round(_to_builtin_number(calculated_low52))) if calculated_low52 is not None else None
                defaults['percent_change'] = int(round(_to_builtin_number(calculated_percent_change))) if calculated_percent_change is not None else None

                # Calculate P/E from current stored price and latest EPS.
                current_price = defaults['price']

                if current_price is not None and eps_value not in (None, 0):
                    price_per_earnings_value = StockCalculator.calculate_pe(stock_for_calc, current_price)

                # PEG = P/E / annual growth rate
                if price_per_earnings_value is not None and annual_eps_growth_rate not in (None, 0):
                    try:
                        pe_per_growth_value = int(round(float(price_per_earnings_value) / float(annual_eps_growth_rate)))
                    except Exception:
                        pe_per_growth_value = None

                defaults['annual_eps_growth_rate'] = int(round(_to_builtin_number(annual_eps_growth_rate))) if annual_eps_growth_rate is not None else None
                defaults['price_per_earnings'] = int(round(_to_builtin_number(price_per_earnings_value))) if price_per_earnings_value is not None else None
                defaults['pe_per_growth'] = int(round(_to_builtin_number(pe_per_growth_value))) if pe_per_growth_value is not None else None

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
