import os
import traceback
from datetime import datetime
from decimal import Decimal
from typing import List, Dict, Any, Optional

from polygon import RESTClient

from database import get_db, Stock


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

            list_date = None
            if hasattr(details, 'list_date') and details.list_date:
                try:
                    list_date = datetime.strptime(details.list_date, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    list_date = None

            eps_value = None
            try:
                last_year = datetime.now().year - 1

                def _dp_value(dp):
                    if dp is None:
                        return None
                    return getattr(dp, 'value', dp)

                reports = client.vx.list_stock_financials(
                    ticker=ticker,
                    timeframe='annual',
                    limit=5,
                )
                target = None
                for rpt in reports:
                    fiscal_year = getattr(rpt, 'fiscal_year', None)
                    if fiscal_year is not None:
                        try:
                            if int(fiscal_year) == int(last_year):
                                target = rpt
                                break
                        except Exception:
                            pass
                    end_date_str = getattr(rpt, 'end_date', None)
                    if end_date_str:
                        try:
                            end_dt = datetime.strptime(end_date_str[:10], '%Y-%m-%d').date()
                            if end_dt.year == last_year:
                                target = rpt
                                break
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
            except Exception:
                eps_value = None

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
            }

            for k, v in optional_map.items():
                if _column_allowed(k):
                    defaults[k] = v

            # Upsert the stock row
            with get_db() as db:
                existing = db.execute(Stock.__table__.select().where(Stock.symbol == ticker)).first()
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
