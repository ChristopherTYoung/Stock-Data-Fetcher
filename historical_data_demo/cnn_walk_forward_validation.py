import argparse
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
from matplotlib.patches import Rectangle
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, OperationalError
from tensorflow.keras import Sequential
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.layers import Conv1D, Dense, Dropout, Flatten
from tensorflow.keras.models import load_model
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.regularizers import l2

WALK_FORWARD_TRAIN_MONTHS = 6
FEATURE_COLS = ["open_norm", "high_norm", "low_norm", "close_norm"]
DEFAULT_FORECAST_SYMBOLS = ["AAPL", "MSFT"]
MODEL_ARTIFACT_PATH = Path(__file__).resolve().parent / "artifacts" / "shared_ohlc_model.keras"
DB_MAX_RETRIES = 4
MIN_MONTHS_REQUIRED = WALK_FORWARD_TRAIN_MONTHS + 1
MAX_SEQS_PER_SYMBOL = 1200
MAX_TOTAL_SEQS = 120000


def load_env_file(env_path: str = ".env") -> None:
    path = Path(env_path)
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def resolve_database_url(cli_database_url: str | None) -> str:
    candidate = (cli_database_url or "").strip()

    if candidate.startswith("${") and candidate.endswith("}"):
        env_key = candidate[2:-1].strip()
        candidate = os.getenv(env_key, "").strip()

    candidate = os.path.expandvars(candidate).strip()

    if not candidate:
        candidate = os.getenv("DB_CONN_STRING", "").strip()
    if not candidate:
        candidate = os.getenv("DATABASE_URL", "").strip()

    if not candidate:
        raise ValueError(
            "Database URL is empty. Set DB_CONN_STRING or DATABASE_URL in .env, "
            "or pass --database-url with a full postgres URL."
        )

    return candidate


@dataclass
class FoldResult:
    train_months: List[pd.Period]
    test_month: pd.Period
    y_train_true: np.ndarray
    y_train_pred: np.ndarray
    y_true: np.ndarray
    y_pred: np.ndarray
    pred_open: np.ndarray
    pred_high: np.ndarray
    pred_low: np.ndarray
    timestamps: pd.Series
    train_mae: float
    train_rmse: float
    mae: float
    rmse: float


def parse_symbols(raw_symbols: List[str]) -> List[str]:
    symbols: List[str] = []
    seen = set()

    for raw in raw_symbols:
        for token in raw.split(","):
            symbol = token.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            symbols.append(symbol)

    if not symbols:
        raise ValueError("At least one ticker symbol is required.")

    return symbols


def execute_with_retries(engine, query, params=None):
    last_exc = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            with engine.connect() as connection:
                return connection.execute(query, params or {}).mappings().all()
        except (OperationalError, DBAPIError) as exc:
            last_exc = exc
            if attempt == DB_MAX_RETRIES:
                break
            sleep_seconds = min(8, 2 ** (attempt - 1))
            print(f"DB query failed (attempt {attempt}/{DB_MAX_RETRIES}). Retrying in {sleep_seconds}s...")
            time.sleep(sleep_seconds)

    raise last_exc


def fetch_symbols_from_stock_table(database_url: str, interval: str) -> List[str]:
    interval_lower = interval.lower()
    is_hourly = interval_lower in {"1h", "1hour", "hourly"}
    engine = create_engine(database_url, pool_pre_ping=True)
    cols_query = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'incrementum'
          AND table_name = 'stock'
        """
    )
    available_cols = {row["column_name"] for row in execute_with_retries(engine, cols_query)}

    for col in ["symbol", "stock_symbol", "ticker"]:
        if col in available_cols:
            symbol_query = text(
                f"""
                SELECT DISTINCT s.{col} AS symbol
                FROM incrementum.stock s
                WHERE s.{col} IS NOT NULL
                  AND TRIM(s.{col}) <> ''
                  AND EXISTS (
                    SELECT 1
                    FROM incrementum.stock_history h
                    WHERE h.stock_symbol = s.{col}
                      AND h.is_hourly = :is_hourly
                  )
                ORDER BY s.{col}
                """
            )
            rows = execute_with_retries(engine, symbol_query, params={"is_hourly": is_hourly})
            symbols = [str(row["symbol"]).strip().upper() for row in rows if row["symbol"]]
            if symbols:
                return symbols

    raise ValueError(
        "Could not resolve ticker column in incrementum.stock. Expected one of: symbol, stock_symbol, ticker."
    )


def fetch_prices(symbol: str, period: str, interval: str) -> pd.DataFrame:
    raise NotImplementedError("Use fetch_prices_from_db for database-backed data loading.")


def fetch_prices_from_db(symbol: str, database_url: str, interval: str) -> pd.DataFrame:
    interval_lower = interval.lower()
    if interval_lower not in {"1h", "1d", "1hour", "1day", "hourly", "daily"}:
        raise ValueError("Interval must be one of: 1h, 1d, hourly, daily")

    is_hourly = interval_lower in {"1h", "1hour", "hourly"}
    query = text(
        """
        SELECT
            day_and_time AS timestamp,
            open_price / 100.0 AS open,
            high / 100.0 AS high,
            low / 100.0 AS low,
            close_price / 100.0 AS close
        FROM incrementum.stock_history
        WHERE stock_symbol = :symbol
          AND is_hourly = :is_hourly
        ORDER BY day_and_time ASC
        """
    )

    engine = create_engine(database_url, pool_pre_ping=True)
    last_exc = None
    for attempt in range(1, DB_MAX_RETRIES + 1):
        try:
            with engine.connect() as connection:
                df = pd.read_sql(query, connection, params={"symbol": symbol.upper(), "is_hourly": is_hourly})
            break
        except (OperationalError, DBAPIError) as exc:
            last_exc = exc
            if attempt == DB_MAX_RETRIES:
                raise
            sleep_seconds = min(8, 2 ** (attempt - 1))
            print(
                f"DB read failed for {symbol} (attempt {attempt}/{DB_MAX_RETRIES}). "
                f"Retrying in {sleep_seconds}s..."
            )
            time.sleep(sleep_seconds)

    if last_exc and 'df' not in locals():
        raise last_exc

    if df.empty:
        frequency = "hourly" if is_hourly else "daily"
        raise ValueError(
            f"No {frequency} rows found in incrementum.stock_history for symbol '{symbol.upper()}'."
        )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


def build_cnn_model(lookback: int, feature_count: int) -> Sequential:
    model = Sequential(
        [
            Conv1D(
                filters=24,
                kernel_size=3,
                activation="relu",
                input_shape=(lookback, feature_count),
                kernel_regularizer=l2(1e-4),
            ),
            Dropout(0.2),
            Conv1D(filters=12, kernel_size=3, activation="relu", kernel_regularizer=l2(1e-4)),
            Flatten(),
            Dense(32, activation="relu", kernel_regularizer=l2(1e-4)),
            Dropout(0.2),
            Dense(4),
        ]
    )
    model.compile(optimizer=Adam(learning_rate=0.001), loss="mse")
    return model


def add_previous_close_normalized_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["prev_close"] = out["close"].shift(1)
    out["open_norm"] = out["open"] / out["prev_close"] - 1.0
    out["high_norm"] = out["high"] / out["prev_close"] - 1.0
    out["low_norm"] = out["low"] / out["prev_close"] - 1.0
    out["close_norm"] = out["close"] / out["prev_close"] - 1.0
    out = out.dropna().reset_index(drop=True)
    return out


def make_sequences(features: np.ndarray, targets: np.ndarray, lookback: int) -> Tuple[np.ndarray, np.ndarray]:
    x, y = [], []
    for i in range(lookback, len(features)):
        x.append(features[i - lookback:i])
        y.append(targets[i])
    x_arr = np.array(x)
    y_arr = np.array(y)
    return x_arr, y_arr


def estimate_range_ratio(df: pd.DataFrame) -> float:
    ratio = ((df["high"] - df["low"]) / df["close"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
    val = float(ratio.dropna().tail(500).median()) if not ratio.dropna().empty else 0.002
    return min(max(val, 0.001), 0.03)


def clip_feature_vector(pred_vec: np.ndarray, reference_features: np.ndarray) -> np.ndarray:
    if reference_features.size == 0:
        return np.clip(pred_vec, -0.05, 0.05)

    low = np.quantile(reference_features, 0.01, axis=0)
    high = np.quantile(reference_features, 0.99, axis=0)
    band = np.maximum(0.005, 0.25 * (high - low))
    return np.clip(pred_vec, low - band, high + band)


def predict_feature_vector(model: Sequential, x_input: np.ndarray) -> np.ndarray:
    return model(x_input, training=False).numpy().flatten()


def print_data_ranges(symbol_dfs: Dict[str, pd.DataFrame], interval: str) -> None:
    print(f"\nLoaded data ranges (interval={interval}):")
    for symbol, df in symbol_dfs.items():
        start_ts = df["timestamp"].min()
        end_ts = df["timestamp"].max()
        span_days = int((end_ts - start_ts).days)
        month_count = int(df["timestamp"].dt.to_period("M").nunique())
        print(
            f"- {symbol}: rows={len(df)}, months={month_count}, "
            f"start={start_ts}, end={end_ts}, span_days={span_days}"
        )


def filter_symbols_with_min_months(symbol_dfs: Dict[str, pd.DataFrame], min_months: int) -> Tuple[Dict[str, pd.DataFrame], List[str]]:
    kept: Dict[str, pd.DataFrame] = {}
    dropped: List[str] = []
    for symbol, df in symbol_dfs.items():
        month_count = int(df["timestamp"].dt.to_period("M").nunique())
        if month_count >= min_months:
            kept[symbol] = df
        else:
            dropped.append(symbol)
    return kept, dropped


def build_train_set_shared(
    symbol_monthly_df: Dict[str, pd.DataFrame],
    train_months: List[pd.Period],
    lookback: int,
    max_seqs_per_symbol: int = MAX_SEQS_PER_SYMBOL,
    max_total_seqs: int = MAX_TOTAL_SEQS,
) -> Tuple[np.ndarray, np.ndarray]:
    x_blocks = []
    y_blocks = []
    rng = np.random.default_rng(42)
    for df in symbol_monthly_df.values():
        train_df = df[df["month"].isin(train_months)]
        if len(train_df) <= lookback:
            continue
        feature_values = train_df[FEATURE_COLS].values
        target_values = train_df[FEATURE_COLS].values
        x_train, y_train = make_sequences(feature_values, target_values, lookback)
        if len(x_train) == 0:
            continue
        if len(x_train) > max_seqs_per_symbol:
            idx = rng.choice(len(x_train), size=max_seqs_per_symbol, replace=False)
            x_train = x_train[idx]
            y_train = y_train[idx]
        x_blocks.append(x_train)
        y_blocks.append(y_train)

    if not x_blocks:
        raise ValueError("No train sequences were produced for the shared model in this fold.")

    x_all = np.concatenate(x_blocks)
    y_all = np.concatenate(y_blocks)

    if len(x_all) > max_total_seqs:
        idx = rng.choice(len(x_all), size=max_total_seqs, replace=False)
        x_all = x_all[idx]
        y_all = y_all[idx]

    return x_all, y_all


def walk_forward_monthly_shared(
    symbol_dfs: Dict[str, pd.DataFrame],
    lookback: int,
    epochs: int,
    batch_size: int,
    verbose: int,
    log_symbols: List[str],
) -> Dict[str, List[FoldResult]]:
    symbol_monthly_df: Dict[str, pd.DataFrame] = {}
    all_months = set()
    for symbol, df in symbol_dfs.items():
        sdf = df.copy()
        sdf["month"] = sdf["timestamp"].dt.to_period("M")
        symbol_monthly_df[symbol] = sdf
        all_months.update(sdf["month"].unique())

    months = sorted(all_months)

    if len(months) < WALK_FORWARD_TRAIN_MONTHS + 1:
        raise ValueError(
            f"Need at least {WALK_FORWARD_TRAIN_MONTHS + 1} months of data for rolling walk-forward validation."
        )

    results: Dict[str, List[FoldResult]] = {symbol: [] for symbol in symbol_dfs.keys()}

    for test_idx in range(WALK_FORWARD_TRAIN_MONTHS, len(months)):
        train_months = months[test_idx - WALK_FORWARD_TRAIN_MONTHS:test_idx]
        test_month = months[test_idx]

        x_train_all, y_train_all = build_train_set_shared(
            symbol_monthly_df=symbol_monthly_df,
            train_months=train_months,
            lookback=lookback,
        )

        model = build_cnn_model(lookback, feature_count=len(FEATURE_COLS))
        model.fit(
            x_train_all,
            y_train_all,
            epochs=epochs,
            batch_size=batch_size,
            verbose=verbose,
            callbacks=[EarlyStopping(monitor="loss", patience=2, restore_best_weights=True)],
        )

        for symbol, df in symbol_monthly_df.items():
            train_df = df[df["month"].isin(train_months)]
            test_df = df[df["month"] == test_month]

            if len(train_df) <= lookback or len(test_df) == 0:
                continue

            train_features = train_df[FEATURE_COLS].values
            train_targets = train_df["close_norm"].values
            x_train_symbol, _ = make_sequences(train_features, train_targets, lookback)
            if len(x_train_symbol) == 0:
                continue

            train_pred_vec = model(x_train_symbol, training=False).numpy()
            train_prev_close = train_df["prev_close"].values[lookback:]
            y_train_true = train_df["close"].values[lookback:]
            y_train_pred = (train_pred_vec[:, 3] + 1.0) * train_prev_close

            train_mae = mean_absolute_error(y_train_true, y_train_pred)
            train_rmse = np.sqrt(mean_squared_error(y_train_true, y_train_pred))

            combined_df = pd.concat([train_df, test_df], ignore_index=True)
            combined_features = combined_df[FEATURE_COLS].values
            y_true = test_df["close"].values
            y_pred_feature_vecs = []
            test_start = len(train_df)
            train_feature_ref = train_df[FEATURE_COLS].values
            for idx in range(test_start, len(combined_df)):
                x_input = combined_features[idx - lookback:idx].reshape(1, lookback, len(FEATURE_COLS))
                raw_pred_vec = predict_feature_vector(model, x_input)
                clipped_pred_vec = clip_feature_vector(raw_pred_vec, train_feature_ref)
                y_pred_feature_vecs.append(clipped_pred_vec)

            y_pred_feature_arr = np.array(y_pred_feature_vecs)
            test_prev_close = test_df["prev_close"].values
            pred_open = (y_pred_feature_arr[:, 0] + 1.0) * test_prev_close
            pred_high_raw = (y_pred_feature_arr[:, 1] + 1.0) * test_prev_close
            pred_low_raw = (y_pred_feature_arr[:, 2] + 1.0) * test_prev_close
            y_pred = (y_pred_feature_arr[:, 3] + 1.0) * test_prev_close

            pred_high = np.maximum.reduce([pred_high_raw, pred_open, y_pred])
            pred_low = np.minimum.reduce([pred_low_raw, pred_open, y_pred])

            mae = mean_absolute_error(y_true, y_pred)
            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            results[symbol].append(
                FoldResult(
                    train_months=train_months,
                    test_month=test_month,
                    y_train_true=y_train_true,
                    y_train_pred=y_train_pred,
                    y_true=y_true,
                    y_pred=y_pred,
                    pred_open=pred_open,
                    pred_high=pred_high,
                    pred_low=pred_low,
                    timestamps=test_df["timestamp"].reset_index(drop=True),
                    train_mae=train_mae,
                    train_rmse=train_rmse,
                    mae=mae,
                    rmse=rmse,
                )
            )

            print(
                f"Validation test={test_month}, symbol={symbol}: "
                f"Train MAE={train_mae:.4f}, Train RMSE={train_rmse:.4f}, "
                f"Test MAE={mae:.4f}, Test RMSE={rmse:.4f}, "
                f"train_months={train_months[0]}..{train_months[-1]}"
            ) if (symbol in log_symbols or verbose > 0) else None

    return results


def summarize_all_data_and_forecast_shared(
    symbol_dfs: Dict[str, pd.DataFrame],
    forecast_symbols: List[str],
    lookback: int,
    epochs: int,
    batch_size: int,
    verbose: int,
    model_path: Path,
) -> Tuple[Dict[str, FoldResult], Dict[str, pd.DataFrame]]:
    symbol_monthly_df: Dict[str, pd.DataFrame] = {}
    all_months = set()
    for symbol, df in symbol_dfs.items():
        sdf = df.copy()
        sdf["month"] = sdf["timestamp"].dt.to_period("M")
        symbol_monthly_df[symbol] = sdf
        all_months.update(sdf["month"].unique())

    months = sorted(all_months)
    if len(months) < 1:
        raise ValueError("Need at least 1 month of data.")

    train_months = months
    x_train_all, y_train_all = build_train_set_shared(
        symbol_monthly_df=symbol_monthly_df,
        train_months=train_months,
        lookback=lookback,
    )

    if model_path.exists():
        print(f"Loading existing model from {model_path}")
        model = load_model(model_path)
    else:
        model = build_cnn_model(lookback, feature_count=len(FEATURE_COLS))

    model.fit(
        x_train_all,
        y_train_all,
        epochs=epochs,
        batch_size=batch_size,
        verbose=verbose,
        callbacks=[EarlyStopping(monitor="loss", patience=2, restore_best_weights=True)],
    )

    model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save(model_path)
    print(f"Saved model to {model_path}")

    all_data_results: Dict[str, FoldResult] = {}
    forecasts: Dict[str, pd.DataFrame] = {}
    for symbol, df in symbol_monthly_df.items():
        train_df = df[df["month"].isin(train_months)].reset_index(drop=True)
        if len(train_df) <= lookback:
            all_data_results[symbol] = FoldResult(
                train_months=train_months,
                test_month=train_months[-1],
                y_train_true=np.array([]),
                y_train_pred=np.array([]),
                y_true=np.array([]),
                y_pred=np.array([]),
                pred_open=np.array([]),
                pred_high=np.array([]),
                pred_low=np.array([]),
                timestamps=pd.Series(dtype="datetime64[ns]"),
                train_mae=float("nan"),
                train_rmse=float("nan"),
                mae=float("nan"),
                rmse=float("nan"),
            )
            forecasts[symbol] = pd.DataFrame(columns=["timestamp", "pred_open", "pred_high", "pred_low", "pred_close"])
            continue

        train_features = train_df[FEATURE_COLS].values
        train_targets = train_df[FEATURE_COLS].values
        x_train_symbol, _ = make_sequences(train_features, train_targets, lookback)
        train_pred_vec = model(x_train_symbol, training=False).numpy()
        train_prev_close = train_df["prev_close"].values[lookback:]
        y_train_true = train_df["close"].values[lookback:]
        y_train_pred = (train_pred_vec[:, 3] + 1.0) * train_prev_close
        train_mae = mean_absolute_error(y_train_true, y_train_pred)
        train_rmse = np.sqrt(mean_squared_error(y_train_true, y_train_pred))

        all_data_results[symbol] = FoldResult(
            train_months=train_months,
            test_month=train_months[-1],
            y_train_true=y_train_true,
            y_train_pred=y_train_pred,
            y_true=np.array([]),
            y_pred=np.array([]),
            pred_open=np.array([]),
            pred_high=np.array([]),
            pred_low=np.array([]),
            timestamps=pd.Series(dtype="datetime64[ns]"),
            train_mae=train_mae,
            train_rmse=train_rmse,
            mae=float("nan"),
            rmse=float("nan"),
        )

        if symbol not in forecast_symbols:
            forecasts[symbol] = pd.DataFrame(columns=["timestamp", "pred_open", "pred_high", "pred_low", "pred_close"])
            continue

        diffs = train_df["timestamp"].diff().dropna()
        step = diffs.median() if not diffs.empty else pd.Timedelta(days=1)
        last_ts = train_df["timestamp"].max()
        month_counts = train_df.groupby(train_df["timestamp"].dt.to_period("M")).size()
        points_to_forecast = int(month_counts.tail(3).median()) if not month_counts.empty else 30
        points_to_forecast = max(points_to_forecast, 1)
        future_ts = [last_ts + (i + 1) * step for i in range(points_to_forecast)]

        history_features = train_features.copy()
        history_close = train_df["close"].values.tolist()
        future_open = []
        future_high = []
        future_low = []
        future_close = []
        train_feature_ref = train_df[FEATURE_COLS].values
        for _ in range(points_to_forecast):
            x_input = history_features[-lookback:].reshape(1, lookback, len(FEATURE_COLS))
            raw_pred_vec = predict_feature_vector(model, x_input)
            pred_vec = clip_feature_vector(raw_pred_vec, train_feature_ref)

            prev_close = float(history_close[-1])
            pred_open = (pred_vec[0] + 1.0) * prev_close
            pred_high_raw = (pred_vec[1] + 1.0) * prev_close
            pred_low_raw = (pred_vec[2] + 1.0) * prev_close
            pred_close = (pred_vec[3] + 1.0) * prev_close

            pred_high = max(pred_high_raw, pred_open, pred_close)
            pred_low = min(pred_low_raw, pred_open, pred_close)

            future_open.append(pred_open)
            future_high.append(pred_high)
            future_low.append(pred_low)
            future_close.append(pred_close)
            history_close.append(pred_close)

            history_features = np.vstack([history_features, pred_vec])

        forecasts[symbol] = pd.DataFrame(
            {
                "timestamp": future_ts,
                "pred_open": future_open,
                "pred_high": future_high,
                "pred_low": future_low,
                "pred_close": future_close,
            }
        )

    return all_data_results, forecasts


def plot_results(
    df: pd.DataFrame,
    fold_results: List[FoldResult],
    next_month_df: pd.DataFrame,
    output_path: str,
    title_suffix: str,
) -> None:
    fig, ax = plt.subplots(figsize=(14, 7))

    all_timestamps = [df["timestamp"]]
    for fold in fold_results:
        all_timestamps.append(fold.timestamps)
    if not next_month_df.empty:
        all_timestamps.append(next_month_df["timestamp"])

    all_ts = pd.concat(all_timestamps).sort_values().reset_index(drop=True)
    if len(all_ts) > 1:
        spacing = mdates.date2num(all_ts.iloc[1]) - mdates.date2num(all_ts.iloc[0])
    else:
        spacing = 1.0
    candle_width = max(spacing * 0.6, 1e-4)

    def draw_candles(
        candle_df: pd.DataFrame,
        color: str,
        label: str | None,
    ) -> None:
        added_label = False
        price_span = float(candle_df["high"].max() - candle_df["low"].min()) if not candle_df.empty else 0.0
        min_body = max(price_span * 0.004, 1e-4)
        for _, row in candle_df.iterrows():
            x = mdates.date2num(pd.to_datetime(row["timestamp"]))
            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])

            ax.vlines(x, l, h, color=color, linewidth=1.1, alpha=0.95, zorder=2)
            body_low = min(o, c)
            body_height = max(abs(c - o), min_body)
            rect = Rectangle(
                (x - candle_width / 2.0, body_low),
                candle_width,
                body_height,
                facecolor=color,
                edgecolor="white",
                linewidth=0.35,
                alpha=0.9,
                zorder=3,
                label=label if (label and not added_label) else None,
            )
            ax.add_patch(rect)
            if label and not added_label:
                added_label = True

    actual_df = df[["timestamp", "open", "high", "low", "close"]].copy()
    draw_candles(actual_df, color="black", label="Actual")

    for i, fold in enumerate(fold_results):
        pred_df = pd.DataFrame(
            {
                "timestamp": fold.timestamps,
                "open": fold.pred_open,
                "high": fold.pred_high,
                "low": fold.pred_low,
                "close": fold.y_pred,
            }
        )
        draw_candles(pred_df, color="tab:blue", label="Validation Prediction" if i == 0 else None)

    if not next_month_df.empty:
        forecast_df = next_month_df.rename(
            columns={
                "pred_open": "open",
                "pred_high": "high",
                "pred_low": "low",
                "pred_close": "close",
            }
        )[["timestamp", "open", "high", "low", "close"]]
        draw_candles(forecast_df, color="tab:orange", label="Next-Month Forecast")

    ax.set_title(f"1D CNN Holdout Validation + Next-Month Forecast ({title_suffix})")
    ax.set_xlabel("Time")
    ax.set_ylabel("Price")
    ax.grid(alpha=0.25)
    ax.legend()
    ax.xaxis_date()
    fig.autofmt_xdate()
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close(fig)


def main() -> None:
    load_env_file()

    parser = argparse.ArgumentParser(
        description="1D CNN rolling walk-forward validation on DB stock data with a shared model across symbols."
    )
    parser.add_argument(
        "symbols",
        nargs="*",
        help="Optional ticker symbols. If omitted, the script loads all symbols from incrementum.stock.",
    )
    parser.add_argument("--interval", default="1d", help="DB frequency: 1h or 1d (default: 1d)")
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL URL. Defaults to DB_CONN_STRING or DATABASE_URL from .env/env.",
    )
    parser.add_argument("--lookback", type=int, default=24, help="Sequence length for CNN input")
    parser.add_argument("--epochs", type=int, default=6, help="Epochs per training run")
    parser.add_argument("--batch-size", type=int, default=32, help="Training batch size")
    parser.add_argument("--verbose", type=int, default=0, help="Keras verbosity")
    parser.add_argument(
        "--plot-all",
        action="store_true",
        help="Plot results for all trained symbols (default plots only forecast symbols).",
    )
    parser.add_argument(
        "--forecast-symbols",
        nargs="*",
        default=DEFAULT_FORECAST_SYMBOLS,
        help="Symbols to forecast after all-stock training (default: AAPL MSFT)",
    )
    args = parser.parse_args()

    database_url = resolve_database_url(args.database_url)
    symbols = parse_symbols(args.symbols) if args.symbols else fetch_symbols_from_stock_table(database_url, args.interval)
    forecast_symbols = parse_symbols(args.forecast_symbols)

    print(f"Loaded {len(symbols)} symbols from {'CLI' if args.symbols else 'incrementum.stock'}")

    symbol_dfs: Dict[str, pd.DataFrame] = {}
    skipped_symbols: List[str] = []
    for symbol in symbols:
        try:
            df = fetch_prices_from_db(symbol, database_url, args.interval)
            norm_df = add_previous_close_normalized_columns(df)
            if norm_df.empty:
                skipped_symbols.append(symbol)
                continue
            symbol_dfs[symbol] = norm_df
        except (ValueError, OperationalError, DBAPIError) as exc:
            print(f"Skipping {symbol}: {exc}")
            skipped_symbols.append(symbol)

    if skipped_symbols:
        print(f"Skipped {len(skipped_symbols)} symbols due to insufficient/missing data.")

    if not symbol_dfs:
        raise ValueError("No symbols with usable data were found for training.")

    symbol_dfs, dropped_for_months = filter_symbols_with_min_months(symbol_dfs, MIN_MONTHS_REQUIRED)
    if dropped_for_months:
        print(
            f"Dropped {len(dropped_for_months)} symbols with fewer than {MIN_MONTHS_REQUIRED} months for walk-forward."
        )
    if not symbol_dfs:
        raise ValueError("No symbols have enough month history for walk-forward training.")

    train_symbols = sorted(symbol_dfs.keys())
    missing_forecasts = [s for s in forecast_symbols if s not in symbol_dfs]
    if missing_forecasts:
        print(f"Forecast symbols not available in training data and will be skipped: {', '.join(missing_forecasts)}")
    forecast_symbols = [s for s in forecast_symbols if s in symbol_dfs]
    print_data_ranges(symbol_dfs, args.interval)

    print(
        f"\nTraining one shared rolling model across {len(train_symbols)} symbols "
        f"(train window = {WALK_FORWARD_TRAIN_MONTHS} months, test = next month)"
    )
    fold_results_by_symbol = walk_forward_monthly_shared(
        symbol_dfs=symbol_dfs,
        lookback=args.lookback,
        epochs=args.epochs,
        batch_size=args.batch_size,
        verbose=args.verbose,
        log_symbols=forecast_symbols,
    )

    all_data_results_by_symbol, forecasts_by_symbol = summarize_all_data_and_forecast_shared(
        symbol_dfs=symbol_dfs,
        forecast_symbols=forecast_symbols,
        lookback=args.lookback,
        epochs=args.epochs,
        batch_size=args.batch_size,
        verbose=args.verbose,
        model_path=MODEL_ARTIFACT_PATH,
    )

    summary_rows = []
    for symbol in train_symbols:
        fold_results = fold_results_by_symbol.get(symbol, [])
        if not fold_results:
            print(f"No validation results generated for symbol {symbol}.")
            continue

        all_train_true = np.concatenate([fold.y_train_true for fold in fold_results])
        all_train_pred = np.concatenate([fold.y_train_pred for fold in fold_results])
        all_test_true = np.concatenate([fold.y_true for fold in fold_results])
        all_test_pred = np.concatenate([fold.y_pred for fold in fold_results])

        overall_train_mae = mean_absolute_error(all_train_true, all_train_pred)
        overall_train_rmse = np.sqrt(mean_squared_error(all_train_true, all_train_pred))
        overall_test_mae = mean_absolute_error(all_test_true, all_test_pred)
        overall_test_rmse = np.sqrt(mean_squared_error(all_test_true, all_test_pred))

        all_data_result = all_data_results_by_symbol.get(symbol)

        print(f"\n=== Summary for {symbol} (shared-model rolling walk-forward) ===")
        print(f"Validation Train MAE: {overall_train_mae:.4f}")
        print(f"Validation Train RMSE: {overall_train_rmse:.4f}")
        print(f"Validation Test MAE: {overall_test_mae:.4f}")
        print(f"Validation Test RMSE: {overall_test_rmse:.4f}")
        print(f"Validation Gap (MAE): {overall_test_mae - overall_train_mae:.4f}")
        print(f"Validation Gap (RMSE): {overall_test_rmse - overall_train_rmse:.4f}")
        if all_data_result is not None and not np.isnan(all_data_result.train_mae):
            print(f"All-Data Train MAE: {all_data_result.train_mae:.4f}")
            print(f"All-Data Train RMSE: {all_data_result.train_rmse:.4f}")

        should_plot = args.plot_all or symbol in forecast_symbols
        if should_plot:
            output_path = f"predictions_{symbol}.png"
            plot_results(
                df=symbol_dfs[symbol],
                fold_results=fold_results,
                next_month_df=forecasts_by_symbol.get(symbol, pd.DataFrame()),
                output_path=output_path,
                title_suffix=f"{symbol} - Shared Model",
            )
            print(f"Saved plot: {output_path}")

        summary_rows.append(
            {
                "symbol": symbol,
                "train_mae": overall_train_mae,
                "train_rmse": overall_train_rmse,
                "test_mae": overall_test_mae,
                "test_rmse": overall_test_rmse,
                "gap_mae": overall_test_mae - overall_train_mae,
                "gap_rmse": overall_test_rmse - overall_train_rmse,
                "all_data_train_mae": all_data_result.train_mae if all_data_result is not None else np.nan,
                "all_data_train_rmse": all_data_result.train_rmse if all_data_result is not None else np.nan,
            }
        )

    if not summary_rows:
        raise ValueError("No valid rolling walk-forward results were produced for the provided symbols.")

    if len(summary_rows) > 1:
        print("\n=== Multi-Symbol Summary (shared model, rolling walk-forward + all-data train) ===")
        summary_df = pd.DataFrame(summary_rows)
        print(summary_df.to_string(index=False, float_format=lambda x: f"{x:.4f}"))


if __name__ == "__main__":
    main()