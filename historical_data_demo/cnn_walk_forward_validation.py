import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Set, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sqlalchemy import create_engine, text
from keras import Model
from keras.layers import Concatenate, Conv1D, Dense, Embedding, Flatten, Input
from keras.optimizers import Adam
import json
from datetime import datetime


MODEL_INPUT_COLUMNS = ["log_open", "log_high", "log_low", "log_close", "log_volume"]
MODEL_INPUT_NORM_COLUMNS = [f"{column}_norm" for column in MODEL_INPUT_COLUMNS]


def mean_absolute_percentage_error(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Calculate MAPE (Mean Absolute Percentage Error)."""
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100


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
    val_month: pd.Period
    test_month: pd.Period
    train_samples: int
    val_samples: int
    test_samples: int
    val_mae_log_return: float
    val_rmse_log_return: float
    test_mae_log_return: float
    test_rmse_log_return: float
    test_mape_close: float


def fetch_prices(symbol: str, period: str, interval: str) -> pd.DataFrame:
    raise NotImplementedError("Use fetch_prices_from_db for database-backed data loading.")


def fetch_available_stocks(database_url: str, limit: int) -> List[str]:
    """Fetch the first N unique stock symbols from the database."""
    query = text(
        """
        SELECT DISTINCT stock_symbol
        FROM incrementum.stock_history
        ORDER BY stock_symbol
        LIMIT :limit
        """
    )
    engine = create_engine(database_url)
    with engine.connect() as connection:
        result = pd.read_sql(query, connection, params={"limit": limit})
    return result["stock_symbol"].tolist()


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
                        close_price / 100.0 AS close,
                        volume
        FROM incrementum.stock_history
        WHERE stock_symbol = :symbolpy
          AND is_hourly = :is_hourly
        ORDER BY day_and_time ASC
        """
    )

    engine = create_engine(database_url)
    with engine.connect() as connection:
        df = pd.read_sql(query, connection, params={"symbolpy": symbol.upper(), "is_hourly": is_hourly})

    if df.empty:
        frequency = "hourly" if is_hourly else "daily"
        raise ValueError(
            f"No {frequency} rows found in incrementum.stock_history for symbol '{symbol.upper()}'."
        )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


def build_cnn_model(
    lookback: int,
    num_features: int,
    num_symbols: int,
    symbol_embedding_dim: int,
) -> Model:
    feature_input = Input(shape=(lookback, num_features), name="feature_input")
    symbol_input = Input(shape=(lookback,), dtype="int32", name="symbol_input")

    symbol_embedding = Embedding(
        input_dim=num_symbols,
        output_dim=symbol_embedding_dim,
        name="symbol_embedding",
    )(symbol_input)

    x = Concatenate(axis=-1, name="feature_symbol_concat")([feature_input, symbol_embedding])
    x = Conv1D(filters=64, kernel_size=3, activation="relu")(x)
    x = Conv1D(filters=16, kernel_size=3, activation="relu")(x)
    x = Flatten()(x)
    x = Dense(64, activation="relu")(x)
    output = Dense(1, name="predicted_log_return_norm")(x)

    model = Model(inputs=[feature_input, symbol_input], outputs=output)
    model.compile(optimizer=Adam(learning_rate=0.001), loss="mse")
    return model


def prepare_stock_features(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    frame["log_return"] = np.log(frame["close"]).diff()
    frame["log_open"] = np.log(frame["open"].clip(lower=1e-12))
    frame["log_high"] = np.log(frame["high"].clip(lower=1e-12))
    frame["log_low"] = np.log(frame["low"].clip(lower=1e-12))
    frame["log_close"] = np.log(frame["close"].clip(lower=1e-12))
    frame["log_volume"] = np.log1p(frame["volume"].clip(lower=0))
    frame["month"] = frame["timestamp"].dt.to_period("M")
    return frame.dropna(subset=["log_return", *MODEL_INPUT_COLUMNS]).reset_index(drop=True)


def make_sequences_for_split(
    stock_frames: Dict[str, pd.DataFrame],
    symbol_to_id: Dict[str, int],
    lookback: int,
    target_months: Set[pd.Period],
    allowed_months: Set[pd.Period],
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    x: List[np.ndarray] = []
    x_symbol_ids: List[np.ndarray] = []
    y_norm: List[float] = []
    y_raw: List[float] = []
    close_context: List[Tuple[float, float]] = []

    for symbol, frame in stock_frames.items():
        symbol_id = symbol_to_id[symbol]
        frame_allowed = frame[frame["month"].isin(allowed_months)].reset_index(drop=True)
        if len(frame_allowed) <= lookback:
            continue

        features = frame_allowed[MODEL_INPUT_NORM_COLUMNS].to_numpy()
        for i in range(lookback, len(frame_allowed)):
            if frame_allowed.at[i, "month"] not in target_months:
                continue

            window = features[i - lookback:i]
            if not np.isfinite(window).all():
                continue

            x.append(window)
            x_symbol_ids.append(np.full((lookback,), symbol_id, dtype=np.int32))
            y_norm.append(float(frame_allowed.at[i, "log_return_norm"]))
            y_raw.append(float(frame_allowed.at[i, "log_return"]))
            close_context.append((float(frame_allowed.at[i - 1, "close"]), float(frame_allowed.at[i, "close"])))

    if not x:
        return (
            np.empty((0, lookback, len(MODEL_INPUT_NORM_COLUMNS)), dtype=float),
            np.empty((0, lookback), dtype=np.int32),
            np.empty((0,), dtype=float),
            np.empty((0,), dtype=float),
            np.empty((0, 2), dtype=float),
        )

    return (
        np.array(x, dtype=float),
        np.array(x_symbol_ids, dtype=np.int32),
        np.array(y_norm, dtype=float),
        np.array(y_raw, dtype=float),
        np.array(close_context, dtype=float),
    )


def run_multi_stock_walk_forward(
    stock_frames: Dict[str, pd.DataFrame],
    symbol_to_id: Dict[str, int],
    lookback: int,
    epochs: int,
    batch_size: int,
    symbol_embedding_dim: int,
    verbose: int,
) -> List[FoldResult]:
    month_set: Set[pd.Period] = set()
    for frame in stock_frames.values():
        month_set.update(frame["month"].unique())
    months = sorted(month_set)

    if len(months) < 14:
        raise ValueError("Need at least 14 months of data for 12-train / 1-val / 1-test walk-forward.")

    results: List[FoldResult] = []

    for test_idx in range(13, len(months)):
        train_months = months[test_idx - 13:test_idx - 1]
        val_month = months[test_idx - 1]
        test_month = months[test_idx]

        train_rows = []
        for frame in stock_frames.values():
            subset = frame[frame["month"].isin(set(train_months))]
            if not subset.empty:
                train_rows.append(subset)

        if not train_rows:
            continue

        train_concat = pd.concat(train_rows, ignore_index=True)
        ret_mean = float(train_concat["log_return"].mean())
        ret_std = float(train_concat["log_return"].std(ddof=0))
        feature_stats: Dict[str, Tuple[float, float]] = {}
        for feature_column in MODEL_INPUT_COLUMNS:
            feature_mean = float(train_concat[feature_column].mean())
            feature_std = float(train_concat[feature_column].std(ddof=0))
            if feature_std == 0 or not np.isfinite(feature_std):
                feature_std = 1.0
            feature_stats[feature_column] = (feature_mean, feature_std)

        if ret_std == 0 or not np.isfinite(ret_std):
            ret_std = 1.0

        normalized_frames: Dict[str, pd.DataFrame] = {}
        for symbol, frame in stock_frames.items():
            normalized = frame.copy()
            normalized["log_return_norm"] = (normalized["log_return"] - ret_mean) / ret_std
            for feature_column in MODEL_INPUT_COLUMNS:
                feature_mean, feature_std = feature_stats[feature_column]
                normalized[f"{feature_column}_norm"] = (
                    normalized[feature_column] - feature_mean
                ) / feature_std
            normalized_frames[symbol] = normalized

        x_train, x_symbol_train, y_train, _, _ = make_sequences_for_split(
            stock_frames=normalized_frames,
            symbol_to_id=symbol_to_id,
            lookback=lookback,
            target_months=set(train_months),
            allowed_months=set(train_months),
        )
        x_val, x_symbol_val, y_val_norm, y_val_raw, val_context = make_sequences_for_split(
            stock_frames=normalized_frames,
            symbol_to_id=symbol_to_id,
            lookback=lookback,
            target_months={val_month},
            allowed_months=set(train_months + [val_month]),
        )
        x_test, x_symbol_test, y_test_norm, y_test_raw, test_context = make_sequences_for_split(
            stock_frames=normalized_frames,
            symbol_to_id=symbol_to_id,
            lookback=lookback,
            target_months={test_month},
            allowed_months=set(train_months + [val_month, test_month]),
        )

        if len(x_train) == 0 or len(x_val) == 0 or len(x_test) == 0:
            print(
                f"Skipping fold test={test_month}: not enough train/val/test sequences "
                f"(train={len(x_train)}, val={len(x_val)}, test={len(x_test)})."
            )
            continue

        model = build_cnn_model(
            lookback,
            num_features=len(MODEL_INPUT_NORM_COLUMNS),
            num_symbols=len(symbol_to_id),
            symbol_embedding_dim=symbol_embedding_dim,
        )
        model.fit(
            [x_train, x_symbol_train],
            y_train,
            validation_data=([x_val, x_symbol_val], y_val_norm),
            epochs=epochs,
            batch_size=batch_size,
            verbose=verbose,
        )

        val_pred_norm = model.predict([x_val, x_symbol_val], verbose=0).flatten()
        test_pred_norm = model.predict([x_test, x_symbol_test], verbose=0).flatten()

        val_pred_raw = val_pred_norm * ret_std + ret_mean
        test_pred_raw = test_pred_norm * ret_std + ret_mean

        val_mae = mean_absolute_error(y_val_raw, val_pred_raw)
        val_rmse = np.sqrt(mean_squared_error(y_val_raw, val_pred_raw))
        test_mae = mean_absolute_error(y_test_raw, test_pred_raw)
        test_rmse = np.sqrt(mean_squared_error(y_test_raw, test_pred_raw))

        prev_close_test = test_context[:, 0]
        actual_close_test = test_context[:, 1]
        predicted_close_test = prev_close_test * np.exp(test_pred_raw)
        test_mape_close = mean_absolute_percentage_error(actual_close_test, predicted_close_test)

        fold_result = FoldResult(
            train_months=train_months,
            val_month=val_month,
            test_month=test_month,
            train_samples=len(x_train),
            val_samples=len(x_val),
            test_samples=len(x_test),
            val_mae_log_return=val_mae,
            val_rmse_log_return=val_rmse,
            test_mae_log_return=test_mae,
            test_rmse_log_return=test_rmse,
            test_mape_close=test_mape_close,
        )
        results.append(fold_result)

        print(
            f"Fold test={test_month}: train={train_months[0]}..{train_months[-1]}, val={val_month}, "
            f"samples(train/val/test)={len(x_train)}/{len(x_val)}/{len(x_test)}, "
            f"val_RMSE(logret)={val_rmse:.6f}, test_RMSE(logret)={test_rmse:.6f}, "
            f"test_MAPE(close)={test_mape_close:.2f}%"
        )

    return results


def get_sorted_months(stock_frames: Dict[str, pd.DataFrame]) -> List[pd.Period]:
    month_set: Set[pd.Period] = set()
    for frame in stock_frames.values():
        month_set.update(frame["month"].unique())
    return sorted(month_set)


def train_shared_model_for_window(
    stock_frames: Dict[str, pd.DataFrame],
    symbol_to_id: Dict[str, int],
    train_months: List[pd.Period],
    val_month: pd.Period,
    lookback: int,
    epochs: int,
    batch_size: int,
    symbol_embedding_dim: int,
    verbose: int,
) -> Tuple[Model, Dict[str, pd.DataFrame], float, float]:
) -> Tuple[Model, Dict[str, pd.DataFrame], float, float, Dict[str, Tuple[float, float]]]:
    train_rows: List[pd.DataFrame] = []
    for frame in stock_frames.values():
        subset = frame[frame["month"].isin(set(train_months))]
        if not subset.empty:
            train_rows.append(subset)

    if not train_rows:
        raise ValueError("No training rows found for final next-month window.")

    train_concat = pd.concat(train_rows, ignore_index=True)
    ret_mean = float(train_concat["log_return"].mean())
    ret_std = float(train_concat["log_return"].std(ddof=0))
    if ret_std == 0 or not np.isfinite(ret_std):
        ret_std = 1.0

    feature_stats: Dict[str, Tuple[float, float]] = {}
    for feature_column in MODEL_INPUT_COLUMNS:
        feature_mean = float(train_concat[feature_column].mean())
        feature_std = float(train_concat[feature_column].std(ddof=0))
        if feature_std == 0 or not np.isfinite(feature_std):
            feature_std = 1.0
        feature_stats[feature_column] = (feature_mean, feature_std)

    normalized_frames: Dict[str, pd.DataFrame] = {}
    for symbol, frame in stock_frames.items():
        normalized = frame.copy()
        normalized["log_return_norm"] = (normalized["log_return"] - ret_mean) / ret_std
        for feature_column in MODEL_INPUT_COLUMNS:
            feature_mean, feature_std = feature_stats[feature_column]
            normalized[f"{feature_column}_norm"] = (normalized[feature_column] - feature_mean) / feature_std
        normalized_frames[symbol] = normalized

    x_train, x_symbol_train, y_train, _, _ = make_sequences_for_split(
        stock_frames=normalized_frames,
        symbol_to_id=symbol_to_id,
        lookback=lookback,
        target_months=set(train_months),
        allowed_months=set(train_months),
    )
    x_val, x_symbol_val, y_val_norm, _, _ = make_sequences_for_split(
        stock_frames=normalized_frames,
        symbol_to_id=symbol_to_id,
        lookback=lookback,
        target_months={val_month},
        allowed_months=set(train_months + [val_month]),
    )

    if len(x_train) == 0 or len(x_val) == 0:
        raise ValueError(
            "Not enough train/validation sequences to train final next-month prediction model."
        )

    model = build_cnn_model(
        lookback,
        num_features=len(MODEL_INPUT_NORM_COLUMNS),
        num_symbols=len(symbol_to_id),
        symbol_embedding_dim=symbol_embedding_dim,
    )
    model.fit(
        [x_train, x_symbol_train],
        y_train,
        validation_data=([x_val, x_symbol_val], y_val_norm),
        epochs=epochs,
        batch_size=batch_size,
        verbose=verbose,
    )

    return model, normalized_frames, ret_mean, ret_std, feature_stats


def predict_month_for_symbol(
    model: Model,
    symbol_id: int,
    normalized_frame: pd.DataFrame,
    lookback: int,
    train_months: List[pd.Period],
    val_month: pd.Period,
    test_month: pd.Period,
    ret_mean: float,
    ret_std: float,
) -> pd.DataFrame:
    frame_allowed = normalized_frame[
        normalized_frame["month"].isin(set(train_months + [val_month, test_month]))
    ].reset_index(drop=True)

    if len(frame_allowed) <= lookback:
        return pd.DataFrame()

    features = frame_allowed[MODEL_INPUT_NORM_COLUMNS].to_numpy()
    rows: List[Dict[str, float]] = []

    for i in range(lookback, len(frame_allowed)):
        if frame_allowed.at[i, "month"] != test_month:
            continue

        window = features[i - lookback:i]
        if not np.isfinite(window).all():
            continue

        symbol_window = np.full((1, lookback), symbol_id, dtype=np.int32)
        pred_norm = float(
            model.predict(
                [window.reshape(1, lookback, len(MODEL_INPUT_NORM_COLUMNS)), symbol_window],
                verbose=0,
            )[0][0]
        )
        pred_raw = pred_norm * ret_std + ret_mean
        prev_close = float(frame_allowed.at[i - 1, "close"])
        pred_close = prev_close * np.exp(pred_raw)

        rows.append(
            {
                "timestamp": frame_allowed.at[i, "timestamp"],
                "actual_close": float(frame_allowed.at[i, "close"]),
                "pred_close": float(pred_close),
            }
        )

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows)


def plot_next_month_predictions(predictions_by_symbol: Dict[str, pd.DataFrame], test_month: pd.Period) -> None:
    symbols = list(predictions_by_symbol.keys())
    if not symbols:
        return

    fig, axes = plt.subplots(len(symbols), 1, figsize=(14, 4 * len(symbols)), sharex=False)
    if len(symbols) == 1:
        axes = [axes]

    for idx, symbol in enumerate(symbols):
        ax = axes[idx]
        pred_df = predictions_by_symbol[symbol]
        ax.plot(pred_df["timestamp"], pred_df["actual_close"], color="black", linewidth=1.3, label="Actual close")
        ax.plot(
            pred_df["timestamp"],
            pred_df["pred_close"],
            color="tab:blue",
            linestyle="--",
            linewidth=1.3,
            label="Predicted close",
        )
        stock_mape = mean_absolute_percentage_error(
            pred_df["actual_close"].to_numpy(),
            pred_df["pred_close"].to_numpy(),
        )
        ax.set_title(f"{symbol} - Next-Month Prediction ({test_month}) | MAPE={stock_mape:.2f}%")
        ax.set_xlabel("Timestamp")
        ax.set_ylabel("Close")
        ax.grid(alpha=0.25)
        ax.legend()

    plt.tight_layout()
    output_path = "next_month_two_stock_predictions.png"
    plt.savefig(output_path)
    print(f"Saved comparison graph to {output_path}")


def main() -> None:
    load_env_file()

    parser = argparse.ArgumentParser(
        description=(
            "Shared multi-stock 1D CNN walk-forward validation. "
            "Uses 12 months train, month 13 validation, month 14 test, then rolls forward."
        )
    )
    parser.add_argument("--interval", default="1h", help="DB frequency: 1h or 1d (default: 1h)")
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL URL. Defaults to DB_CONN_STRING or DATABASE_URL from .env/env.",
    )
    parser.add_argument("--lookback", type=int, default=24, help="Sequence length for CNN input")
    parser.add_argument("--epochs", type=int, default=20, help="Epochs per fold")
    parser.add_argument("--batch-size", type=int, default=32, help="Training batch size")
    parser.add_argument(
        "--symbol-embedding-dim",
        type=int,
        default=8,
        help="Embedding dimension for stock symbol identity (default: 8)",
    )
    parser.add_argument("--verbose", type=int, default=0, help="Keras verbosity")
    parser.add_argument("--max-stocks", type=int, default=100, help="Number of symbols to include (default: 100)")
    args = parser.parse_args()

    database_url = resolve_database_url(args.database_url)

    symbols = fetch_available_stocks(database_url, limit=args.max_stocks)
    if not symbols:
        raise ValueError("No stock symbols found in incrementum.stock_history.")

    print(f"Loading data for up to {args.max_stocks} stocks...")
    stock_frames: Dict[str, pd.DataFrame] = {}
    skipped = 0

    for symbol in symbols:
        try:
            raw_df = fetch_prices_from_db(symbol, database_url, args.interval)
            feature_df = prepare_stock_features(raw_df)
            if len(feature_df) <= args.lookback:
                skipped += 1
                continue
            stock_frames[symbol] = feature_df
        except Exception as exc:
            skipped += 1
            print(f"Skipping {symbol}: {exc}")

    if not stock_frames:
        raise ValueError("No symbols had enough usable data after feature preparation.")

    symbol_to_id = {symbol: idx for idx, symbol in enumerate(sorted(stock_frames.keys()))}

    print(f"Loaded {len(stock_frames)} symbols, skipped {skipped}. Running rolling walk-forward validation...")
    fold_results = run_multi_stock_walk_forward(
        stock_frames=stock_frames,
        symbol_to_id=symbol_to_id,
        lookback=args.lookback,
        epochs=args.epochs,
        batch_size=args.batch_size,
        symbol_embedding_dim=args.symbol_embedding_dim,
        verbose=args.verbose,
    )

    if not fold_results:
        raise ValueError("No folds were produced. Try smaller lookback, daily interval, or more historical data.")

    avg_val_rmse = float(np.mean([fold.val_rmse_log_return for fold in fold_results]))
    avg_test_rmse = float(np.mean([fold.test_rmse_log_return for fold in fold_results]))
    avg_test_mape = float(np.mean([fold.test_mape_close for fold in fold_results]))

    print("\n=== Overall Results Across Folds ===")
    print(f"Folds run: {len(fold_results)}")
    print(f"Average Validation RMSE (log return): {avg_val_rmse:.6f}")
    print(f"Average Test RMSE (log return): {avg_test_rmse:.6f}")
    print(f"Average Test MAPE (close): {avg_test_mape:.2f}%")

    months = get_sorted_months(stock_frames)
    if len(months) < 14:
        print("Not enough months for final next-month plotting window.")
        return

    final_train_months = months[-14:-2]
    final_val_month = months[-2]
    final_test_month = months[-1]

    print(
        "\nRunning final next-month prediction analysis with shared model: "
        f"train={final_train_months[0]}..{final_train_months[-1]}, "
        f"val={final_val_month}, test={final_test_month}"
    )

    final_model, normalized_frames, ret_mean, ret_std = train_shared_model_for_window(
        stock_frames=stock_frames,
        symbol_to_id=symbol_to_id,
        train_months=final_train_months,
        val_month=final_val_month,
        lookback=args.lookback,
        epochs=args.epochs,
        batch_size=args.batch_size,
        symbol_embedding_dim=args.symbol_embedding_dim,
        verbose=args.verbose,
    )

    candidate_symbols = sorted(list(stock_frames.keys()))
    predictions_by_symbol: Dict[str, pd.DataFrame] = {}
    for symbol in candidate_symbols:
        pred_df = predict_month_for_symbol(
            model=final_model,
            symbol_id=symbol_to_id[symbol],
            normalized_frame=normalized_frames[symbol],
            lookback=args.lookback,
            train_months=final_train_months,
            val_month=final_val_month,
            test_month=final_test_month,
            ret_mean=ret_mean,
            ret_std=ret_std,
        )
        if pred_df.empty:
            continue
        predictions_by_symbol[symbol] = pred_df
        if len(predictions_by_symbol) == 2:
            break

    if len(predictions_by_symbol) < 2:
        print(
            "Could not find two symbols with enough next-month prediction points "
            "for plotting."
        )
        return

    print("\nPer-stock next-month prediction quality:")
    for symbol, pred_df in predictions_by_symbol.items():
        stock_mape = mean_absolute_percentage_error(
            pred_df["actual_close"].to_numpy(),
            pred_df["pred_close"].to_numpy(),
        )
        print(
            f"  {symbol}: rows={len(pred_df)}, "
            f"MAPE={stock_mape:.2f}%"
        )

    plot_next_month_predictions(
        predictions_by_symbol=predictions_by_symbol,
        test_month=final_test_month,
    )


if __name__ == "__main__":
    main()