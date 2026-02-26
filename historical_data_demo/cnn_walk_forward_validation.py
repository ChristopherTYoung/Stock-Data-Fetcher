import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine, text
from tensorflow.keras import Sequential
from tensorflow.keras.layers import Conv1D, Dense, Flatten
from tensorflow.keras.optimizers import Adam


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
    y_true: np.ndarray
    y_pred: np.ndarray
    timestamps: pd.Series
    mae: float
    rmse: float


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
            close_price / 100.0 AS close
        FROM incrementum.stock_history
        WHERE stock_symbol = :symbol
          AND is_hourly = :is_hourly
        ORDER BY day_and_time ASC
        """
    )

    engine = create_engine(database_url)
    with engine.connect() as connection:
        df = pd.read_sql(query, connection, params={"symbol": symbol.upper(), "is_hourly": is_hourly})

    if df.empty:
        frequency = "hourly" if is_hourly else "daily"
        raise ValueError(
            f"No {frequency} rows found in incrementum.stock_history for symbol '{symbol.upper()}'."
        )

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


def build_cnn_model(lookback: int) -> Sequential:
    model = Sequential(
        [
            Conv1D(filters=32, kernel_size=3, activation="relu", input_shape=(lookback, 1)),
            Conv1D(filters=16, kernel_size=3, activation="relu"),
            Flatten(),
            Dense(64, activation="relu"),
            Dense(1),
        ]
    )
    model.compile(optimizer=Adam(learning_rate=0.001), loss="mse")
    return model


def make_sequences(series: np.ndarray, lookback: int) -> Tuple[np.ndarray, np.ndarray]:
    x, y = [], []
    for i in range(lookback, len(series)):
        x.append(series[i - lookback:i])
        y.append(series[i])
    x_arr = np.array(x).reshape(-1, lookback, 1)
    y_arr = np.array(y)
    return x_arr, y_arr


def walk_forward_monthly(
    df: pd.DataFrame,
    lookback: int,
    epochs: int,
    batch_size: int,
    verbose: int,
) -> List[FoldResult]:
    df = df.copy()
    df["month"] = df["timestamp"].dt.to_period("M")
    months = sorted(df["month"].unique())

    if len(months) < 7:
        raise ValueError("Need at least 7 months of data for this walk-forward scheme.")

    results: List[FoldResult] = []

    for test_idx in range(6, len(months)):
        train_months = months[test_idx - 6:test_idx]
        test_month = months[test_idx]

        train_df = df[df["month"].isin(train_months)]
        test_df = df[df["month"] == test_month]

        if len(train_df) <= lookback or len(test_df) == 0:
            continue

        scaler = MinMaxScaler()
        train_scaled = scaler.fit_transform(train_df[["close"]].values).flatten()
        x_train, y_train = make_sequences(train_scaled, lookback)

        model = build_cnn_model(lookback)
        model.fit(x_train, y_train, epochs=epochs, batch_size=batch_size, verbose=verbose)

        history_scaled = scaler.transform(train_df[["close"]].values).flatten().tolist()
        y_true = test_df["close"].values
        y_pred_scaled = []

        for actual in y_true:
            x_input = np.array(history_scaled[-lookback:]).reshape(1, lookback, 1)
            pred_scaled = float(model.predict(x_input, verbose=0).flatten()[0])
            y_pred_scaled.append(pred_scaled)

            actual_scaled = float(scaler.transform(np.array([[actual]])).flatten()[0])
            history_scaled.append(actual_scaled)

        y_pred = scaler.inverse_transform(np.array(y_pred_scaled).reshape(-1, 1)).flatten()

        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))
        results.append(
            FoldResult(
                train_months=train_months,
                test_month=test_month,
                y_true=y_true,
                y_pred=y_pred,
                timestamps=test_df["timestamp"].reset_index(drop=True),
                mae=mae,
                rmse=rmse,
            )
        )

        print(
            f"Fold test={test_month}: MAE={mae:.4f}, RMSE={rmse:.4f}, "
            f"train_months={train_months[0]}..{train_months[-1]}"
        )

    return results


def forecast_next_month(
    df: pd.DataFrame,
    lookback: int,
    epochs: int,
    batch_size: int,
    verbose: int,
) -> pd.DataFrame:
    df = df.copy()
    df["month"] = df["timestamp"].dt.to_period("M")
    months = sorted(df["month"].unique())
    if len(months) < 6:
        raise ValueError("Need at least 6 months of data to forecast next month.")

    train_months = months[-6:]
    train_df = df[df["month"].isin(train_months)].reset_index(drop=True)

    scaler = MinMaxScaler()
    train_scaled = scaler.fit_transform(train_df[["close"]].values).flatten()
    x_train, y_train = make_sequences(train_scaled, lookback)

    model = build_cnn_model(lookback)
    model.fit(x_train, y_train, epochs=epochs, batch_size=batch_size, verbose=verbose)

    points_in_last_month = len(df[df["month"] == months[-1]])
    points_to_forecast = max(points_in_last_month, 1)

    diffs = train_df["timestamp"].diff().dropna()
    step = diffs.median() if not diffs.empty else pd.Timedelta(days=1)

    history_scaled = train_scaled.tolist()
    future_scaled = []
    for _ in range(points_to_forecast):
        x_input = np.array(history_scaled[-lookback:]).reshape(1, lookback, 1)
        pred_scaled = float(model.predict(x_input, verbose=0).flatten()[0])
        future_scaled.append(pred_scaled)
        history_scaled.append(pred_scaled)

    future_values = scaler.inverse_transform(np.array(future_scaled).reshape(-1, 1)).flatten()

    last_ts = df["timestamp"].max()
    future_ts = [last_ts + (i + 1) * step for i in range(points_to_forecast)]
    return pd.DataFrame({"timestamp": future_ts, "pred_close": future_values})


def plot_results(df: pd.DataFrame, fold_results: List[FoldResult], next_month_df: pd.DataFrame) -> None:
    plt.figure(figsize=(14, 7))

    plt.plot(df["timestamp"], df["close"], label="Actual close", color="black", linewidth=1.2)

    for i, fold in enumerate(fold_results):
        label = "Walk-forward prediction" if i == 0 else None
        plt.plot(fold.timestamps, fold.y_pred, color="tab:blue", alpha=0.9, linewidth=1.2, label=label)

    if not next_month_df.empty:
        plt.plot(
            next_month_df["timestamp"],
            next_month_df["pred_close"],
            color="tab:orange",
            linestyle="--",
            linewidth=1.8,
            label="Next-month forecast",
        )

    plt.title("1D CNN Walk-Forward Validation + Next-Month Forecast")
    plt.xlabel("Time")
    plt.ylabel("Close Price")
    plt.legend()
    plt.grid(alpha=0.25)
    plt.tight_layout()
    plt.savefig("predictions.png")


def main() -> None:
    load_env_file()

    parser = argparse.ArgumentParser(description="1D CNN walk-forward monthly validation on DB stock data.")
    parser.add_argument("symbol", help="Ticker symbol, e.g. AAPL")
    parser.add_argument("--interval", default="1h", help="DB frequency: 1h or 1d (default: 1h)")
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL URL. Defaults to DB_CONN_STRING or DATABASE_URL from .env/env.",
    )
    parser.add_argument("--lookback", type=int, default=24, help="Sequence length for CNN input")
    parser.add_argument("--epochs", type=int, default=20, help="Epochs per fold")
    parser.add_argument("--batch-size", type=int, default=32, help="Training batch size")
    parser.add_argument("--verbose", type=int, default=0, help="Keras verbosity")
    args = parser.parse_args()

    symbol = args.symbol.strip().upper()
    database_url = resolve_database_url(args.database_url)
    df = fetch_prices_from_db(symbol, database_url, args.interval)
    fold_results = walk_forward_monthly(
        df=df,
        lookback=args.lookback,
        epochs=args.epochs,
        batch_size=args.batch_size,
        verbose=args.verbose,
    )

    if not fold_results:
        raise ValueError("No valid folds were produced. Try a smaller lookback or more data.")

    all_true = np.concatenate([fold.y_true for fold in fold_results])
    all_pred = np.concatenate([fold.y_pred for fold in fold_results])
    overall_mae = mean_absolute_error(all_true, all_pred)
    overall_rmse = np.sqrt(mean_squared_error(all_true, all_pred))
    print(f"Overall MAE: {overall_mae:.4f}")
    print(f"Overall RMSE: {overall_rmse:.4f}")

    next_month_df = forecast_next_month(
        df=df,
        lookback=args.lookback,
        epochs=args.epochs,
        batch_size=args.batch_size,
        verbose=args.verbose,
    )

    plot_results(df, fold_results, next_month_df)


if __name__ == "__main__":
    main()