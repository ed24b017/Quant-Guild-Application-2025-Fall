import argparse
from pathlib import Path

import pandas as pd

from utils import Portfolio, TradeExecutor, Evaluator


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Multi-product trading simulation")
    p.add_argument("--prices", required=True, help="Path to prices CSV")
    p.add_argument("--signals", required=True, help="Path to signals CSV")
    p.add_argument("--initial_capital", type=float, default=1000.0, help="Initial capital in base currency")
    p.add_argument("--tx_cost", type=float, default=0.0, help="Transaction cost as fraction (e.g. 0.001)")
    p.add_argument("--cash_symbol", type=str, default="ORBS", help="Symbol representing cash")
    p.add_argument("--output_csv", type=str, default="results/results.csv", help="Output results CSV path")
    p.add_argument("--summary", type=str, default="results/summary.txt", help="Summary report output path")
    p.add_argument("--risk_free", type=float, default=0.0, help="Annual risk-free rate for Sharpe")
    p.add_argument("--log", type=bool, default=False, help="Turn on logging")
    return p.parse_args()


def load_prices(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    if "timestamp" not in df.columns:
        raise ValueError("Prices CSV must contain a 'timestamp' column")

    allproducts = ["PHOENIX_FEATHERS", "UNICORN_HORNS", "ELVEN_WINE", "VAMPIRE_BLOOD"]

    # Pick out only CLOSE columns we care about
    close_cols = [f"CLOSE_{p}" for p in allproducts if f"CLOSE_{p}" in df.columns]

    if not close_cols:
        raise ValueError("Prices CSV must include CLOSE_ columns for products")

    # Rename CLOSE_P1 → P1, CLOSE_P2 → P2, etc.
    rename_map = {f"CLOSE_{p}": p for p in allproducts if f"CLOSE_{p}" in df.columns}
    df = df[["timestamp"] + close_cols].rename(columns=rename_map)
    # print(df)
    return df


def load_signals(path: str) -> pd.DataFrame:
    sdf = pd.read_csv(path)
    # support either a single 'signal' column or (timestamp, signal)
    if "signal" not in sdf.columns:
        raise ValueError("Signals CSV must contain a 'signal' column")
    # If there is a timestamp column, use it to align; else index assumes same order as prices
    sdf["signal"] = sdf["signal"].astype(str)
    return sdf


def write_summary(summary_path: str, metrics: dict) -> None:
    lines = [
        f"final_value: {metrics['final_value']:.6f}",
        f"total_trades: {metrics['total_trades']}",
        f"max_drawdown: {metrics['max_drawdown']:.3f}",
        f"volatility: {metrics['volatility']:.6f}",
        f"sharpe: {metrics['sharpe']:.6f}",
    ]
    Path(summary_path).write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


def main() -> None:
    args = parse_args()
    Path(args.output_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary).parent.mkdir(parents=True, exist_ok=True)


    prices = load_prices(args.prices)
    signals = load_signals(args.signals)

    # Align lengths: if signals shorter, reindex; if longer, truncate
    if len(signals) < len(prices):
        # forward fill last signal if desired; here we align by index and pad with ORBS
        signals = signals.reindex(range(len(prices))).fillna("ORBS")
    elif len(signals) > len(prices):
        signals = signals.iloc[: len(prices)]

    portfolio = Portfolio(initial_capital=args.initial_capital, transaction_cost=args.tx_cost, cash_symbol=args.cash_symbol,log = args.log)
    executor = TradeExecutor(portfolio=portfolio, price_df=prices, signal_df=signals, cash_symbol=args.cash_symbol)
    results = executor.run()

    # Save results CSV
    results.to_csv(args.output_csv, index=False)  
    evaluator = Evaluator(results["new_portfolio_value"]) 
    metrics = evaluator.summary(portfolio, risk_free_rate=args.risk_free)
    write_summary(args.summary, metrics)


if __name__ == "__main__":
    main()


