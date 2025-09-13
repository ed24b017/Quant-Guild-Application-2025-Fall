from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


@dataclass
class Trade:
    timestamp: int 
    from_asset: str
    to_asset: str
    price_from: Optional[float]
    price_to: Optional[float]
    size_base_ccy: float
    transaction_cost: float


class Portfolio:
    """
    Manages a single-asset-or-cash portfolio with transaction costs.
    """

    def __init__(
        self,
        initial_capital: float,
        transaction_cost: float = 0.0,
        cash_symbol: str = "CASH",
        log: bool = False,
    ) -> None:
        self.cash_symbol = cash_symbol
        self.transaction_cost = float(transaction_cost)
        self.base_ccy_cash: float = float(initial_capital)
        self.holding_symbol: str = cash_symbol
        self.holding_units: float = 0.0  # units of current product
        self.num_trades: int = 0
        self.trade_log: List[Trade] = []
        self.log = log

    def _apply_transaction_cost(self, notional: float) -> float:
        fee = abs(notional) * self.transaction_cost
        return fee

    def _liquidate_current(self, timestamp: int, price: Optional[float]) -> Tuple[float, float]:
        if self.holding_symbol == self.cash_symbol:
            return 0.0, 0.0
        if price is None or not np.isfinite(price):
            raise ValueError("Missing price to liquidate current holding")
        notional = self.holding_units * price
        fee = self._apply_transaction_cost(notional)
        self.base_ccy_cash += notional - fee
        sold_symbol = self.holding_symbol
        self.holding_symbol = self.cash_symbol
        self.holding_units = 0.0
        self.num_trades += 1
        self.trade_log.append(
            Trade(
                timestamp=timestamp,
                from_asset=sold_symbol,
                to_asset=self.cash_symbol,
                price_from=price,
                price_to=None,
                size_base_ccy=notional,
                transaction_cost=fee,
            )
        )
        if self.log and self.trade_log:
                last_trade = self.trade_log[-1]
                print(
                    f"[{last_trade.timestamp}] {last_trade.from_asset} → {last_trade.to_asset} | "
                    f"Size(base): {last_trade.size_base_ccy:.2f} | "
                    f"Price_from: {last_trade.price_from} | Price_to: {last_trade.price_to} | "
                    f"Fee: {last_trade.transaction_cost:.2f}"
                )

        return notional, fee

    def _buy_new(self, timestamp: int, symbol: str, price: Optional[float]) -> Tuple[float, float]:
        if symbol == self.cash_symbol:
            return 0.0, 0.0
        if price is None or not np.isfinite(price):
            raise ValueError("Missing price to buy new holding")
        notional = self.base_ccy_cash
        if notional <= 0.0:
            self.holding_symbol = self.cash_symbol
            self.holding_units = 0.0
            return 0.0, 0.0
        fee = self._apply_transaction_cost(notional)
        investable = max(notional - fee, 0.0)
        units = investable / price if price > 0 else 0.0
        self.base_ccy_cash -= notional  # spend all cash
        self.holding_symbol = symbol
        self.holding_units = units
        self.num_trades += 1
        self.trade_log.append(
            Trade(
                timestamp=timestamp,
                from_asset=self.cash_symbol,
                to_asset=symbol,
                price_from=None,
                price_to=price,
                size_base_ccy=notional,
                transaction_cost=fee,
            )
        )

        if self.log and self.trade_log:
                last_trade = self.trade_log[-1]
                print(
                    f"[{last_trade.timestamp}] {last_trade.from_asset} → {last_trade.to_asset} | "
                    f"Size(base): {last_trade.size_base_ccy:.2f} | "
                    f"Price_from: {last_trade.price_from} | Price_to: {last_trade.price_to} | "
                    f"Fee: {last_trade.transaction_cost:.2f}"
                )
        return notional, fee

    def rebalance(self, timestamp: int, target_symbol: str, price_map: Dict[str, Optional[float]]) -> None:

        if target_symbol is None:
            return;
            
        target_symbol = target_symbol.upper()
        if target_symbol == "NIL" or target_symbol == "NAN":
               return 
        if target_symbol not in price_map and target_symbol != self.cash_symbol:
            raise ValueError(f"Price for target symbol {target_symbol} not provided")

        if self.holding_symbol == target_symbol:
            return

        if self.holding_symbol != self.cash_symbol:
            current_price = price_map.get(self.holding_symbol)
            self._liquidate_current(timestamp, current_price)

        if target_symbol != self.cash_symbol:
            buy_price = price_map.get(target_symbol)
            self._buy_new(timestamp, target_symbol, buy_price)

    def value(self, price_map: Dict[str, Optional[float]]) -> float:
        if self.holding_symbol == self.cash_symbol:
            return float(self.base_ccy_cash)
        price = price_map.get(self.holding_symbol)
        if price is None or not np.isfinite(price):
            raise ValueError(f"Missing price for valuation of {self.holding_symbol}")
        return float(self.base_ccy_cash + self.holding_units * price)

    def liquidate_all(self, timestamp: int, price_map: Dict[str, Optional[float]]) -> None:
        if self.holding_symbol == self.cash_symbol:
            return
        price = price_map.get(self.holding_symbol)
        self._liquidate_current(timestamp, price)


class TradeExecutor:
    """Executes trades based on signals and a price dataframe."""

    def __init__(
        self,
        portfolio: Portfolio,
        price_df: pd.DataFrame,
        signal_df: pd.DataFrame,
        cash_symbol: str = "CASH",
    ) -> None:
        self.portfolio = portfolio
        self.price_df = price_df
        self.cash_symbol = cash_symbol

        # normalize signals: dict {timestamp: signal}
        self.signal_map = dict(
            zip(signal_df["timestamp"].astype(int), signal_df["signal"].astype(str).str.upper())
        )

        # product columns (assumes all non-timestamp columns are products)
        self.product_cols = [c for c in price_df.columns if c != "timestamp"]
        #print(self.product_cols)

    def run(self) -> pd.DataFrame:
        records: List[Dict[str, object]] = []

        for _, row in self.price_df.iterrows():
            ts = int(row["timestamp"])
            signal = self.signal_map.get(ts, None)


            price_map: Dict[str, Optional[float]] = {c: row[c] for c in self.product_cols}
            prev_holding_symbol = self.portfolio.holding_symbol
            self.portfolio.rebalance(ts, signal, price_map)
            pv = self.portfolio.value(price_map)

            records.append(
                {
                    "timestamp": ts,
                    "signal": signal,
                    "holding": prev_holding_symbol,
                    "new_portfolio_value": pv,
                }
            )

        # Final liquidation
        last_row = self.price_df.iloc[-1]
        last_ts = int(last_row["timestamp"])
        last_price_map: Dict[str, Optional[float]] = {c: last_row[c] for c in self.product_cols}
        self.portfolio.liquidate_all(last_ts, last_price_map)

        final_value = self.portfolio.value(last_price_map)
        if records:
            records[-1]["holding"] = self.portfolio.holding_symbol
            records[-1]["portfolio_value"] = final_value

        return pd.DataFrame.from_records(records)


class Evaluator:
    """Computes performance metrics from a portfolio value time series."""

    def __init__(self, value_series: pd.Series, freq_per_year: Optional[int] = None) -> None:
        self.value_series = value_series.astype(float)
        self.returns = self.value_series.pct_change().fillna(0.0)
        self.freq_per_year = freq_per_year or self._infer_freq_per_year()

    def _infer_freq_per_year(self) -> int:
        return 252

    def final_value(self) -> float:
        return float(self.value_series.iloc[-1])

    def total_trades(self, portfolio: Portfolio) -> int:
        return int(portfolio.num_trades)

    def max_drawdown(self) -> float:
        cum_max = self.value_series.cummax()
        drawdown = (self.value_series - cum_max) / cum_max
        return float(drawdown.min() * 100)  # in percentage

    def volatility(self) -> float:
        vol = self.returns.std(ddof=0) * np.sqrt(self.freq_per_year)
        return float(vol)

    def sharpe(self, risk_free_rate: float = 0.0) -> float:
        rf_period = (1 + risk_free_rate) ** (1 / self.freq_per_year) - 1
        excess = self.returns - rf_period
        mean_excess = excess.mean()
        std_excess = excess.std(ddof=0)
        if std_excess == 0:
            return 0.0
        return float((mean_excess / std_excess) * np.sqrt(self.freq_per_year))

    def summary(self, portfolio: Portfolio, risk_free_rate: float = 0.0) -> Dict[str, float]:
        return {
            "final_value": self.final_value(),
            "total_trades": self.total_trades(portfolio),
            "max_drawdown": self.max_drawdown(),
            "volatility": self.volatility(),
            "sharpe": self.sharpe(risk_free_rate=risk_free_rate),
        }
