# Trading Simulation Backend

Simulate a multi-product (5 products) single-position portfolio with optional transaction costs.
please note, when a currency is purchased, it is bought/sold at the close price in a given timestamp.

## Install

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

## Input CSVs

- Prices CSV columns: `timestamp, product_1, product_2, product_3, product_4, product_5`
- Signals CSV columns: `signal`

Signals must be one of the product column names or `ORBS` (case-insensitive).

## Run

```bash
python backtest.py --prices path/to/input.csv --signals path/to/signals.csv 
```
## For getting trade logs 
```bash
python backtest.py --prices path/to/input.csv --signals path/to/signals.csv --log True 
```

## Output

- Results CSV: `timestamp, signal, holding, portfolio_value`
- Summary file with final value, total trades, max drawdown, volatility, Sharpe

## Forward Bias Test
```bash
python forward_bias.py --strategy path/to/template.py --prices path/to/input.csv
```

## Notes

- Portfolio is always fully in exactly one product or in ORBS.
- Transaction cost is a fixed fraction of traded notional.
- Final timestamp auto-liquidates any remaining holdings to base currency.


