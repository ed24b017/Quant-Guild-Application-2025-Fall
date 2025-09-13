import argparse
from pathlib import Path
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Template strategy for generating signals from market data")
    p.add_argument("-i", "--input", required=True, help="Input CSV path (must include 'timestamp')")
    p.add_argument("-o", "--output", default="signals/signals.csv", help="Output CSV path for signals")
    return p.parse_args()

def generate_signals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Template strategy for 4 products: PHOENIX_FEATHERS, VAMPIRE_BLOOD, UNICORN_HORNS, and ELVEN_WINE.

    Input:
        df: DataFrame with columns
            - 'timestamp'
            - 'CLOSE_PHOENIX_FEATHERS'
            - 'CLOSE_VAMPIRE_BLOOD'
            - 'CLOSE_UNICORN_HORNS'
            - 'CLOSE_ELVEN_WINE'

    Output:
        DataFrame with columns
            - 'timestamp'
            - 'signal' (string: "PHOENIX_FEATHERS", "VAMPIRE_BLOOD", "UNICORN_HORNS", "ORBS", or "NIL")
    """

    # --- Input validation ---
    if "timestamp" not in df.columns:
        raise ValueError("Input CSV must contain a 'timestamp' column")

    required_cols = ["CLOSE_PHOENIX_FEATHERS", "CLOSE_VAMPIRE_BLOOD", "CLOSE_UNICORN_HORNS", "CLOSE_ELVEN_WINE"]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # --- Example logic (replace with your own rules) ---
    # Rules:
    # - If CLOSE_PHOENIX_FEATHERS rises vs previous row → buy PHOENIX_FEATHERS
    # - Else if CLOSE_VAMPIRE_BLOOD rises vs previous row → buy VAMPIRE_BLOOD
    # - Else → buy UNICORN_HORNS
    # - First row → NIL (no signal)

    signals = []
    for idx, row in df.iterrows():
        if idx == 0:
            signals.append("NIL")
            continue

        prev_row = df.iloc[idx - 1]
        sig = "ORBS"

        if row["CLOSE_PHOENIX_FEATHERS"] > 1.2 * prev_row["CLOSE_PHOENIX_FEATHERS"]:
            sig = "PHOENIX_FEATHERS"
        elif row["CLOSE_VAMPIRE_BLOOD"] > prev_row["CLOSE_VAMPIRE_BLOOD"]:
            sig = "VAMPIRE_BLOOD"
        else:
            sig = "UNICORN_HORNS"

        signals.append(sig)

    # --- Package into output DataFrame ---
    out = pd.DataFrame({
        "timestamp": df["timestamp"],
        "signal": signals,
    })
    return out


def main() -> None:
    args = parse_args()

    df = pd.read_csv(args.input)
    signals = generate_signals(df)

    # Ensure directory exists
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    signals.to_csv(out_path, index=False)
    print(f"Wrote signals to {out_path}")


if __name__ == "__main__":
    main()

