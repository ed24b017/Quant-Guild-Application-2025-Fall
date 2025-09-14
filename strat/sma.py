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

        prev_row = df.iloc[idx-1]
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
    
    selected_cols = ["timestamp", "CLOSE_UNICORN_HORNS", "CLOSE_ELVEN_WINE", "CLOSE_VAMPIRE_BLOOD", "CLOSE_PHOENIX_FEATHERS"]
    df_sel = df[selected_cols]
    df_sel.columns = ["timestamp", "UNICORN_HORNS", "ELVEN_WINE", "VAMPIRE_BLOOD", "PHOENIX_FEATHERS"]


    # sma. 

    final_cols = [col for col in df_sel.columns if col != "timestamp"]

    df_sma = df_sel.copy()
    small = 50
    large = 200
    for col in final_cols : 
        df_sma[f"{small}_{col}"] = df_sma[col].rolling(window=small).mean()
        df_sma[f"{large}_{col}"] = df_sma[col].rolling(window=large).mean()

    # we could write a small strategy based on sma and find out if it works or not. 
    # lets calculate the slope and generate trading signals. 

    lookback = 25
    for col in final_cols:
        df_sma[f"{col}"] = ((df_sma[f'50_{col}'] - df_sma[f'50_{col}'].shift(lookback))/df_sma[f'50_{col}'].shift(lookback)) * 100
        
    # once we have found out the slope of this, let's go and find out the maximum slope. 
    df_sma['max_slope'] = df_sma[[f"{col}" for col in final_cols]].max(axis=1)
    df_sma['sym_max_slope'] = df_sma[[f"{col}" for col in final_cols]].idxmax(axis=1)

    # alright, now the dataset is ready, it's time to put it to the test. 
    # initialization variables. 

    orbs = 1000
    current_prod = "None"
    holdings = 0
    signal = ''

    ans = []

    for t in range(lookback + small):
        ans.append({"timestamp": t, "signal" : "NIL"})

    for t in range(lookback + small, 5000):
        
        
        t_prod = df_sma.at[t, 'sym_max_slope']
        t_price = df_sel.at[t, t_prod]
        
        if orbs == 0:
            # we already have a prod at hand. 
            # we could sell it or hold it.
            
            if (t_prod != current_prod) : 
                # we sell it. 
                orbs = df_sel.at[t, current_prod] * holdings
                holdings = 0
                current_prod = "None"
                signal = "ORBS";
            else : signal = "NIL"
        
        else : 
            signal = t_prod
            holdings = orbs/t_price
            orbs = 0;
            current_prod = t_prod
        
        ans.append({"timestamp" : t, "signal" : signal})

    ans = pd.DataFrame(ans)
    
    # Ensure directory exists
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ans[['timestamp', 'signal']].to_csv(out_path, index=False)
    print(f"Wrote signals to {out_path}")


if __name__ == "__main__":
    main()

