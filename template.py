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


    # here, lets define the lookback. 
    lookback = 300

    results_df = []

    close_cols = df_sel.columns[1:]
    df_rets = pd.DataFrame()
    for col in close_cols:
        df_rets[f'{col}'] = ((df_sel[col] - df_sel[col].shift(lookback)) / df_sel[col].shift(lookback)) * 100



    df_rets["max_ret"] = df_rets[1:].max(axis=1)
    ret_cols = close_cols
    df_rets["Symbol_for_max_ret"] = df_rets[ret_cols].idxmax(axis=1)


    orbs = 1000
    holdings = 0
    signals = []
    total_points = len(df)
    products = [col.replace('CLOSE_', '') for col in close_cols]
    return_threshold = 1.5 # this should be changed to different values.

    # for the lookback period, there will be no trading data. 

    for t in range(lookback):
        signals.append({"timestamp" : df_sel['timestamp'].iloc[t], 'signal' : "NIL"})


    current_prod = "None"

    for t in range(lookback+1, total_points+1):
        
        timestamp = t
        
        df_max_ret = df_rets.at[t-1, 'max_ret']    
        df_sym_for_max_ret = df_rets.at[t-1, 'Symbol_for_max_ret']
        
        signal = ''
        
        # all of them set. now logic. 
        
        # for the first iteration, we are not applying the minimum ratio concept here, 
        # where the % ratio must be greater than a specified value. 
        
        if (orbs == 0):
            # here we have some product in the portfolio already. 
            if (current_prod != df_sym_for_max_ret and current_prod != "None"):
                # here, the current product is gone from the top leaderboard, 
                # therefore, we sell the current product.   
                signal = "ORBS"
                orbs = holdings * (df_sel.at[t-1, current_prod])   
                current_prod = "None"
                        
            elif (current_prod == df_sym_for_max_ret):
                current_prod = current_prod
                signal = "NIL"
                
        else :
            
            signal = df_sym_for_max_ret
            holdings = orbs/df_sel.at[t-1, signal]
            orbs = 0
            current_prod = df_sym_for_max_ret
        
        signals.append({"timestamp" : t, 'signal' : signal})

        fdf = pd.DataFrame(signals)

        

    # Ensure directory exists
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fdf[['timestamp', 'signal']].to_csv(out_path, index=False)
    print(f"Wrote signals to {out_path}")


if __name__ == "__main__":
    main()

