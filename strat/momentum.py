import argparse
from pathlib import Path
import pandas as pd
import time
import numpy as np


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
    
    st = time.time()
    args = parse_args()

    df = pd.read_csv(args.input)
    selected_cols = ["timestamp", "CLOSE_UNICORN_HORNS", "CLOSE_ELVEN_WINE", "CLOSE_VAMPIRE_BLOOD", "CLOSE_PHOENIX_FEATHERS"]
    
    df_sel = df[selected_cols]
    df_sel.columns = ["timestamp", "UNICORN_HORNS", "ELVEN_WINE", "VAMPIRE_BLOOD", "PHOENIX_FEATHERS"]

    close_cols = df_sel.columns[1:]
    
    lookbacks = range(int(len(df)/50), int(len(df)/10), int(len(df)/40))
    
    returns_dict = {}  
    
    for lookback in lookbacks:
        
        df_rets = pd.DataFrame(index=df_sel.index)
        
        for col in close_cols:
            
            df_rets[f'{col}'] = ((df_sel[col] - df_sel[col].shift(lookback)) / df_sel[col].shift(lookback)) * 100
            
        df_rets["max_ret"] = df_rets[close_cols].max(axis=1)
        
        df_rets["Symbol_for_max_ret"] = df_rets[close_cols].idxmax(axis=1)
        
        returns_dict[lookback] = df_rets




    ffdf = pd.DataFrame()
    maxi = 0
    return_threshold = 1.75
    total_points = len(df_sel)



    for lookback in lookbacks:
        
        df_rets = returns_dict[lookback]
        
        signals = np.full(total_points, "NIL", dtype=object)
        
        orbs = 1000.0
        
        holdings = 0.0
        
        current_prod = "None"

        signals[:lookback] = "NIL"

        for t in range(lookback, total_points):
            
            df_max_ret = df_rets.at[t, 'max_ret']
            
            df_sym_for_max_ret = df_rets.at[t, 'Symbol_for_max_ret']
            
            if current_prod != "None" :
                df_ret_curr_prod = df_rets.at[t, current_prod]
            else :
                df_ret_curr_prod = 0
            
            signal = ''
            
            if orbs == 0:
                
                if current_prod != df_sym_for_max_ret and current_prod != "None":
                    
                    signal = "ORBS"
                    orbs = holdings * df_sel.at[t, current_prod]
                    holdings = 0.0
                    current_prod = "None"
                    
                elif current_prod == df_sym_for_max_ret and df_ret_curr_prod < return_threshold:
                    
                    signal = "ORBS"
                    orbs = holdings * df_sel.at[t, current_prod]
                    holdings = 0.0
                    current_prod = "None"
                    
                elif current_prod == df_sym_for_max_ret and df_ret_curr_prod >= return_threshold:
                    signal = "NIL"
                    current_prod = current_prod
                    
            else:
                
                if df_max_ret >= return_threshold:
                    
                    signal = df_sym_for_max_ret
                    holdings = orbs / df_sel.at[t, signal]
                    orbs = 0.0
                    current_prod = df_sym_for_max_ret
                    
                else:
                    signal = "NIL"
            
            signals[t] = signal
        
        fdf = pd.DataFrame({
            "timestamp": df_sel['timestamp'],
            "signal": signals
        })
        
        ans = orbs
        
        if current_prod != "None":
            ans += holdings * df_sel.at[total_points-1, current_prod]
        
        if ans > maxi:
            maxi = ans
            ffdf = fdf
            

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    ffdf[['timestamp', 'signal']].to_csv(out_path, index=False)
    print(f"Wrote signals to {out_path}")
    
    et = time.time()
    tt = et - st
    print("total time : ", tt, "s")

if __name__ == "__main__":
    main()

