# This is file where all the testing and other stuff happens.


import numpy as np
import pandas as pd


master_df = pd.read_csv("/Users/aarya/Kaarthi/Code/Quant-Guild-Application-2025-Fall/data/input.csv")





# ### Momentum: 
# 
# Okay, first we will be dealing with the momentum strat.
# For this strat, first, we need to take in the first few rows to be not trading, or the lookback period. So, for the lookback period, there will be no trading to avoid losses. 
# a entry in the time stamp indicates the end of day. so any trade must be done for the end of day price only. 
# create a new df with 4 cols and 90% of rows consisting of % returns for each stock. then, find the maximum among the 4 stocks. add another col indicating whether we have money or stock in the account at that prev time stamp eod. 
# 
# for ex, if the previous day, we have sold all the stock, the prev stock should be hold and the present stock is money.
# so, to take a decision, we look at the present day's status, and after taking the decision, we update the next day's status, since we cannot buy and sell at the same time. 
# 
# So, we cannot buy and sell at the same timestamp.
# 
# create a new df with 6 cols : ts, s1, s2, s3, s4, status. 
# create a new ddf 2 cols : ts, trading_sig
# this will be the final dataset to output. 
# 
# 


df = master_df



selected_cols = ["timestamp", "CLOSE_UNICORN_HORNS", "CLOSE_ELVEN_WINE", "CLOSE_VAMPIRE_BLOOD", "CLOSE_PHOENIX_FEATHERS"]
df_sel = df[selected_cols]
df_sel.columns = ["timestamp", "UNICORN_HORNS", "ELVEN_WINE", "VAMPIRE_BLOOD", "PHOENIX_FEATHERS"]


# here, lets define the lookback. 

results_df = []

for lookback in range(10, 750, 10):
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
    fdf[['timestamp', 'signal']].to_csv(f'data/signals{lookback}.csv', index=False)
    
    
    




