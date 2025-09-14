import os 
import subprocess
import pandas as pd
import time

start_time = time.time()
subprocess.run(["python3" ,"strat/testing.py"], capture_output=True)

df = []

for t in range(10, 750, 10):
    result = subprocess.run(["python3", "backtest.py", "--prices" ,"data/input.csv", "--signals", f"data/signals{t}.csv"], capture_output=True)    
    with open("results/summary.txt", "r") as f:
        lines = f.readlines()
        df.append([t] + [float(line.split(":")[1].strip()) for line in lines])

    columns = ["t", "final_value", "total_trades", "max_drawdown ( in %)", "volatility", "sharpe"]
    summary_df = pd.DataFrame(df, columns=columns)

end_time = time.time()
total_time = round(end_time - start_time, 5)


print(summary_df.to_markdown(index=False))
max_sharpe = summary_df['sharpe'].max()
max_total = summary_df['final_value'].max()

print("Max Sharpe Ratio is :" , max_sharpe)
print("Total final_value is :", max_total)
print("Total time is", total_time, "s")

