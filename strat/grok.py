import pandas as pd
import numpy as np

# Load dataset
df = pd.read_csv('data/input.csv')

# Select CLOSE columns for momentum calculation
close_cols = [col for col in df.columns if col.startswith('CLOSE_')]
products = [col.replace('CLOSE_', '') for col in close_cols]

# Calculate 500-period percentage returns
lookback = 500
df_rets = df.iloc[lookback:].copy()  # t=501 to 5000
for col in close_cols:
    df_rets[f'Return_{col}'] = (df_rets[col] - df_rets[col].shift(lookback)) / df_rets[col].shift(lookback) * 100

# Initialize portfolio
orbs = 1000  # Starting ORBS
current_product = None  # Held product
holdings = 0  # Quantity of product
signals = []  # Store signals
return_threshold = 2  # Minimum return to buy (%)

# Process t=1 to 500 (no trading, insufficient data)
for t in range(500):
    signals.append({'timestamp': df['timestamp'].iloc[t], 'signal': 'NIL'})

# Process t=501 to 5000
for t in df_rets.index:
    timestamp = df_rets['timestamp'].iloc[t - 500]  # Adjust for iloc[500:]
    
    # Get max return and product
    return_cols = [f'Return_{col}' for col in close_cols]
    max_return = df_rets[return_cols].iloc[t - 500].fillna(-1000).max()
    max_product_col = df_rets[return_cols].iloc[t - 500].fillna(-1000).idxmax()
    max_product = max_product_col.replace('Return_CLOSE_', '') if pd.notna(max_product_col) else 'NIL'

    if t == df_rets.index[-1] and current_product:
        # Final timestamp: sell to ORBS
        sell_price = df_rets[f'CLOSE_{current_product}'].iloc[t - 500]
        orbs += holdings * sell_price
        signals.append({'timestamp': timestamp, 'signal': 'ORBS'})
        current_product = None
        holdings = 0
    elif current_product:
        # Check if current product's return is still #1
        current_return = df_rets[f'Return_CLOSE_{current_product}'].iloc[t - 500]
        if pd.isna(current_return):
            current_return = -1000
        if current_return < max_return:
            # Sell current product, buy max return product if above threshold
            sell_price = df_rets[f'CLOSE_{current_product}'].iloc[t - 500]
            orbs += holdings * sell_price
            current_product = None
            holdings = 0
            if max_return > return_threshold:
                buy_price = df_rets[f'CLOSE_{max_product}'].iloc[t - 500]
                holdings = orbs / buy_price
                orbs = 0
                current_product = max_product
                signals.append({'timestamp': timestamp, 'signal': max_product})
            else:
                signals.append({'timestamp': timestamp, 'signal': 'ORBS'})
        else:
            # Hold current product
            signals.append({'timestamp': timestamp, 'signal': 'NIL'})
    else:
        # Holding ORBS: buy max return product if above threshold
        if max_return > return_threshold:
            buy_price = df_rets[f'CLOSE_{max_product}'].iloc[t - 500]
            holdings = orbs / buy_price
            orbs = 0
            current_product = max_product
            signals.append({'timestamp': timestamp, 'signal': max_product})
        else:
            signals.append({'timestamp': timestamp, 'signal': 'NIL'})

# Create signals DataFrame
signals_df = pd.DataFrame(signals)

# Ensure all timestamps are included
signals_df = signals_df.set_index('timestamp').reindex(df['timestamp']).reset_index()
signals_df['signal'] = signals_df['signal'].fillna('NIL')

# Save to signals.csv
signals_df[['timestamp', 'signal']].to_csv('signals.csv', index=False)