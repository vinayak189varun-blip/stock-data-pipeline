import yfinance as yf
import pandas as pd
from datetime import datetime

# Top Indian stocks (Nifty 50 major list + Shriram Finance)
stocks = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "LT.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS", "SUNPHARMA.NS",
    "TITAN.NS", "ULTRACEMCO.NS", "NESTLEIND.NS", "WIPRO.NS", "HCLTECH.NS",
    "BAJFINANCE.NS", "BAJAJFINSV.NS", "POWERGRID.NS", "NTPC.NS", "ONGC.NS",
    "JSWSTEEL.NS", "TATASTEEL.NS", "INDUSINDBK.NS", "ADANIENT.NS", "ADANIPORTS.NS",
    "GRASIM.NS", "COALINDIA.NS", "DRREDDY.NS", "EICHERMOT.NS", "HEROMOTOCO.NS",
    "CIPLA.NS", "BRITANNIA.NS", "APOLLOHOSP.NS", "DIVISLAB.NS", "SBILIFE.NS",
    "HDFCLIFE.NS", "TECHM.NS", "UPL.NS", "BAJAJ-AUTO.NS", "TATAMOTORS.NS",
    "SHRIRAMFIN.NS"  # explicitly added
]

all_data = []

for stock in stocks:
    print(f"Fetching data for {stock}")
    
    df = yf.download(stock, period="5d", interval="1d")
    
    if df.empty:
        print(f"No data for {stock}")
        continue

    df.reset_index(inplace=True)
    
    df['stock'] = stock
    df['load_time'] = datetime.now()
    
    all_data.append(df)

# Combine all stocks
final_df = pd.concat(all_data)

# Select useful columns
final_df = final_df[
    ['Date', 'stock', 'Open', 'High', 'Low', 'Close', 'Volume', 'load_time']
]

# Save output with date partition
today = datetime.now().strftime("%Y-%m-%d")
file_name = f"output/stock_data_{today}.csv"

final_df.to_csv(file_name, index=False)

print(f"Pipeline completed. File saved: {file_name}")
