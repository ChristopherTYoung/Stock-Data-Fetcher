import yfinance as yf

# Test 1: Basic download
print("Test 1: Basic download with yf.download")
df = yf.download("AAPL", interval="1m", start="2024-06-01", end="2024-06-07", progress=False)
print(f"Result: {len(df)} rows")
print(df.head() if not df.empty else "Empty DataFrame")
print()

# # Test 2: Ticker object
# print("Test 2: Using Ticker object")
# ticker = yf.Ticker("AAPL")
# hist = ticker.history(period="2y", interval="1h")
# print(f"Result: {len(hist)} rows")
# print(hist.head() if not hist.empty else "Empty DataFrame")
