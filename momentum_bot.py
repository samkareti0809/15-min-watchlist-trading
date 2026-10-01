import sys
import os
import time
import io
import sqlite3
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone, timedelta

# --- CONFIGURATION ---
DB_NAME = "momentum_cache.db"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

def init_database():
    """Initializes SQLite database with UPSERT-compatible primary keys."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS premarket_watchlist (
            ticker TEXT PRIMARY KEY,
            trigger_price REAL,
            stop_loss REAL,
            target_price REAL,
            cache_date TEXT
        )
    ''')
    conn.commit()
    conn.close()

def send_telegram_alert(message):
    """Sends summary or breakout notifications to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"\n[Telegram Simulation Alert]:\n{message}\n")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        response = requests.post(url, json=payload, timeout=5)
        if response.status_code != 200:
            print(f"❌ Telegram Error: {response.text}")
    except Exception as e:
        print(f"❌ Connection error sending Telegram alert: {e}")

def get_full_nse_universe():
    """Dynamically fetches all active NSE equities from official CSV."""
    url = "https://archives.nseindia.com/content/equities/EQUITY_L.csv"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        res = requests.get(url, headers=headers, timeout=10)
        df = pd.read_csv(io.BytesIO(res.content))
        return [str(sym).strip() + ".NS" for sym in df['SYMBOL'].tolist()]
    except Exception as e:
        print(f"⚠️ Error fetching NSE master list: {e}. Using fallback universe.")
        return ["CUPID.NS", "SCHNEIDER.NS", "MAZDOCK.NS", "COCHINSHIP.NS", "HAL.NS", "BEL.NS", "TITAN.NS"]

def get_batch_universe(batch_num):
    """Splits the full NSE universe into 10 equal 10% batches."""
    full_universe = get_full_nse_universe()
    total_stocks = len(full_universe)
    batch_size = total_stocks // 10
    
    start_idx = (batch_num - 1) * batch_size
    # Ensure the 10th batch captures any remainder stocks
    end_idx = total_stocks if batch_num == 10 else batch_num * batch_size
    
    batch_list = full_universe[start_idx:end_idx]
    print(f"📦 Batch {batch_num}/10 Loaded: Stocks index {start_idx} to {end_idx} (Total: {len(batch_list)})")
    return batch_list

# =========================================================================
# BATCHED SCREENER & CACHER
# =========================================================================
def run_batch_screener(batch_num):
    init_database()
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    
    today_str = datetime.now().strftime("%Y-%m-%d")
    universe = get_batch_universe(batch_num)
    
    print(f"🔍 Running Batch {batch_num} Screener for {today_str}...")
    qualified_count = 0
    
    for ticker in universe:
        success = False
        attempts = 0
        while not success and attempts < 2:
            try:
                attempts += 1
                df = yf.download(ticker, interval="1d", period="6mo", auto_adjust=True, progress=False)
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
                    
                df = df.dropna().copy()
                if len(df) < 50:
                    success = True
                    continue
                    
                # Daily Indicators
                df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
                df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()
                df['Vol_MA_20'] = df['Volume'].rolling(window=20).mean()
                
                last_close = df['Close'].iloc[-1]
                ema_50 = df['EMA_50'].iloc[-1]
                ema_200 = df['EMA_200'].iloc[-1]
                
                # Trend & Breakout Logic
                trend_ok = (last_close > ema_50) and (last_close > ema_200)
                high_20 = df['High'].iloc[-20:-1].max()
                is_near_high = last_close >= (high_20 * 0.98)
                recent_vol_spike = (df['Volume'].iloc[-1] > df['Vol_MA_20'].iloc[-1] * 1.2) or \
                                   (df['Volume'].iloc[-2] > df['Vol_MA_20'].iloc[-2] * 1.2)
                                   
                if trend_ok and is_near_high and recent_vol_spike:
                    trigger = round(high_20, 2)
                    sl = round(trigger * 0.975, 2)
                    tp = round(trigger * 1.06, 2)
                    
                    # UPSERT: Safe concurrent writes across multiple batches
                    cursor.execute('''
                        INSERT OR REPLACE INTO premarket_watchlist (ticker, trigger_price, stop_loss, target_price, cache_date)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (ticker, trigger, sl, tp, today_str))
                    conn.commit()
                    qualified_count += 1
                    print(f"✅ Found Setup: {ticker} (Trigger: {trigger})")
                
                success = True
            except Exception as e:
                time.sleep(1) # Brief cooldown on error before retry
                
        # Politeness delay to prevent Yahoo Finance rate-limiting
        time.sleep(0.5)

    conn.close()
    print(f"Batch {batch_num} complete. Found {qualified_count} setups added/updated in database.")

# =========================================================================
# 15-MINUTE INTRADAY MONITOR
# =========================================================================
def run_monitor():
    init_database()
    conn = sqlite3.connect(DB_NAME)
    try:
        watchlist_df = pd.read_sql_query("SELECT * FROM premarket_watchlist", conn)
    except Exception as e:
        print(f"⚠️ Database error or missing table: {e}")
        watchlist_df = pd.DataFrame()
    finally:
        conn.close()
    
    if watchlist_df.empty:
        print("⚠️ Watchlist cache is empty. No stocks to monitor.")
        return

    print(f"⚡ Checking {len(watchlist_df)} cached stocks on 15m live data...")
    
    for _, row in watchlist_df.iterrows():
        ticker = row['ticker']
        trigger_price = row['trigger_price']
        
        try:
            df = yf.download(ticker, interval="15m", period="1d", auto_adjust=True, progress=False)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
                
            if df.empty:
                continue
                
            latest_close = df['Close'].iloc[-1]
            latest_high = df['High'].iloc[-1]
            
            if latest_high >= trigger_price:
                alert_msg = (
                    f"🚨 *15M BREAKOUT TRIGGERED!* 🚀\n\n"
                    f"📈 *Stock:* `{ticker}`\n"
                    f"⚡ *Trigger Level:* INR {trigger_price}\n"
                    f"🔴 *Stop-Loss:* INR {row['stop_loss']}\n"
                    f"🎯 *Target:* INR {row['target_price']}\n"
                    f"📊 *Current Price:* INR {latest_close:.2f}"
                )
                send_telegram_alert(alert_msg)
                print(f"✅ Alert sent for {ticker}!")
            else:
                print(f"⏳ {ticker}: Monitoring... (High: INR {latest_high} | Trigger: INR {trigger_price})")
            
            time.sleep(0.3) # Rate limit protection during intraday checks
        except Exception as e:
            continue

# =========================================================================
# ENTRY POINT
# =========================================================================
if __name__ == "__main__":
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.isdigit():
            # Run specific batch (1 through 10) passed via CLI
            run_batch_screener(int(arg))
        elif arg == "monitor":
            run_monitor()
        else:
            print("Unknown argument. Use a batch number (1-10) or 'monitor'.")
    else:
        # Fallback auto-detection for monitor vs evening batches if needed
        IST = timezone(timedelta(hours=5, minutes=30))
        now_ist = datetime.now(IST)
        if 9 <= now_ist.hour < 16:
            run_monitor()
        else:
            print("Please specify a batch number (1 to 10) to run an EOD batch scan.")
