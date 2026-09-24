
"""
Pipeline: Crawl giá CK VN → Supabase → Tính PnL
Chạy tự động hàng ngày qua GitHub Actions
"""
import os
import time
from datetime import date, datetime
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ============ 1. CẤU HÌNH ============
MY_PORTFOLIO = ['VCB','ACB','FPT','HPG','MWG','VNM','MSN','GAS','VIC','SSI']

INDUSTRY_MAP = {
    'VCB': ('Ngân hàng TMCP Ngoại thương Việt Nam', 'Ngân hàng'),
    'ACB': ('Ngân hàng TMCP Á Châu',                 'Ngân hàng'),
    'FPT': ('Công ty Cổ phần FPT',                    'Công nghệ thông tin'),
    'HPG': ('Công ty Cổ phần Tập đoàn Hòa Phát',      'Thép - Vật liệu'),
    'MWG': ('Công ty Cổ phần Đầu tư Thế Giới Di Động','Bán lẻ'),
    'VNM': ('Công ty Cổ phần Sữa Việt Nam',           'Thực phẩm - Đồ uống'),
    'MSN': ('Công ty Cổ phần Tập đoàn Masan',         'Thực phẩm - Đồ uống'),
    'GAS': ('Tổng Công ty Khí Việt Nam',              'Dầu khí'),
    'VIC': ('Tập đoàn Vingroup',                      'Bất động sản'),
    'SSI': ('Công ty Cổ phần Chứng khoán SSI',        'Chứng khoán'),
}

DB_HOST     = os.environ['DB_HOST']
DB_PORT     = os.environ['DB_PORT']
DB_USER     = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME     = os.environ['DB_NAME']

# ============ 2. KẾT NỐI DB ============
safe_pwd = quote_plus(DB_PASSWORD)
conn_str = f"postgresql+psycopg2://{DB_USER}:{safe_pwd}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
engine = create_engine(conn_str)

# ============ 3. CRAWL GIÁ ============
from vnstock import Quote

def fetch_daily_price(symbol, start='2024-01-01', end=None):
    end = end or date.today().isoformat()
    for src in ['KBS', 'VCI', 'TCBS']:
        try:
            q = Quote(symbol=symbol, source=src)
            df = q.history(start=start, end=end, interval='1D')
            if df is not None and not df.empty:
                df = df.rename(columns={'time': 'trade_date'})
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                df['symbol'] = symbol
                # Đổi đơn vị nghìn đồng -> đồng
                for col in ['open','high','low','close']:
                    df[col] = df[col] * 1000
                return df[['symbol','trade_date','open','high','low','close','volume']]
        except Exception as e:
            print(f"  ⚠️ {symbol} | {src}: {e}")
            time.sleep(0.5)
    return None

def upsert_prices(df):
    if df is None or df.empty:
        return 0
    rows = df.to_dict('records')
    with engine.begin() as conn:
        for r in rows:
            conn.execute(text("""
                INSERT INTO fact_daily_price
                    (symbol, trade_date, open, high, low, close, volume)
                VALUES (:symbol, :trade_date, :open, :high, :low, :close, :volume)
                ON CONFLICT (symbol, trade_date) DO UPDATE SET
                    open=EXCLUDED.open, high=EXCLUDED.high,
                    low=EXCLUDED.low, close=EXCLUDED.close,
                    volume=EXCLUDED.volume;
            """), r)
    return len(rows)

# ============ 4. UPSERT dim_stock ============
def upsert_dim_stock():
    with engine.begin() as conn:
        for sym, (name, ind) in INDUSTRY_MAP.items():
            conn.execute(text("""
                INSERT INTO dim_stock (symbol, organ_name, exchange, industry, updated_at)
                VALUES (:sym, :name, 'HOSE', :ind, NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    organ_name = EXCLUDED.organ_name,
                    industry   = EXCLUDED.industry,
                    updated_at = NOW();
            """), {"sym": sym, "name": name, "ind": ind})

# ============ 5. TÍNH PNL ============
def compute_pnl():
    tx = pd.read_sql("""
        SELECT * FROM fact_portfolio_transactions
        ORDER BY trade_date, id
    """, engine)

    price_df = pd.read_sql("""
        SELECT DISTINCT ON (symbol) symbol, trade_date, close
        FROM fact_daily_price
        ORDER BY symbol, trade_date DESC;
    """, engine)
    latest_price = dict(zip(price_df['symbol'], price_df['close']))

    state = {}
    for _, r in tx.iterrows():
        s = r['symbol']
        st = state.setdefault(s, {'qty': 0, 'cost': 0.0, 'realized': 0.0})
        if r['action'] == 'BUY':
            st['cost'] += r['quantity'] * r['price'] + r['fee']
            st['qty']  += r['quantity']
        else:
            if st['qty'] == 0: continue
            avg = st['cost'] / st['qty']
            st['realized'] += (r['price'] - avg) * r['quantity'] - r['fee']
            st['qty']  -= r['quantity']
            st['cost'] -= avg * r['quantity']

    today = date.today()
    out = []
    for s, st in state.items():
        if st['qty'] <= 0: continue
        avg_cost = st['cost'] / st['qty']
        close    = latest_price.get(s, 0)
        mv       = close * st['qty']
        out.append({
            'trade_date': today, 'symbol': s, 'quantity': st['qty'],
            'avg_cost': round(avg_cost, 2), 'close_price': close,
            'unrealized_pnl': round((close-avg_cost)*st['qty'], 0),
            'realized_pnl': round(st['realized'], 0),
            'market_value': round(mv, 0), 'nav': round(mv, 0),
        })
    return pd.DataFrame(out)

# ============ 6. MAIN ============
def run():
    print(f"🚀 Pipeline start: {datetime.now()}")
    upsert_dim_stock()
    print("  ✅ dim_stock")
    total = 0
    for s in MY_PORTFOLIO:
        df = fetch_daily_price(s, start='2024-01-01')
        n = upsert_prices(df)
        total += n
        print(f"  ✔ {s}: +{n}")
    print(f"  ✅ prices: {total}")
    pnl = compute_pnl()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM fact_daily_pnl WHERE trade_date = CURRENT_DATE;"))
    pnl.to_sql('fact_daily_pnl', engine, if_exists='append', index=False)
    print(f"  ✅ pnl: {len(pnl)}")
    print(f"🎉 Done: {datetime.now()}")

if __name__ == '__main__':
    run()
