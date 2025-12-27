import pandas as pd
import sqlite3
import os

# 1. 设置路径 (和之前配置一致)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "meta.db")
DATA_DIR = os.path.join(BASE_DIR, "data", "daily")

def read_metadata():
    print(f"--- 连接 SQLite: {DB_PATH} ---")
    # 直接像打开文件一样打开数据库
    conn = sqlite3.connect(DB_PATH)
    
    # 读股票列表
    df_stocks = pd.read_sql("SELECT * FROM stock_basic LIMIT 5", conn)
    print("股票列表前5行：")
    print(df_stocks)
    
    # 读日志
    df_log = pd.read_sql("SELECT * FROM update_log ORDER BY execute_date DESC LIMIT 5", conn)
    print("\n最近更新日志：")
    print(df_log)
    
    conn.close()

def read_market_data(date_str):
    file_path = os.path.join(DATA_DIR, f"{date_str}.parquet")
    print(f"\n--- 读取 Parquet: {file_path} ---")
    
    if not os.path.exists(file_path):
        print("文件不存在，请先运行 run_daily_update.py 下载数据")
        return

    # Pandas 直接读，无需任何 Server
    df = pd.read_parquet(file_path)
    
    # 随便查一只股票，比如 'sh.600519' (茅台) 或第一只
    if not df.empty:
        print(f"数据总行数: {len(df)}")
        print("前5行数据：")
        print(df.head())
        
        # 演示查询速度
        print("\n筛选代码为 sh.600000 (浦发银行) 的数据：")
        subset = df[df['code'] == 'sh.600000']
        print(subset)

if __name__ == "__main__":
    read_metadata()
    # 尝试读取 2023-01-03 的数据 (如果不存则会提示)
    read_market_data("2023-01-03")
