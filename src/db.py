import sqlite3
import pandas as pd
from datetime import datetime
from .config import DB_PATH

class DBManager:
    def __init__(self, db_path=DB_PATH):
        self.db_path = str(db_path)
        self._init_tables()

    def get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_tables(self):
        """Initialize metadata tables if they don't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # 1. Stock Basic Info
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stock_basic (
            code TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            area TEXT,
            industry TEXT,
            market TEXT,
            list_date TEXT
        )
        ''')

        # 2. Trade Calendar
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS trade_cal (
            calendar_date TEXT PRIMARY KEY,
            is_trading_day INTEGER
        )
        ''')

        # 3. Update Log
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS update_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            execute_date TEXT,
            status TEXT,
            message TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        conn.commit()
        conn.close()

    def get_latest_update_date(self):
        """Get the last date we successfully updated data."""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT execute_date FROM update_log 
            WHERE status = 'SUCCESS' 
            ORDER BY execute_date DESC 
            LIMIT 1
        ''')
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def log_update(self, date_str, status, message=""):
        """Log the result of a daily update."""
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO update_log (execute_date, status, message)
            VALUES (?, ?, ?)
        ''', (date_str, status, message))
        conn.commit()
        conn.close()

    def save_stock_basic(self, df: pd.DataFrame):
        """Save/Update stock list."""
        if df.empty:
            return
        conn = self.get_connection()
        # pandas to_sql with 'replace' drops the table, which kills primary keys if not careful.
        # simpler to just replace for a small table like stock_basic or use append.
        # For stock_basic, full replace is usually fine as it's small (5000 rows).
        df.to_sql('stock_basic', conn, if_exists='replace', index=False)
        conn.close()

    def save_trade_cal(self, df: pd.DataFrame):
        """Save trade calendar."""
        if df.empty:
            return
        conn = self.get_connection()
        # Append logic or replace? Calendar is static-ish but grows.
        # Let's replace to be safe and simple for now.
        df.to_sql('trade_cal', conn, if_exists='replace', index=False)
        conn.close()

    def get_trade_cal(self, start_date, end_date):
        """Get trading days between range."""
        conn = self.get_connection()
        query = f"SELECT calendar_date FROM trade_cal WHERE is_trading_day = 1 AND calendar_date BETWEEN '{start_date}' AND '{end_date}' ORDER BY calendar_date"
        df = pd.read_sql(query, conn)
        conn.close()
        return df['calendar_date'].tolist()
