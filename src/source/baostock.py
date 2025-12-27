import baostock as bs
import pandas as pd
from datetime import datetime
from .base import DataSource
import time

class BaostockSource(DataSource):
    def login(self):
        lg = bs.login()
        if lg.error_code != '0':
            raise Exception(f"Baostock login failed: {lg.error_msg}")
        print(f"Baostock logged in: {lg.error_msg}")

    def logout(self):
        bs.logout()
        print("Baostock logged out")

    def get_stock_basic(self) -> pd.DataFrame:
        """
        Get all A-share stocks.
        """
        rs = bs.query_stock_basic()
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        # Rename columns to match our DB schema if needed or keep as is
        # DB schema: code, symbol, name, area, industry, market, list_date
        # Baostock fields: code, tradeStatus, code_name
        # Note: query_stock_basic doesn't return area/industry. 
        # We might need better source for that later or just ignore.
        # For now, we map what we have.
        
        # Baostock returns: code, code_name, ipoDate, outDate, type, status
        df_mapped = df.rename(columns={
            'code': 'code',
            'code_name': 'name',
            'ipoDate': 'list_date'
        })
        
        # Add missing columns as empty to verify schema
        for col in ['symbol', 'area', 'industry', 'market']:
            if col not in df_mapped.columns:
                df_mapped[col] = ''
                
        return df_mapped

    def get_trade_cal(self, start_date: str, end_date: str) -> pd.DataFrame:
        rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        return df

    def get_daily_data(self, date: str) -> pd.DataFrame:
        """
        Get 5-minute data for ALL stocks on a specific date.
        """
        # 1. Get stock list first (can cache this in memory or DB)
        stock_df = self.get_stock_basic()
        stock_codes = stock_df['code'].tolist()
        
        print(f"Fetching 5-min data for {len(stock_codes)} stocks on {date}...")
        
        all_data = []
        
        # Fields to fetch for 5min
        # Note: Baostock 5min fields usually include 'time' (YYYYMMDDHHMMSS)
        fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"
        
        # 2. Loop requests
        count = 0
        total = len(stock_codes)
        
        for code in stock_codes:
            rs = bs.query_history_k_data_plus(
                code, fields, 
                start_date=date, end_date=date, 
                frequency="5", adjustflag="3" # 5=5min, 3=Unadjusted
            )
            
            if rs.error_code == '0':
                while rs.next():
                    all_data.append(rs.get_row_data())
            
            count += 1
            if count % 100 == 0:
                print(f"Progress: {count}/{total}")
        
        if not all_data:
            return pd.DataFrame()
            
        df = pd.DataFrame(all_data, columns=fields.split(','))
        
        # Convert numeric columns
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        return df
