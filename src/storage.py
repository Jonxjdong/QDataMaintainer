import pandas as pd
from .config import DAILY_DATA_DIR, MIN5_DATA_DIR

class StorageManager:
    def __init__(self, frequency='5min'):
        """
        frequency: 'd' or '5min'
        """
        self.frequency = frequency
        if frequency == '5min':
            self.data_dir = MIN5_DATA_DIR
        else:
            self.data_dir = DAILY_DATA_DIR

    def save_daily_data(self, date_str: str, df: pd.DataFrame):
        """
        Save market data to a parquet file.
        FileName: YYYY-MM-DD.parquet (inside min5/ or daily/)
        """
        if df.empty:
            print(f"Warning: No data to save for {date_str}")
            return
        
        file_path = self.data_dir / f"{date_str}.parquet"
        
        # Ensure optimal compression
        # For 5min data, compression is critical
        df.to_parquet(file_path, engine='pyarrow', compression='zstd', index=False)
        print(f"Saved {file_path} ({len(df)} rows)")

    def load_daily_data(self, date_str: str) -> pd.DataFrame:
        """Load specific date data."""
        file_path = self.data_dir / f"{date_str}.parquet"
        if not file_path.exists():
            return pd.DataFrame()
        return pd.read_parquet(file_path)
