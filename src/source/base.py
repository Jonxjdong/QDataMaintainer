from abc import ABC, abstractmethod
import pandas as pd

class DataSource(ABC):
    @abstractmethod
    def login(self):
        pass

    @abstractmethod
    def logout(self):
        pass

    @abstractmethod
    def get_stock_basic(self) -> pd.DataFrame:
        """Get list of all stocks."""
        pass

    @abstractmethod
    def get_trade_cal(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Get trading calendar."""
        pass

    @abstractmethod
    def get_daily_data(self, date: str) -> pd.DataFrame:
        """Get market data for all stocks on a specific date."""
        pass
