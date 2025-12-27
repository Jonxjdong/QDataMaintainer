import argparse
import datetime
import pandas as pd
from src.db import DBManager
from src.storage import StorageManager
from src.source.baostock import BaostockSource

def main():
    parser = argparse.ArgumentParser(description="Run Daily Stock Data Update")
    parser.add_argument("--start_date", help="Force start date (YYYY-MM-DD)", default=None)
    parser.add_argument("--end_date", help="Force end date (YYYY-MM-DD)", default=None)
    args = parser.parse_args()

    # 1. Initialize Managers
    db = DBManager()
    storage = StorageManager(frequency='5min')
    source = BaostockSource()

    # 2. Login to Data Source
    try:
        source.login()
         
        # 3. Update Meta (Stock List & Calendar)
        # Ideally we update stock list occasionally. Let's do it every run for now or check date.
        print("Updating stock list...")
        stock_list = source.get_stock_basic()
        db.save_stock_basic(stock_list)
        
        # Update Calendar (Current Year)
        this_year = datetime.datetime.now().year
        print("Updating trade calendar...")
        cal_df = source.get_trade_cal(f"{this_year-5}-01-01", f"{this_year+1}-12-31")
        db.save_trade_cal(cal_df)

        # 4. Determine Date Range
        if args.start_date:
            start_date = args.start_date
        else:
            last_date = db.get_latest_update_date()
            if last_date:
                start_date_obj = datetime.datetime.strptime(last_date, "%Y-%m-%d") + datetime.timedelta(days=1)
                start_date = start_date_obj.strftime("%Y-%m-%d")
            else:
                # Default init date if empty
                start_date = "2023-01-01"
        
        if args.end_date:
            end_date = args.end_date
        else:
            end_date = datetime.datetime.now().strftime("%Y-%m-%d")

        if start_date > end_date:
            print("Already up to date.")
            return

        print(f"Plan to update from {start_date} to {end_date}")
        
        # 5. Get valid trading days
        trading_days = db.get_trade_cal(start_date, end_date)
        
        if not trading_days:
            print("No trading days in range.")
            return

        # 6. Loop and Download
        for date_str in trading_days:
            print(f"Processing {date_str}...")
            try:
                # Check if we already have parquet (optional double check)
                # existing = storage.load_daily_data(date_str)
                # if not existing.empty:
                #     print(f"Skipping {date_str}, data exists.")
                #     continue

                daily_df = source.get_daily_data(date_str)
                
                if not daily_df.empty:
                    storage.save_daily_data(date_str, daily_df)
                    db.log_update(date_str, "SUCCESS", f"Rows: {len(daily_df)}")
                else:
                    print(f"No data found for {date_str} (might be holiday or error)")
                    db.log_update(date_str, "EMPTY", "No data returned")
                    
            except Exception as e:
                print(f"Error on {date_str}: {e}")
                db.log_update(date_str, "FAILED", str(e))
                # Break or Continue? Continue is usually flexible.
                continue

    finally:
        source.logout()

if __name__ == "__main__":
    main()
