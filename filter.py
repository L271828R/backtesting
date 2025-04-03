import pandas as pd
import sys

def filter_by_date(input_file, output_file, target_date):
    # Read the CSV file into a DataFrame.
    df = pd.read_csv(input_file)
    
    # Convert the "session" column and target_date to datetime.
    df['session_date'] = pd.to_datetime(df['session'])
    target_date_dt = pd.to_datetime(target_date)
    
    # Define the start date as two days prior to the target date.
    start_date = target_date_dt - pd.Timedelta(days=2)
    
    # Filter rows where the session date is between start_date and target_date (inclusive).
    mask = (df['session_date'] >= start_date) & (df['session_date'] <= target_date_dt)
    filtered_df = df[mask]
    
    # Export the filtered DataFrame to a new CSV file.
    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered data for {start_date.date()} through {target_date_dt.date()} saved to {output_file}.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python filter_by_date.py <input_file> <target_date>")
        print("Example: python filter_by_date.py input.csv 2022-05-11")
        sys.exit(1)
        
    input_file = sys.argv[1]
    target_date = sys.argv[2]
    output_file = "data.csv"
    
    filter_by_date(input_file, output_file, target_date)

