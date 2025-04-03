import pandas as pd
import datetime
import os

def process_data(csv_file, days_back=10, target_time="12:30"):
    # STEP A: Read CSV (CSV has headers: Date, Time, Open, High, Low, Close, Volume)
    df = pd.read_csv(
        csv_file,
        parse_dates={'datetime': ['Date', 'Time']},
        infer_datetime_format=True
    )
    # Rename columns for consistency.
    df.columns = ['datetime', 'Open', 'High', 'Low', 'Close', 'Volume']
    df.set_index('datetime', inplace=True)
    df.sort_index(inplace=True)

    # STEP B: Filter data to the last `days_back` calendar days.
    last_dt = df.index.max()
    cutoff_dt = last_dt - pd.Timedelta(days=days_back)
    df = df.loc[df.index >= cutoff_dt]

    # Create 'date' and 'time' columns for convenience.
    df['date'] = df.index.date
    df['time'] = df.index.time

    # STEP C: Define a "session" for each bar.
    # If time >= 9:30, session is that day; otherwise, session is previous day.
    nine_thirty = pd.to_datetime("09:30:00").time()
    def assign_session(dt):
        if dt.time() >= nine_thirty:
            return dt.date()
        else:
            return (dt - pd.Timedelta(days=1)).date()
    df['session'] = df.index.to_series().apply(assign_session)

    # Save filtered data for QA if needed.
    df.to_csv("filtered_ES_30min_9AM_4PM.csv")

    # STEP D: Compute Session-based VWAP and VWAP Bands.
    df['typical_price'] = (df['High'] + df['Low'] + df['Close']) / 3
    df['cum_count'] = df.groupby('session').cumcount() + 1
    df['cum_tp_volume'] = (df['typical_price'] * df['Volume']).groupby(df['session']).cumsum()
    df['cum_volume'] = df.groupby('session')['Volume'].cumsum()
    df['VWAP'] = df['cum_tp_volume'] / df['cum_volume']

    # Compute cumulative (expanding) standard deviation for each session.
    df['session_std'] = df.groupby('session')['typical_price'].transform(lambda x: x.expanding().std().fillna(0))
    # For futures data, data before 9:30 should not be used in the new session's std.
    df.loc[df['time'] < nine_thirty, 'session_std'] = 0

    df['vwap_upper'] = df['VWAP'] + 2 * df['session_std']
    df['vwap_lower'] = df['VWAP'] - 2 * df['session_std']

    # STEP E: Compute Target Time Labels (only on target_time, e.g., 12:30).
    target_time_obj = pd.to_datetime(target_time).time()

    def label_intraday_state(session_df, check_time):
        # Pick only bars that are exactly at the target time.
        target_bar = session_df.loc[session_df['time'] == check_time]
        if target_bar.empty:
            return None
        # Use the last bar at that time.
        selected_bar = target_bar.iloc[-1]
        final_dt = selected_bar.name  # datetime index of the target bar
        # To compute the label, use data only up to the check time.
        session_up_to_check = session_df[session_df['time'] <= check_time]
        high = session_up_to_check['High'].max()
        low = session_up_to_check['Low'].min()
        rng = high - low
        final_close = selected_bar['Close']
        if rng == 0:
            return (final_dt, 'neutral')
        position = (final_close - low) / rng
        if position >= 0.75:
            return (final_dt, 'high')
        elif position <= 0.25:
            return (final_dt, 'low')
        else:
            return (final_dt, 'neutral')

    target_time_labels = {}
    for session_key, session_df in df.groupby('session'):
        result = label_intraday_state(session_df, target_time_obj)
        if result is not None:
            bar_dt, label = result
            target_time_labels[bar_dt] = label

    # Merge target time labels into the main dataframe as a new column.
    # Only rows corresponding to the target time will have a non-null label.
    df['target_label'] = df.index.map(target_time_labels)
    
    # (Optional) Compute additional daily aggregates if needed.
    daily = df.groupby('date').agg(
        open=('Open', 'first'),
        close=('Close', 'last')
    ).reset_index()
    daily['down_day'] = daily['close'] < daily['open']
    daily['group'] = (daily['down_day'] != daily['down_day'].shift()).cumsum()
    down_groups = daily[daily['down_day']].groupby('group').size()
    max_consecutive_down = down_groups.max() if not down_groups.empty else 0
    print("Maximum consecutive down days:", max_consecutive_down)

    return df

def save_processed_data(df, filename):
    df.to_csv(filename)

def save_formulas_csv():
    """
    Save an additional CSV file that includes Google Sheets formulas for auditing the calculations.
    Assumes the raw data is in columns A-G with headers:
    Date, Time, Open, High, Low, Close, Volume.
    The additional columns (H-P) contain formulas:
    session, typical_price, cum_count, cum_tp_volume, cum_volume, VWAP, session_std, vwap_upper, vwap_lower.
    """
    formulas_csv = '''Date,Time,Open,High,Low,Close,Volume,session,typical_price,cum_count,cum_tp_volume,cum_volume,VWAP,session_std,vwap_upper,vwap_lower
2025-01-27,9:30:00,5998,6037.75,5996.75,6037,255260,=IF(TIMEVALUE(B2)>=TIMEVALUE("09:30:00"),A2,A2-1),=(D2+E2+F2)/3,=1,=I2*G2,=G2,0,=K2/L2,=K2/L2+2*0,=K2/L2-2*0
2025-01-27,10:00:00,6036.75,6043,6017.5,6041,180546,=IF(TIMEVALUE(B3)>=TIMEVALUE("09:30:00"),A3,A3-1),=(D3+E3+F3)/3,=IF(H3=H2,J2+1,1),=IF(H3=H2,K2+I3*G3,I3*G3),=IF(H3=H2,L2+G3,G3),=K3/L3,=STDEV(FILTER($I$2:I3,$H$2:H3=H3)),=K3/L3+2*STDEV(FILTER($I$2:I3,$H$2:H3=H3)),=K3/L3-2*STDEV(FILTER($I$2:I3,$H$2:H3=H3))
2025-01-27,10:30:00,6041,6048,6031.25,6031.75,137204,=IF(TIMEVALUE(B4)>=TIMEVALUE("09:30:00"),A4,A4-1),=(D4+E4+F4)/3,=IF(H4=H3,J3+1,1),=IF(H4=H3,K3+I4*G4,I4*G4),=IF(H4=H3,L3+G4,G4),=K4/L4,=STDEV(FILTER($I$2:I4,$H$2:H4=H4)),=K4/L4+2*STDEV(FILTER($I$2:I4,$H$2:H4=H4)),=K4/L4-2*STDEV(FILTER($I$2:I4,$H$2:H4=H4))
2025-01-27,11:00:00,6031.75,6033.5,6013.75,6016,110116,=IF(TIMEVALUE(B5)>=TIMEVALUE("09:30:00"),A5,A5-1),=(D5+E5+F5)/3,=IF(H5=H4,J4+1,1),=IF(H5=H4,K4+I5*G5,I5*G5),=IF(H5=H4,L4+G5,G5),=K5/L5,=STDEV(FILTER($I$2:I5,$H$2:H5=H5)),=K5/L5+2*STDEV(FILTER($I$2:I5,$H$2:H5=H5)),=K5/L5-2*STDEV(FILTER($I$2:I5,$H$2:H5=H5))
2025-01-27,11:30:00,6016,6028.75,6013.25,6019.25,118030,=IF(TIMEVALUE(B6)>=TIMEVALUE("09:30:00"),A6,A6-1),=(D6+E6+F6)/3,=IF(H6=H5,J5+1,1),=IF(H6=H5,K5+I6*G6,I6*G6),=IF(H6=H5,L5+G6,G6),=K6/L6,=STDEV(FILTER($I$2:I6,$H$2:H6=H6)),=K6/L6+2*STDEV(FILTER($I$2:I6,$H$2:H6=H6)),=K6/L6-2*STDEV(FILTER($I$2:I6,$H$2:H6=H6))
'''
    with open('formulas_ES_30min.csv', 'w') as f:
        f.write(formulas_csv)
    print("CSV with formulas saved to formulas_ES_30min.csv")

if __name__ == '__main__':
    processed_df = process_data("ES_30min.csv", days_back=1000, target_time="12:30")
    save_processed_data(processed_df, "filtered_ES_30min_9AM_4PM.csv")
    save_formulas_csv()
    print("Processed data saved to filtered_ES_30min_9AM_4PM.csv")

