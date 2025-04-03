import pandas as pd
from datetime import datetime
from TradeData import TradeData  # Assumes TradeData is defined in trade_data.py

buffer = 10

def analyze_session(session, df):
    """
    Analyze a single session.

    This function looks for a 12:30 row to determine the trade rule:
      - If target_label is 'high': short at the vwap_upper price (increased by buffer points).
      - If target_label is 'low': buy at the vwap_lower price (decreased by buffer points).

    Then, it retrieves the 15:00 and 16:00 Close prices, computes the profit
    (for shorts: entry - evaluation; for longs: evaluation - entry) and indicates
    if the trade was profitable.
    
    Returns a dictionary with the session results or None if required rows are missing.
    """
    # Convert time column to string format ("HH:MM:SS") if needed.
    if not pd.api.types.is_string_dtype(df['time']):
        df = df.copy()
        df['time_str'] = df['time'].dt.strftime("%H:%M:%S")
    else:
        df['time_str'] = df['time']

    # Filter rows for the desired times.
    row_1230 = df[df['time_str'] == "12:30:00"]
    row_1500 = df[df['time_str'] == "15:00:00"]
    row_1600 = df[df['time_str'] == "16:00:00"]

    if row_1230.empty:
        print(f"Session {session} does not have a 12:30 entry.")
        return None

    entry_row = row_1230.iloc[0]
    target_label = entry_row['target_label']
    if pd.isna(target_label):
        print(f"Session {session} 12:30 row does not have a target_label.")
        return None

    # Determine the entry price based on the trade rule.
    # For a "high" rule, add 40 points to vwap_upper.
    # For a "low" rule, subtract 40 points from vwap_lower.
    if target_label.lower() == "high":
        entry_price = entry_row['vwap_upper'] + buffer
    elif target_label.lower() == "low":
        entry_price = entry_row['vwap_lower'] - buffer
    else:
        print(f"Session {session} has an unrecognized target_label: {target_label}")
        return None

    if row_1500.empty:
        print(f"Session {session} does not have a 15:00 entry.")
        return None
    if row_1600.empty:
        print(f"Session {session} does not have a 16:00 entry.")
        return None

    price_1500 = row_1500.iloc[0]['Close']
    price_16 = row_1600.iloc[0]['Close']

    # Calculate profit.
    # For a short trade ("high"): profit = entry_price - evaluation_price.
    # For a long trade ("low"): profit = evaluation_price - entry_price.
    if target_label.lower() == "high":
        profit_15 = entry_price - price_1500
        profit_16 = entry_price - price_16
    else:  # target_label.lower() == "low"
        profit_15 = price_1500 - entry_price
        profit_16 = price_16 - entry_price

    result = {
        "session": session,
        "target_label": target_label,
        "entry_price": entry_price,
        "price_15": price_1500,
        "profit_15": profit_15,
        "profitable_15": profit_15 > 0,
        "price_16": price_16,
        "profit_16": profit_16,
        "profitable_16": profit_16 > 0,
    }
    return result

def compute_winning_streaks(session_results):
    """
    Computes the longest, average, and shortest winning streaks (in days)
    based on sessions where the 16:00 trade was profitable.
    Sessions are assumed to have a date string in ISO format ("YYYY-MM-DD").
    """
    # Sort session_results by date (session string)
    sorted_results = sorted(
        session_results, 
        key=lambda x: datetime.strptime(x["session"], "%Y-%m-%d")
    )
    
    streaks = []
    current_streak = 0
    for res in sorted_results:
        if res.get("profitable_16"):
            current_streak += 1
        else:
            if current_streak > 0:
                streaks.append(current_streak)
            current_streak = 0
    if current_streak > 0:
        streaks.append(current_streak)

    if streaks:
        longest = max(streaks)
        shortest = min(streaks)
        average = sum(streaks) / len(streaks)
    else:
        longest = shortest = average = 0

    return longest, average, shortest

def main():
    # Path to your CSV file with trade data.
    csv_file = "filtered_ES_30min_9AM_4PM.csv"
    trade_data = TradeData(csv_file)

    session_results = []
    for session, df in trade_data.sessions:
        result = analyze_session(session, df)
        if result is not None:
            session_results.append(result)

    # Save individual session results to a CSV file.
    results_df = pd.DataFrame(session_results)
    results_csv_file = "session_results.csv"
    results_df.to_csv(results_csv_file, index=False)
    print(f"Individual session results saved to '{results_csv_file}'.")

    # Compute summary statistics over all sessions.
    total_sessions = len(results_df)
    avg_profit_15 = results_df['profit_15'].mean() if total_sessions > 0 else None
    avg_profit_16 = results_df['profit_16'].mean() if total_sessions > 0 else None
    positive_sessions_15 = results_df['profitable_15'].sum() if total_sessions > 0 else 0
    positive_sessions_16 = results_df['profitable_16'].sum() if total_sessions > 0 else 0

    # Compute winning streaks (using the 16:00 trade results).
    longest_streak, average_streak, shortest_streak = compute_winning_streaks(session_results)

    summary_lines = [
        f"Total sessions analyzed: {total_sessions}",
        f"Average profit at 15:00: {avg_profit_15:.2f}" if avg_profit_15 is not None else "Average profit at 15:00: N/A",
        f"Average profit at 16:00: {avg_profit_16:.2f}" if avg_profit_16 is not None else "Average profit at 16:00: N/A",
        f"Sessions profitable at 15:00: {positive_sessions_15} ({(positive_sessions_15/total_sessions*100):.2f}%)" if total_sessions > 0 else "Sessions profitable at 15:00: N/A",
        f"Sessions profitable at 16:00: {positive_sessions_16} ({(positive_sessions_16/total_sessions*100):.2f}%)" if total_sessions > 0 else "Sessions profitable at 16:00: N/A",
        "",
        "Winning Streaks (based on 16:00 profit):",
        f"  Longest winning streak (days): {longest_streak}",
        f"  Average winning streak (days): {average_streak:.2f}",
        f"  Shortest winning streak (days): {shortest_streak}",
    ]
    summary_text = "\n".join(summary_lines)

    summary_txt_file = "summary_stats.txt"
    with open(summary_txt_file, "w") as f:
        f.write(summary_text)

    print(f"Summary statistics saved to '{summary_txt_file}'.")

if __name__ == "__main__":
    main()

