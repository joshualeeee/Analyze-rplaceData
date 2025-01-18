import sys
from datetime import datetime
import time
from collections import Counter
import pandas as pd

def checkDates(startDate, endDate):
    try:
        datetime.strptime(startDate, "%Y-%m-%d %H")
    except ValueError:
        print(f"Error: '{startDate}' is not in the correct format 'YYYY-MM-DD HH'.")
        sys.exit(1)

    try:
        datetime.strptime(endDate, "%Y-%m-%d %H")
    except ValueError:
        print(f"Error: '{endDate}' is not in the correct format 'YYYY-MM-DD HH'.")
        sys.exit(1)

    if endDate <= startDate:
        print("Error: End date must be after the start date.")
        sys.exit(1) 
    
    return startDate, endDate

def pandas(startDate, endDate):
    # Read the CSV file
    df = pd.read_csv('../../2022_place_canvas_history.csv', usecols=['timestamp', 'pixel_color', 'coordinate'])

    # Filter rows based on the time range
    filtered_df = df[(df['timestamp'] >= startDate) & (df['timestamp'] <= endDate)]

    # Get most placed pixel_color and coordinate
    pixel_color = filtered_df['pixel_color'].value_counts().idxmax()
    coordinate = filtered_df['coordinate'].value_counts().idxmax()

    return pixel_color, coordinate

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: analyzer.py <start_date> <end_date>")
        sys.exit(1)

    # Extract command-line arguments
    start_date_str = sys.argv[1]
    end_date_str = sys.argv[2]

    # Validate and start and end date
    startDate, endDate = checkDates(start_date_str, end_date_str)
    
    # Start the timer
    startTime = time.perf_counter_ns()

    color, coord = pandas(startDate, endDate)

    # End the timer
    endTime = time.perf_counter_ns()

    # Calculate elapsed time in seconds
    elapsedTime_ns = endTime - startTime
    elapsedTime_ms = elapsedTime_ns / 1_000_000
    
    print(f"**Timeframe:** {start_date_str} to {end_date_str}")
    print(f"**Execution Time:** {elapsedTime_ms:.6f} ms")
    print(f"**Most Placed Color:** {color} ")
    print(f"**Most Placed Pixel Location:** {coord} ")

