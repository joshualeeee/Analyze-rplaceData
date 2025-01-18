import sys
from datetime import datetime
import time
from collections import Counter
import polars as pl

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

def polars(startDate, endDate):
    startDate = datetime.strptime(startDate, "%Y-%m-%d %H").strftime("%Y-%m-%d %H:%M:%S")
    endDate = datetime.strptime(endDate, "%Y-%m-%d %H").strftime("%Y-%m-%d %H:%M:%S")

    lazy_df = pl.scan_csv("../../2022_place_canvas_history.csv")

    filtered = (
        lazy_df
        .filter((pl.col("timestamp") >= pl.lit(startDate)) & (pl.col("timestamp") <= pl.lit(endDate)))
        .collect()  # Execute and load into memory after filtering
    )

    # same query as duckDB in polars form
    pixel_color = (
        filtered
        .group_by("pixel_color")  
        .len()  
        .sort("len", descending=True)  
        .select("pixel_color")
        .head(1)  
    )[0, "pixel_color"]

    coordinate = (
        filtered
        .group_by("coordinate") 
        .len()  
        .sort("len", descending=True)  
        .select("coordinate")
        .head(1)  
    )[0, "coordinate"]
    
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

    color, coord = polars(startDate, endDate)

    # End the timer
    endTime = time.perf_counter_ns()

    # Calculate elapsed time in seconds
    elapsedTime_ns = endTime - startTime
    elapsedTime_ms = elapsedTime_ns / 1_000_000
    
    print(f"**Timeframe:** {start_date_str} to {end_date_str}")
    print(f"**Execution Time:** {elapsedTime_ms:.6f} ms")
    print(f"**Most Placed Color:** {color} ")
    print(f"**Most Placed Pixel Location:** {coord} ")

