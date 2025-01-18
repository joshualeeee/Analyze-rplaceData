import sys
from datetime import datetime
import time
from collections import Counter
import duckdb

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

def duckDB(startDate, endDate):
    # Format start and end date to wort with query
    startDate = datetime.strptime(startDate, "%Y-%m-%d %H").strftime("%Y-%m-%d %H:%M:%S")
    endDate = datetime.strptime(endDate, "%Y-%m-%d %H").strftime("%Y-%m-%d %H:%M:%S")

    # Connect to DuckDB (in-memory or persistent)
    con = duckdb.connect()
    # Query to find the most frequent pixel_color and location
    query = f"""
            WITH
            pixel_color_frequency AS (
                SELECT pixel_color, COUNT(*) as frequency
                FROM read_csv_auto('../../2022_place_canvas_history.csv')
                WHERE timestamp BETWEEN '{startDate}' AND '{endDate}'
                GROUP BY pixel_color
                ORDER BY frequency DESC
                LIMIT 1
            ),
            coord_frequency AS (
                SELECT coordinate, COUNT(*) as frequency
                FROM read_csv_auto('../../2022_place_canvas_history.csv')
                WHERE timestamp BETWEEN '{startDate}' AND '{endDate}'
                GROUP BY coordinate
                ORDER BY frequency DESC
                LIMIT 1
            )
            SELECT
                (SELECT pixel_color FROM pixel_color_frequency),
                (SELECT coordinate FROM coord_frequency)
    """

    # Execute the query
    result = con.execute(query).fetchall()
    pixel_color = result[0][0]
    coordinate = result[0][1]

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
    
    print(startDate, endDate)

    # Start the timer
    startTime = time.perf_counter_ns()

    color, coord = duckDB(startDate, endDate)

    # End the timer
    endTime = time.perf_counter_ns()

    # Calculate elapsed time in seconds
    elapsedTime_ns = endTime - startTime
    elapsedTime_ms = elapsedTime_ns / 1_000_000
    
    print(f"**Timeframe:** {start_date_str} to {end_date_str}")
    print(f"**Execution Time:** {elapsedTime_ms:.6f} ms")
    print(f"**Most Placed Color:** {color} ")
    print(f"**Most Placed Pixel Location:** {coord} ")

