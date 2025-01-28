import sys
from datetime import datetime
import time
from collections import Counter
import duckdb
import webcolors
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

def hex_to_name(hex):
    try:
        return webcolors.hex_to_name(hex)
    except ValueError:
        return hex_to_closest_name(hex)

def hex_to_closest_name(hex):
    css3_colors = {name: webcolors.name_to_rgb(name) for name in webcolors.names("css3")}
    target = webcolors.hex_to_rgb(hex)
    # Find the closest color by calculating the Euclidean distance
    closest_color = min(
        css3_colors,
        key=lambda name: sum((css3_colors[name][i] - target[i]) ** 2 for i in range(3))
    )
    return closest_color

def parquet_analyzer(startDate, endDate):
    # Format start and end date to wort with query
    startDate = datetime.strptime(startDate, "%Y-%m-%d %H").strftime("%Y-%m-%d %H:%M:%S")
    endDate = datetime.strptime(endDate, "%Y-%m-%d %H").strftime("%Y-%m-%d %H:%M:%S")
    parquet_file = './rPlace.parquet'

    con = duckdb.connect()
    
    # Ranking of Colors by Distinct Users
    result = con.execute(f"""
                            SELECT 
                                pixel_color, 
                                COUNT(DISTINCT user_id_numerical) AS distinct_user_count
                            FROM 
                                parquet_scan('{parquet_file}')
                            WHERE 
                                timestamp BETWEEN '{startDate}' AND '{endDate}'
                            GROUP BY 
                                pixel_color
                            ORDER BY 
                                distinct_user_count DESC
                            """).fetchall()
    print("**Top Ranking of Colors by Distinct Users**")
    for i, res in enumerate(result):
        print(f"{i + 1}. {hex_to_name(res[0])}: {res[1]} users")

     # Average Session Length
    result = con.execute(f"""WITH session_starts AS (
                                SELECT
                                    user_id_numerical,
                                    timestamp,
                                    CASE
                                        WHEN EXTRACT(EPOCH FROM timestamp) - EXTRACT(EPOCH FROM LAG(timestamp) OVER (
                                            PARTITION BY user_id_numerical
                                            ORDER BY timestamp
                                        )) > 900 OR LAG(timestamp) OVER (
                                            PARTITION BY user_id_numerical
                                            ORDER BY timestamp
                                        ) IS NULL THEN 1
                                        ELSE 0
                                    END AS new_session_flag
                                FROM
                                    parquet_scan('{parquet_file}')
                                WHERE
                                    timestamp BETWEEN '{startDate}' AND '{endDate}'
                            ),
                            -- Group sessions by user and assign unique session IDs
                            grouped_sessions AS (
                                SELECT
                                    user_id_numerical,
                                    timestamp,
                                    SUM(new_session_flag) OVER (
                                        PARTITION BY user_id_numerical
                                        ORDER BY timestamp
                                    ) AS session_id
                                FROM
                                    session_starts
                            ),
                            -- Calculate sessions with start, end, and event count
                            sessions AS (
                                SELECT
                                    MIN(timestamp) AS session_start,
                                    MAX(timestamp) AS session_end,
                                FROM
                                    grouped_sessions
                                GROUP BY
                                    user_id_numerical, session_id
                                HAVING
                                    COUNT(*) > 1 -- Exclude sessions with only 1 event
                            )
                            SELECT
                                AVG(EXTRACT(EPOCH FROM session_end) - EXTRACT(EPOCH FROM session_start)) AS average_session_length
                            FROM
                                sessions;
                            """).fetchall()

    print("\n**Average Session Length**")
    print(f"Output: {result[0][0]:.2f} seconds\n")


     # Pixel Counts
    result = con.execute(f"""
                           WITH pixel_counts AS (
                                SELECT
                                    COUNT(*) AS pixels_placed
                                FROM
                                    parquet_scan('{parquet_file}')
                                WHERE
                                    timestamp BETWEEN '{startDate}' AND '{endDate}'
                                GROUP BY
                                    user_id_numerical
                            )
                            SELECT
                                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pixels_placed) AS percentile_50,
                                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY pixels_placed) AS percentile_75,
                                PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY pixels_placed) AS percentile_90,
                                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY pixels_placed) AS percentile_99
                            FROM
                                pixel_counts;
                        """).fetchall()

    print("**Percentiles of Pixels Placed**")
    print(f'50th Percentile: {result[0][0]} pixels')
    print(f'75th Percentile: {result[0][1]} pixels')
    print(f'90th Percentile: {result[0][2]} pixels')
    print(f'99th Percentile: {result[0][3]} pixels\n')


    # Count of First-Time Users
    result = con.execute(f"""
                           SELECT
                                COUNT(DISTINCT user_id_numerical) AS first_time_users
                            FROM
                                parquet_scan('{parquet_file}')
                            WHERE
                                timestamp BETWEEN '{startDate}' AND '{endDate}'AND user_id_numerical NOT IN (
                                    SELECT
                                        DISTINCT user_id_numerical
                                    FROM
                                        parquet_scan('{parquet_file}')
                                    WHERE
                                        timestamp < '{startDate}'
                                );
                            """).fetchall()

    print("**Count of First-Time Users**")
    print(f"Output: {result[0][0]} users\n")

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

    parquet_analyzer(startDate, endDate)

    # End the timer
    endTime = time.perf_counter_ns()

    # Calculate elapsed time in seconds
    elapsedTime_ns = endTime - startTime
    elapsedTime_ms = elapsedTime_ns / 1_000_000
    
    print(f"**Timeframe:** {start_date_str} to {end_date_str}")
    print(f"**Execution Time:** {elapsedTime_ms:.4f} ms")

