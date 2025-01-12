import csv
import sys
from datetime import datetime
import time
from collections import Counter

def checkDates(startDate, endDate):
    try:
        startDate = datetime.strptime(startDate, "%Y-%m-%d %H")
    except ValueError:
        print(f"Error: '{startDate}' is not in the correct format 'YYYY-MM-DD HH'.")
        sys.exit(1)

    try:
        endDate = datetime.strptime(endDate, "%Y-%m-%d %H")
    except ValueError:
        print(f"Error: '{endDate}' is not in the correct format 'YYYY-MM-DD HH'.")
        sys.exit(1)

    if endDate <= startDate:
        print("Error: End date must be after the start date.")
        sys.exit(1) 
    
    return startDate, endDate

def main(startDate, endDate):
    with open('../2022_place_canvas_history.csv', 'r') as file:
        colorCounter = Counter()
        coordCounter = Counter()

        reader = csv.reader(file)
        headers = next(reader)  # Get the header row
        timeIdx = headers.index('timestamp')
        pixel_colorIdx = headers.index('pixel_color')
        coordIdx = headers.index('coordinate')

        for row in reader:
            # Parse the timestamp
            time = datetime.strptime(row[timeIdx],  "%Y-%m-%d %H")
            
            # Filter rows based on the time range
            if startDate <= time <= endDate:
                # Increment counters for pixel_color and coordinate
                colorCounter[row[pixel_colorIdx]] += 1
                coordCounter[row[coordIdx]] += 1
        
        mostPlacedColor, colorCount = colorCounter.most_common(1)[0]
        mostPlacedCoord, coordCount = coordCounter.most_common(1)[0]

        return mostPlacedColor, mostPlacedCoord    

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

    color, coord = main(startDate, endDate)

    # End the timer
    endTime = time.perf_counter_ns()

    # Calculate elapsed time in nanoseconds
    elapsedTime_ns = endTime - startTime
    elapsedTime_s = elapsedTime_ns / 1000000000
    
    print(f"**Timeframe:** {start_date_str} to {end_date_str}")
    print(f"**Execution Time:** {elapsedTime_s:.6f} seconds")
    print(f"**Most Placed Color:** {color} ")
    print(f"**Most Placed Pixel Location:** {coord} ")

