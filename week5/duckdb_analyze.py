import sys
from datetime import datetime
import time
from collections import Counter
import duckdb
import webcolors
import pandas as pd
import matplotlib.pyplot as plt

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

def premleague_analyzer():
    parquet_file = '../rPlace.parquet'

    con = duckdb.connect()
    
    labels = ["arsenal", "spurs"]

    # top left and bottom right for main artworks
    coords = [[[704, 484], [752, 532]], [[1684, 420], [1714, 466]]]    

    x1_arsenal, y1_arsenal = coords[0][0]
    x2_arsenal, y2_arsenal = coords[0][1]

    x1_spurs, y1_spurs = coords[1][0]
    x2_spurs, y2_spurs = coords[1][1]
    
    result = con.execute(f"""
                            WITH arsenal_users AS (
                                SELECT DISTINCT user_id_numerical as user_id
                                FROM '{parquet_file}'
                                WHERE x BETWEEN {x1_arsenal} AND {x2_arsenal}
                                AND y BETWEEN {y1_arsenal} AND {y2_arsenal}
                            ),
                            spurs_users AS (
                                SELECT DISTINCT user_id_numerical as user_id
                                FROM '{parquet_file}'
                                WHERE x BETWEEN {x1_spurs} AND {x2_spurs}
                                AND y BETWEEN {y1_spurs} AND {y2_spurs}
                            ),
                            common_users AS (
                                SELECT user_id FROM arsenal_users
                                INTERSECT
                                SELECT user_id FROM spurs_users
                            )
                            SELECT 
                                (SELECT COUNT(*) FROM arsenal_users) AS arsenal_user_count,
                                (SELECT COUNT(*) FROM spurs_users) AS spurs_user_count,
                                (SELECT COUNT(*) FROM common_users) AS common_user_count,
                                (SELECT COUNT(*) FROM (
                                    SELECT user_id FROM arsenal_users
                                    UNION
                                    SELECT user_id FROM spurs_users
                                )) AS total_unique_users;
                        """).fetchall()

    # Print results
    print("\n**Results**")
    print(f"Arsenal Distinct Users: {result[0][0]}")
    print(f"Spurs Distinct Users: {result[0][1]}")
    print(f"Common Users: {result[0][2]}")

    # Execute the query
    result = con.execute(f"""
                            WITH color_counts AS (
                                SELECT 
                                    CASE 
                                        WHEN x BETWEEN {x1_arsenal} AND {x2_arsenal} AND y BETWEEN {y1_arsenal} AND {y2_arsenal} THEN 'arsenal'
                                        WHEN x BETWEEN {x1_spurs} AND {x2_spurs} AND y BETWEEN {y1_spurs} AND {y2_spurs} THEN 'spurs'
                                    END AS team,
                                    pixel_color, 
                                    COUNT(*) AS color_count
                                FROM read_parquet('{parquet_file}')
                                WHERE (x BETWEEN {x1_arsenal} AND {x2_arsenal} AND y BETWEEN {y1_arsenal} AND {y2_arsenal}) 
                                OR (x BETWEEN {x1_spurs} AND {x2_spurs} AND y BETWEEN {y1_spurs} AND {y2_spurs})
                                GROUP BY team, pixel_color
                            )
                            SELECT team, pixel_color, color_count
                            FROM color_counts
                            ORDER BY team, color_count DESC
                        """).fetchall()
                        
    df = pd.DataFrame(result, columns=["team", "pixel_color", "color_count"])

    # Convert pixel colors to English names
    df["color_name"] = df["pixel_color"].apply(hex_to_name)

    # Separate data for Arsenal and Spurs
    arsenal_df = df[df["team"] == "arsenal"].head(5)  # Top 5 colors
    spurs_df = df[df["team"] == "spurs"].head(5)  # Top 5 colors

    # Arsenal Pie Chart
    fig, ax = plt.subplots(figsize=(6, 6))
    fig.patch.set_facecolor("grey")  # Set background color to grey

    ax.pie(
        arsenal_df["color_count"],
        labels=arsenal_df["color_name"],  # Use English names
        autopct='%1.1f%%',
        startangle=90,
        colors=arsenal_df["pixel_color"]  # Use actual pixel colors
    )
    ax.set_title("Top 5 Colors in Arsenal Artwork", color="black")
    ax.set_facecolor("grey")

    plt.savefig("./arsenal_pie_chart.png", facecolor="grey")
    plt.close()

    # Spurs Pie Chart
    fig, ax = plt.subplots(figsize=(6, 6))
    fig.patch.set_facecolor("grey")  # Set background color to grey

    ax.pie(
        spurs_df["color_count"],
        labels=spurs_df["color_name"],  # Use English names
        autopct='%1.1f%%',
        startangle=90,
        colors=spurs_df["pixel_color"]  # Use actual pixel colors
    )
    ax.set_title("Top 5 Colors in Spurs Artwork", color="black")
    ax.set_facecolor("grey")

    plt.savefig("./spurs_pie_chart.png", facecolor="grey")
    plt.close()

if __name__ == "__main__":
    premleague_analyzer()

