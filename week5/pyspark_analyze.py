import sys
from datetime import datetime
import time
from collections import Counter
import pyspark
import webcolors
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , when, count


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

    # Initialize Spark Session
    spark = SparkSession.builder.appName("PremierLeagueAnalyzer").getOrCreate()

    # Load Parquet File
    df = spark.read.parquet(parquet_file)

    # Define coordinate ranges
    coords = [[[704, 484], [752, 532]], [[1684, 420], [1714, 466]]]    

    x1_arsenal, y1_arsenal = coords[0][0]
    x2_arsenal, y2_arsenal = coords[0][1]

    x1_spurs, y1_spurs = coords[1][0]
    x2_spurs, y2_spurs = coords[1][1]

    # Define conditions for Arsenal and Spurs
    arsenal_cond = (col("x").between(x1_arsenal, x2_arsenal) & col("y").between(y1_arsenal, y2_arsenal))
    spurs_cond = (col("x").between(x1_spurs, x2_spurs) & col("y").between(y1_spurs, y2_spurs))

    # Get distinct user counts
    arsenal_users = df.filter(arsenal_cond).select("user_id_numerical").distinct()
    spurs_users = df.filter(spurs_cond).select("user_id_numerical").distinct()

    common_users = arsenal_users.intersect(spurs_users)

    arsenal_count = arsenal_users.count()
    spurs_count = spurs_users.count()
    common_count = common_users.count()

    # Print results
    print("\n**Results**")
    print(f"Arsenal Distinct Users: {arsenal_count}")
    print(f"Spurs Distinct Users: {spurs_count}")
    print(f"Common Users: {common_count}")

    # Count colors in each team's artwork
    color_counts = df.filter(arsenal_cond | spurs_cond) \
                     .groupBy(
                         when(arsenal_cond, "arsenal")
                         .when(spurs_cond, "spurs")
                         .alias("team"),
                         col("pixel_color")
                     ) \
                     .agg(count("*").alias("color_count")) \
                     .orderBy("team", col("color_count").desc())

    # Convert to Pandas DataFrame
    df = color_counts.toPandas()

    df["color_name"] = df["pixel_color"].apply(hex_to_name)

    # Separate data for Arsenal and Spurs
    arsenal_df = df[df["team"] == "arsenal"].head(5)  
    spurs_df = df[df["team"] == "spurs"].head(5) 

    # Arsenal Pie Chart
    fig, ax = plt.subplots(figsize=(6, 6))
    fig.patch.set_facecolor("grey") 

    ax.pie(
        arsenal_df["color_count"],
        labels=arsenal_df["color_name"],  
        autopct='%1.1f%%',
        startangle=90,
        colors=arsenal_df["pixel_color"]  
    )
    ax.set_title("Top 5 Colors in Arsenal Artwork", color="black")
    ax.set_facecolor("grey")

    plt.savefig("./arsenal_pie_chart_pyspark.png", facecolor="grey")
    plt.close()

    # Spurs Pie Chart
    fig, ax = plt.subplots(figsize=(6, 6))
    fig.patch.set_facecolor("grey")  

    ax.pie(
        spurs_df["color_count"],
        labels=spurs_df["color_name"],  
        autopct='%1.1f%%',
        startangle=90,
        colors=spurs_df["pixel_color"]  
    )
    ax.set_title("Top 5 Colors in Spurs Artwork", color="black")
    ax.set_facecolor("grey")

    plt.savefig("./spurs_pie_chart_pyspark.png", facecolor="grey")
    plt.close()

    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    premleague_analyzer()
