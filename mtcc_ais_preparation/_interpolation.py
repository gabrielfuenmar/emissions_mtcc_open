"""
Interpolation functions
___________________
pos_interp - Interpolate missing positions in the raw AIS data with PySpark

Author: Gabriel Fuentes
Affiliation: NHH (Norway) / MTCC Latin America
Contact: gabriel.fuentes@nhh.no
"""

# Import necessary PySpark modules
import pyspark.sql
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F
from typing import Set, Dict, List, Tuple, Optional

# Function to perform linear interpolation of latitudes and longitudes
def pos_interp(dfspark: DataFrame,
               interval_minutes: int,
               group: Optional[List[str]] = None, 
               columns: Optional[List[str]] = ["*"]):
    '''
    Linear interpolation of latitudes and longitudes every resample_interval minutes.

    Uses the Spark SQL `explode` function to create interpolated positions. Missing positions 
    within a 2-hour window are interpolated; beyond that, values remain unchanged.

    Parameters
    ----------
    dfspark: Spark DataFrame
        Input Spark DataFrame containing vessel tracking data.

    interval_minutes: int
        Frequency in minutes for interpolating positions.

    group: list of str, default None
        List of columns used to partition the DataFrame for processing.

    columns: list of str, default ["*"]
        List of columns to retain in the output DataFrame. If not specified, all columns are retained.

    Returns
    -------
    Spark DataFrame
        DataFrame with interpolated latitudes and longitudes on the `dt_pos_utc` column, 
        spaced every `interval_minutes`.
    '''
    try:
        # Convert resampling interval from minutes to seconds
        resample_interval = interval_minutes * 60

        # Use all columns if none are explicitly specified
        if columns == ["*"]:
            columns = dfspark.columns

        # Define additional required columns based on input columns
        if "segr" and "flag" in columns:
            # Case with `segr` and `flag` in columns
            cols_in = columns + [
                "LAG(dt_pos_utc) OVER (PARTITION BY imo, segr, flag ORDER BY dt_pos_utc ASC) as dt_pos_utc_lag",
                "dt_pos_utc as NextTimestamp",
                "latitude as latitude_old",
                "longitude as longitude_old",
                "LAG(latitude) OVER (PARTITION BY imo, segr, flag ORDER BY dt_pos_utc ASC) as latitude_lag",
                "LAG(longitude) OVER (PARTITION BY imo, segr, flag ORDER BY dt_pos_utc ASC) as longitude_lag"
            ]
        else:
            # Default grouping columns if not provided
            if not group:
                group = ["imo"]  # Default group for partitioning
            partition_by_clause = ", ".join(group)

            cols_in = columns + [
                f"LAG(dt_pos_utc) OVER (PARTITION BY {partition_by_clause} ORDER BY dt_pos_utc ASC) as dt_pos_utc_lag",
                "dt_pos_utc as NextTimestamp",
                "latitude as latitude_old",
                "longitude as longitude_old",
                f"LAG(latitude) OVER (PARTITION BY {partition_by_clause} ORDER BY dt_pos_utc ASC) as latitude_lag",
                f"LAG(longitude) OVER (PARTITION BY {partition_by_clause} ORDER BY dt_pos_utc ASC) as longitude_lag"
            ]

        # Perform interpolation using Spark SQL functions
        df_interpolated = (
            dfspark
            .selectExpr(*cols_in)
            # Round up and down timestamps to resampling interval
            .withColumn("PreviousTimestampRoundUp", 
                        F.expr(f"to_timestamp(ceil(unix_timestamp(dt_pos_utc_lag) / {resample_interval}) * {resample_interval})"))
            .withColumn("NextTimestampRoundDown", 
                        F.expr(f"to_timestamp(floor(unix_timestamp(NextTimestamp) / {resample_interval}) * {resample_interval})"))
            .filter("PreviousTimestampRoundUp <= NextTimestampRoundDown")  # Filter valid time ranges
            # Generate interpolated timestamps within the valid range
            .withColumn("dt_pos_utc", 
                        F.expr(f"explode(sequence(PreviousTimestampRoundUp, NextTimestampRoundDown, interval {resample_interval} second)) as dt_pos_utc"))
            .filter("dt_pos_utc < NextTimestamp")  # Ensure timestamps are valid
            # Interpolate latitude based on time difference
            .withColumn(
                "latitude",
                F.when(
                    F.expr("(unix_timestamp(dt_pos_utc) - unix_timestamp(dt_pos_utc_lag)) < 7200"),
                    F.expr(f"(unix_timestamp(dt_pos_utc) - unix_timestamp(dt_pos_utc_lag)) \
                            / (unix_timestamp(NextTimestamp) - unix_timestamp(dt_pos_utc_lag)) \
                            * (latitude_old - latitude_lag) + latitude_lag")
                ).otherwise(F.col("latitude_old"))  # Retain original latitude for large gaps
            )
            # Interpolate longitude similarly
            .withColumn(
                "longitude",
                F.when(
                    F.expr("(unix_timestamp(dt_pos_utc) - unix_timestamp(dt_pos_utc_lag)) < 7200"),
                    F.expr(f"(unix_timestamp(dt_pos_utc) - unix_timestamp(dt_pos_utc_lag)) \
                            / (unix_timestamp(NextTimestamp) - unix_timestamp(dt_pos_utc_lag)) \
                            * (longitude_old - longitude_lag) + longitude_lag")
                ).otherwise(F.col("longitude_old"))  # Retain original longitude for large gaps
            )
            .selectExpr(*columns)  # Retain user-specified columns
            # Calculate previous frequency for checking gaps
            .withColumn("previous_freq",
                        F.expr(f"LAG(dt_pos_utc) OVER (PARTITION BY imo ORDER BY dt_pos_utc ASC) as previous_freq"))
            .withColumn(
                "freq",
                F.when(
                    F.expr("(unix_timestamp(dt_pos_utc) - unix_timestamp(previous_freq)) < 7200"),
                    F.expr(f"(unix_timestamp(dt_pos_utc) - unix_timestamp(previous_freq)) / 3600")
                ).otherwise(F.lit(0))  # Set frequency to 0 for large gaps
            )
            .drop("previous_freq")  # Drop intermediate column
        )

        return df_interpolated

    except Exception as e:
        # Handle exceptions and provide debugging hints
        print(f'Have you checked that the columns (imo, mmsi, draught, sog, latitude, longitude, dt_pos_utc) are in the dataframe?')
        print(f'Error: \n{e}\n')

