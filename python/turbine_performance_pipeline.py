from pathlib import Path
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# P
PROJECT_DIR = Path(__file__).resolve().parents[1]

print(f"Project directory: {PROJECT_DIR}")

# Future enhancement: pass these through command-line arguments or pipeline config.
FILE_PATHS = str(PROJECT_DIR / "data" / "data_group_*.csv")
OUTPUT_PATH = str(PROJECT_DIR / "output")

# Constants for data processing
MIN_TURBINE_ID = 1
MAX_TURBINE_ID = 500
MAX_WIND_SPEED_MS = 60.0
MAX_POWER_MW = 10.0
RAW_COLUMNS = ["timestamp", "turbine_id", "wind_speed", "wind_direction", "power_output"]

def create_spark_session():
    """Initializes and returns a Spark session."""
    spark = SparkSession.builder.appName("WindTurbinePipeline").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def get_turbine_schema() -> StructType:
    """Returns the structure of the data in the data_group_*.csv files"""
    return StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("turbine_id", IntegerType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("wind_direction", IntegerType(), True),
            StructField("power_output", DoubleType(), True),
        ]
    )

def ingest_data(spark: SparkSession, file_paths: str ):
    """Ingests raw CSV data for multiple turbine groups."""
    schema = get_turbine_schema()
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE") # Just letting everything be read initially to prevent failures
        .schema(schema)
        .csv(file_paths)
    )

def clean_data(df: DataFrame) -> DataFrame:
    """
    Cleans raw data by dropping rows with missing pk fields,
    filtering extreme outliers and invalid values.
    imputing missing measurements
    """

    # Drop rows where pk is null. Not chained to filter just for clarity of purpose atm.
    df_pk_cleaned = df.dropna(subset=["timestamp", "turbine_id"])
    
    # Filer out outlier rows with extreme or invalid values and cast timestamp to date for later aggregation
    df_filtered = (df_pk_cleaned.filter(F.col("turbine_id") > 0)
        .filter(F.col("wind_speed").isNull() | F.col("wind_speed").between(0.0, MAX_WIND_SPEED_MS))
        .filter(F.col("power_output").isNull() | F.col("power_output").between(0.0, MAX_POWER_MW))
        .filter(F.col("wind_direction").isNull() | F.col("wind_direction").between(0, 359))
        .withColumn("date", F.to_date("timestamp"))  # Convert timestamp to date for daily aggregation
    )

    return df_filtered

def impute_data(df: DataFrame) -> DataFrame:
    """Impute null data with median values for each turbine"""

    # Calculate median per turbine
    turbine_medians = df.groupBy("turbine_id").agg(
        F.percentile_approx("wind_speed", 0.5).alias("turbine_wind_speed_median"),
        F.percentile_approx("power_output", 0.5).alias("turbine_power_output_median"),
    )

    # Apply the median values to impute missing wind_speed and power_output values
    imputed = (
        df.join(turbine_medians, on="turbine_id", how="left")
        .withColumn(
            "wind_speed",
            F.coalesce(
                F.col("wind_speed"),
                F.col("turbine_wind_speed_median"),
            ),
        )
        .withColumn(
            "power_output",
            F.coalesce(
                F.col("power_output"),
                F.col("turbine_power_output_median"),
            ),
        )
        .withColumn(
            "wind_direction",
            F.coalesce(
                F.col("wind_direction"),
                F.lit(0),  # wind_direction is not imputed with median, but set to 0 if null as it's not used later.
            ),
        )
        .drop(
            "turbine_wind_speed_median",
            "turbine_power_output_median",
        )
    ) 

    return imputed

def calculate_summary_statistics(df: DataFrame) -> DataFrame:
    """
    Calculates daily summary statistics (min, max, average) for each turbine.
    """    
    summary_df = df.groupBy("turbine_id", "date").agg(
        F.min("power_output").alias("min_power"),
        F.max("power_output").alias("max_power"),
        F.avg("power_output").alias("avg_power"),
        F.count("power_output").alias("reading_count")
    )
    
    return summary_df

def identify_anomalies(summary_df: DataFrame) -> DataFrame:
    """
    Identifies turbines whose average daily output deviates beyond 2 standard deviations 
    from the fleet/group mean for that specific day.
    """

    # Not chained the identification of anomalies to the summary_df for clarity of purpose.
    window_spec = Window.partitionBy("date")
    
    stats_df = (summary_df.withColumn("fleet_mean", F.avg("avg_power").over(window_spec))
                         .withColumn("fleet_stddev", F.stddev("avg_power").over(window_spec))  
    )
    
    anomalies_df = stats_df.withColumn(
        "is_anomaly",
        F.abs(F.col("avg_power") - F.col("fleet_mean")) > (2 * F.col("fleet_stddev"))
    )
    
    return anomalies_df

def store_data(cleaned_df: DataFrame, summary_df: DataFrame, output_path: str):
    """
    Persists processed data and metrics to storage (e.g., Parquet/Delta format).
    """
    cleaned_df.write.mode("append").partitionBy("turbine_id").parquet(f"{output_path}/cleaned_data")
    summary_df.write.mode("append").parquet(f"{output_path}/summary_statistics")

def run_pipeline(file_paths, output_path):
    """Main execution flow for the pipeline."""
    spark = create_spark_session()
    
    # 1. Ingest
    raw_df = ingest_data(spark, file_paths)
    
    # 2. Clean
    # Cleans the data: The raw data contains missing values and outliers, which must be
    # removed or imputed.
    cleaned_df = clean_data(raw_df)
    imputed_df = impute_data(cleaned_df)

    # 3. Calculate Summary Statistics
    # Calculates summary statistics: For each turbine, calculate the minimum, maximum, and
    # average power output over a given time period (e.g., 24 hours).
    summary_df = calculate_summary_statistics(imputed_df)
    
    # 4. Identify Anomalies
    # Identifies anomalies: Identify any turbines that have significantly deviated from their
    # expected power output over the same time period. Anomalies can be defined as turbines
    # whose output is outside of 2 standard deviations from the mean.
    anomalies_df = identify_anomalies(summary_df)
    
    # 5. Store
    # Stores the processed data: Store the cleaned data and summary statistics in a database
    # for further analysis.
    store_data(imputed_df, anomalies_df, output_path)
    
    spark.stop()


def main() -> None:
    run_pipeline(FILE_PATHS, OUTPUT_PATH)


if __name__ == "__main__":
    main()







