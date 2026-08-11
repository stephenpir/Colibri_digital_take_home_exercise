
# Wind turbine performance pipeline

This proof of concept is a PySpark batch pipeline for daily wind-turbine measurement CSVs. It reads the three supplied turbine-group files, cleans and imputes measurements, calculates daily performance metrics, flags anomalous turbine-days, and writes the results as Parquet datasets.

## Project layout

```text
Colibri_digital_take_home_exercise/
├── data/                         # Input CSV files: data_group_*.csv
├── output/                       # Created by the pipeline; ignored by Git
├── python/
│   └── turbine_performance_pipeline.py
├── requirements.txt
└── README.md
```

## Setup and run

Create and activate a virtual environment from the project directory:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run the pipeline:

```bash
python python/turbine_performance_pipeline.py
```

The input and output locations are currently module constants in the pipeline as this is only a POC:
Making these command-line arguments or configuration values is a future enhancement.

```python
FILE_PATHS = "<project>/data/data_group_*.csv"
OUTPUT_PATH = "<project>/output"
```


## Pipeline design

1. **Ingest** — Spark reads all `data_group_*.csv` files using an explicit schema. CSV read mode is `PERMISSIVE`, so values that cannot be parsed into the expected type become null rather than failing the entire batch.
2. **Clean** — Rows with a missing timestamp or turbine ID are removed. The remaining rows must have a positive turbine ID, wind speed between 0 and 60 m/s, power output between 0 and 10 MW, and direction between 0 and 359 degrees. Null measurement fields are retained for imputation.
3. **Impute** — Spark calculates approximate median wind speed and power output for each turbine. These medians are joined back to the data and replace null values. Missing wind direction is set to `0` (North).
4. **Summarise** — For each turbine and calendar date, the pipeline calculates minimum, maximum, average, and count of power-output readings.
5. **Detect anomalies** — A turbine-day is flagged when its average power output differs from that day's fleet average by more than two standard deviations.
6. **Store** — The imputed measurements are written to `output/cleaned_data`, partitioned by `turbine_id`. The anomaly-enriched daily summaries are written to `output/summary_statistics`.

## Output fields

`summary_statistics` contains:

- `turbine_id`, `date`
- `min_power`, `max_power`, `avg_power`, `reading_count`
- `fleet_mean`, `fleet_stddev`, `is_anomaly`

## Assumptions and limitations
- The request for database output was a request for parquet and not a "True" db though it would be easy to modify as such.
- The date is derived directly from the input timestamp. The data is assumed to use the reporting timezone; UTC conversion is not currently applied.
- A turbine's median is calculated across the current input batch, not separately per day. If all values for a given turbine and measurement are null, its median also remains null.
- The supplied file groups are read as one batch, and no explicit duplicate-record handling is implemented.
- Writes use `append` mode. Re-running the same input will append duplicate output records; a production implementation should use an idempotent merge or partition replacement strategy.
- Parquet is used as a local, queryable proof-of-concept storage format. In production, the storage boundary could be replaced with Delta/Iceberg tables or a data warehouse sink.
- The physical bounds and anomaly rule are deliberately simple; production thresholds should be based on turbine model specifications and an expected power curve.


## Further enhancements
1. Script to accept command-line arguments to allow flexibility in the processing of different files and configurations.
2. For incremental processing, a date filter could be added to only process new dates as opposed to the entire dataset.  
3. Compression and chaining of transformations can be used for more complex data processing tasks for performance, but has been left out for simplicity and readability. 
4. Error handling and logging should be implemented to capture any issues during the pipeline execution.
5. Standardised test harnesses should be created to ensure its robustness and reliability.

## Scaleability
The pipeline easily scales horizontally across a cluster when processing months or years of multi-turbine logs simultaneously. Partitioning data by turbine_id and date ensures efficient read/write operations and reduces the load on individual nodes. The partitioning strategy also prevents any repartitioning of data across nodes, thus maintaining optimal resource utilization.
The use of Spark's window functions allows for complex calculations such as calculating the average wind speed per turbine over a given period, which is crucial for understanding turbine performance trends. This approach enables the pipeline to handle large datasets efficiently without compromising performance.