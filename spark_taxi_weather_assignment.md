# Project Assignment: NYC Taxi × Weather Analytics with Apache Spark

## Goal

Build a batch analytics pipeline that answers a real question: **does weather affect taxi rides in New York?** You will load several months of real trip data, clean it, aggregate it, enrich it with data from a public weather API and a reference table, and present your findings with numbers and charts.

**Estimated effort:** 1–1.5 weeks part-time.
**Environment:** your laptop. `pip install pyspark requests matplotlib` plus Java 11 or 17 (check with `java -version`). Run Spark in local mode (`master("local[*]")`). No Docker, no clusters, no API keys, no registration anywhere.

## Data sources (all free and keyless)

1. **NYC Yellow Taxi trip records** — official Parquet files, ~3 million rows / ~50 MB per month. Use January–March 2024. Direct download pattern:
   `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet` (and `-02`, `-03`).
   Landing page with docs and data dictionary: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
2. **Taxi Zone Lookup Table** (CSV) — maps `PULocationID` / `DOLocationID` to borough and zone name. Linked on the same landing page (`https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`).
3. **Open-Meteo historical weather API** — hourly precipitation and temperature for NYC, no key required:
   `https://archive-api.open-meteo.com/v1/archive?latitude=40.71&longitude=-74.01&start_date=2024-01-01&end_date=2024-03-31&hourly=temperature_2m,precipitation&timezone=America/New_York`

Note: Spark reads local files, not HTTPS URLs — download the Parquet/CSV files first (curl, wget, or Python `requests`).

---

## Part 1 — Load and clean

1. Load all three months into a single DataFrame. Print the schema and the total row count.
2. The data is genuinely dirty. Apply and **document** cleaning rules, at minimum dropping rows where: pickup/dropoff timestamps fall outside Jan–Mar 2024 (yes, there are trips "from" 2001 and 2098 in there); dropoff is before pickup; duration is over 6 hours; `trip_distance` ≤ 0 or > 100 miles; `fare_amount` ≤ 0 or `total_amount` ≤ 0.
3. Add derived columns: `duration_min`, `pickup_date`, `pickup_hour` (timestamp truncated to the hour), `speed_mph`, and `tip_pct` = tip / fare × 100 (guard against division by zero).
4. Report row counts before and after cleaning, and roughly how many rows each rule removed.
5. Save the cleaned data as Parquet, **partitioned by `pickup_date`**.

## Part 2 — Core analytics

Using the Spark DataFrame API, compute:

1. Trips and total revenue per day, per hour of day, and per day of week.
2. Average fare, distance, duration, and speed per day.
3. Tip analysis — **restricted to credit-card payments only** (`payment_type = 1`). Explain in your README why including cash rides would silently break this analysis. (Hint: look at `tip_amount` for cash trips.)

## Part 3 — Weather enrichment (API)

1. Call the Open-Meteo archive API for the same date range. The response contains parallel JSON arrays (`hourly.time`, `hourly.temperature_2m`, `hourly.precipitation`) — convert them into a Spark DataFrame with one row per hour.
2. Join it with your hourly trip aggregates on the hour. **Careful with timezones:** pickup timestamps are NYC local time, so request the API with `timezone=America/New_York` and verify both sides actually line up before joining.
3. Answer each of these with concrete numbers **and** a chart:
   - Do people tip more when it rains? (Define what counts as a "rainy hour" — e.g. precipitation above some threshold — and justify your choice.)
   - Are trips slower (average speed) in the rain?
   - Does hourly trip volume change with rain? With temperature (bucket temperatures into ranges)?

## Part 4 — Zone enrichment (broadcast join)

1. Join the zone lookup table using a **broadcast join**.
2. Report the top 10 pickup zones and top 10 dropoff zones by trip count.
3. Compare average tip percentage across boroughs.

## Part 5 — Window functions

Implement at least two of the following:

1. Day-over-day percentage change in daily trip count (`lag`).
2. 7-day rolling average of daily trips (`rowsBetween`).
3. Top 3 busiest hours within each day (`dense_rank`).

## Part 6 — Results and write-up

1. Write your final daily summary table (trips, revenue, tips, speed, joined with weather) to Parquet or CSV.
2. Produce at least 3 charts. matplotlib/seaborn are fine; `toPandas()` is allowed **only** on aggregated results (roughly under 10k rows), never on raw trip data.
3. Write a findings section: 5–10 sentences with concrete numbers answering the weather questions.

---

## Rules

All heavy processing must use the Spark DataFrame API. No pandas on raw trips, no `collect()` on anything except small aggregates, no Python UDFs where a built-in function exists.

## Written questions (short answers in the README)

1. When Spark "reads" the three Parquet files, at what point is data actually loaded? Explain lazy evaluation and the difference between transformations and actions using examples from your own code.
2. Why is a broadcast join the right choice for the zone table? What would Spark do with a regular join?
3. Point out one narrow and one wide transformation in your pipeline. Where do shuffles happen?
4. Why is the cleaned output partitioned by date, and in which queries does that pay off?

## Deliverables

1. A Git repository (or a notebook + files) with the code and a `README.md`: setup and run instructions, cleaning rules, findings, and the written answers.
2. The code must fully regenerate all outputs from the raw downloads.
3. The charts (embedded in the README or the notebook).

## Evaluation

| Criterion | Weight |
|---|---|
| Data cleaning correctness and documentation | 20% |
| Correct Spark usage (DataFrame API, broadcast join, windows, no pandas shortcuts) | 30% |
| Weather analysis and quality of findings | 25% |
| Code quality and README | 15% |
| Written answers | 10% |

## Stretch goals (optional, extra credit)

- Scale up to the full year 2024 (~40M rows). Show one query with and without partition pruning and compare runtimes; include the `.explain()` output.
- Add US public holidays from the Nager.Date API (free, keyless) and check how holidays affect trip volume.
- Compare yellow vs green taxis on the same metrics.
- Zone-level rain sensitivity: which zones gain or lose the most trips when it rains?

## Suggested milestones

- **Days 1–2:** setup, download, load, clean, save partitioned Parquet.
- **Days 3–4:** core aggregations and zone analysis.
- **Day 5:** weather API, join, weather questions.
- **Days 6–7:** window functions, charts, README, findings.
