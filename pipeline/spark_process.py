"""Build the website datasets from Hadoop MapReduce output and HDFS source files."""
import re
import sys

from pyspark.sql import SparkSession, functions as F

from pipeline.airport_aliases import ALIAS


HDFS_ROOT = sys.argv[1].rstrip("/")
spark = (
    SparkSession.builder.appName("AeroPulse")
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
    .getOrCreate()
)


def mapped_values(path, key_count, value_count):
    rows = spark.read.text(path).withColumn(
        "_fields", F.split(F.col("value"), "\t", 2)
    ).select(
        F.split(F.col("_fields").getItem(0), "\x1f").alias("key"),
        F.split(F.col("_fields").getItem(1), r"\|").alias("values"),
    )
    if rows.where(
        (F.size("key") != key_count) | (F.size("values") != value_count)
    ).limit(1).count():
        raise ValueError(f"Malformed Hadoop MapReduce output: {path}")
    return rows


def pct(column):
    return F.regexp_replace(F.trim(F.col(column).cast("string")), "%", "").cast(
        "double"
    )


def write_hdfs_csv(frame, name):
    destination = f"{HDFS_ROOT}/processed/{name}"
    frame.coalesce(1).write.mode("overwrite").option("header", True).option(
        "nullValue", ""
    ).csv(destination)
    print(f"Spark wrote {destination}", flush=True)


try:
    carrier_rows = mapped_values(f"{HDFS_ROOT}/mapreduce/carrier", 3, 4)
    carrier_key = F.col("key")
    carrier_values = F.col("values")
    carrier = carrier_rows.select(
        carrier_key.getItem(0).alias("airline"),
        carrier_key.getItem(1).cast("int").alias("year"),
        carrier_key.getItem(2).cast("int").alias("month_no"),
        carrier_values.getItem(0).cast("long").alias("pax"),
        carrier_values.getItem(1).cast("long").alias("flights"),
        carrier_values.getItem(2).cast("double").alias("_plf_sum"),
        carrier_values.getItem(3).cast("long").alias("_plf_count"),
    ).select(
        "airline",
        "year",
        "month_no",
        "pax",
        "flights",
        F.when(
            F.col("_plf_count") > 0,
            F.col("_plf_sum") / F.col("_plf_count"),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("plf"),
        F.concat(
            F.col("year").cast("string"),
            F.lit("-"),
            F.lpad(F.col("month_no").cast("string"), 2, "0"),
        ).alias("month"),
    )

    city_rows = mapped_values(f"{HDFS_ROOT}/mapreduce/city", 4, 2)
    city_key = F.col("key")
    city_values = F.col("values")
    city_aliases = F.create_map(
        *[
            item
            for alias, canonical in ALIAS.items()
            for item in (F.lit(alias), F.lit(canonical))
        ]
    )
    city_pairs = city_rows.select(
        city_key.getItem(0).cast("int").alias("year"),
        city_key.getItem(1).cast("int").alias("month_no"),
        city_key.getItem(2).alias("_a"),
        city_key.getItem(3).alias("_b"),
        city_values.getItem(0).cast("double").alias("_pax"),
        city_values.getItem(1).cast("double").alias("_freight"),
    )

    def canonical_city(column):
        cleaned = F.upper(F.trim(F.regexp_replace(F.col(column), r"\s*\(.*\)", "")))
        return F.coalesce(city_aliases[cleaned], cleaned)

    city_pairs = city_pairs.withColumn("_a", canonical_city("_a")).withColumn(
        "_b", canonical_city("_b")
    ).where(F.col("_a") != F.col("_b"))
    pairs = (
        city_pairs.select(
            "year",
            "month_no",
            F.least("_a", "_b").alias("x"),
            F.greatest("_a", "_b").alias("y"),
            "_pax",
            "_freight",
        )
        .groupBy("year", "month_no", "x", "y")
        .agg(
            F.sum("_pax").alias("pax"),
            F.sum("_freight").alias("freight"),
        )
        .where(F.col("pax") > 0)
        .withColumn(
            "month",
            F.concat(
                F.col("year").cast("string"),
                F.lit("-"),
                F.lpad(F.col("month_no").cast("string"), 2, "0"),
            ),
        )
        .select("year", "month_no", "x", "y", "pax", "freight", "month")
    )

    coordinates = spark.read.option("header", True).option(
        "inferSchema", True
    ).csv(f"{HDFS_ROOT}/reference/cities.csv")
    cities = (
        pairs.select(F.col("x").alias("city"))
        .unionByName(pairs.select(F.col("y").alias("city")))
        .distinct()
        .join(coordinates.select("city", "lat", "lon"), "city", "left")
        .select("city", "lat", "lon")
    )

    daily_source = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{HDFS_ROOT}/raw/daily.csv")
    )
    otp_columns = [
        re.fullmatch(r"On Time Performance \((.*)\)", column)
        for column in daily_source.columns
    ]
    airline_names = {
        "Air Asia": "AirAsia India",
        "Indigo": "IndiGo",
        "Spicejet": "SpiceJet",
        "GoAir": "Go First",
    }
    otp_frames = []
    for match in otp_columns:
        if match is None:
            continue
        source_name = match.group(1)
        plf_name = f"Passenger Load Factor ({source_name})"
        plf_value = (
            pct(plf_name)
            if plf_name in daily_source.columns
            else F.lit(None).cast("double")
        )
        otp_frames.append(
            daily_source.select(
                F.to_date(F.col("Date").cast("string")).alias("_date"),
                F.lit(airline_names.get(source_name, source_name)).alias("airline"),
                pct(match.group(0)).alias("otp"),
                plf_value.alias("plf"),
            ).where(F.col("otp").isNotNull())
        )
    if not otp_frames:
        raise ValueError("The daily source file contains no on-time performance columns")
    otp_daily = otp_frames[0]
    for frame in otp_frames[1:]:
        otp_daily = otp_daily.unionByName(frame)
    otp = (
        otp_daily.withColumn("month", F.date_format("_date", "yyyy-MM"))
        .groupBy("month", "airline")
        .agg(F.avg("otp").alias("otp"), F.avg("plf").alias("plf"))
        .select("month", "airline", "otp", "plf")
    )

    daily = (
        daily_source.select(
            F.to_date(F.col("Date").cast("string")).alias("date"),
            F.col("Domestic (Departure Flights)")
            .cast("double")
            .alias("dep_flights"),
            (
                F.col("Domestic (Arriving Pax)").cast("double")
                + F.col("Domestic (Departing Pax)").cast("double")
            ).alias("pax"),
        )
        .where(F.col("dep_flights").isNotNull())
    )

    international_source = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(f"{HDFS_ROOT}/raw/intl_country.csv")
    )
    international = international_source.select(
        (F.col("Year").cast("int") + 2000).alias("year"),
        F.col("Quarter").alias("quarter"),
        F.col("Country").alias("country"),
        (
            F.col("PaxToIndia").cast("double")
            + F.col("PaxFromIndia").cast("double")
        ).alias("pax"),
    )

    for name, frame, sort_columns in (
        ("carrier", carrier, ["year", "month_no", "airline"]),
        ("pairs", pairs, ["year", "month_no", "x", "y"]),
        ("cities", cities, ["city"]),
        ("otp", otp, ["month", "airline"]),
        ("daily", daily, ["date"]),
        ("intl", international, ["year", "quarter", "country"]),
    ):
        write_hdfs_csv(frame.orderBy(*sort_columns), name)
finally:
    spark.stop()
