from pyspark.rdd import RDD
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import to_date, transform, when

OFAC_DIR = "data/ofac/"
GBR_DIR = "data/gbr/"


def convert_date(date: str):
    """
    Converts string formatted date to date fomat
    if string formatted date is in one of the formats: ["dd/MM/yyyy", "dd MMM yyyy", "yyyy"]
    or else marking it as Invalid
    """
    return when(to_date(date, "dd/MM/yyyy").isNotNull(), to_date(date, "dd/MM/yyyy")) \
        .when(to_date(date, "dd MMM yyyy").isNotNull(), to_date(date, "dd MMM yyyy")) \
        .when(to_date(date, "yyyy").isNotNull(), to_date(date, "yyyy")) \
        .otherwise("Invalid")


def standardize_data(spark: SparkSession, df: RDD) -> DataFrame:
    """
    Standardize the raw data into a desired format(columns, conversions) to handle
    cleansing of raw data or removing columns that we don't need in future
    or any data conversions so that data is more reliable and accurate.
    """
    df = df.withColumn("date_of_births_transformed",
                       transform(df.reported_dates_of_birth, convert_date))
    df.createOrReplaceTempView("standardized_sanctions")
    standardize_data_sql = f"""
                        SELECT
                        CAST(id AS INT),
                        trim(name) AS name,
                        type,
                        aliases.value AS aliases,
                        id_numbers.value AS id_numbers,
                        date_of_births_transformed AS date_of_births
                        FROM
                        standardized_sanctions
                        """
    return spark.sql(standardize_data_sql)


def matches(spark: SparkSession, ofac_df: DataFrame, gbr_df: DataFrame) -> DataFrame:
    """
    Returns common sanctioned entities if it matches one of the following:
    1. Name
    2. Alaises
    3. id_numbers
    4. Date of birth
    """
    ofac_df.createOrReplaceTempView("ofac")
    gbr_df.createOrReplaceTempView("gbr")

    result_sql = """
            SELECT
            ofac.id AS ofac_id,
            gbr.id AS grb_id,
            ofac.type AS Type,
            IFNULL(ofac.name, gbr.name) AS Name,
            CASE WHEN lower(ofac.name) = lower(gbr.name) THEN True ELSE False END AS name_matches,
            arrays_overlap(ofac.date_of_births, gbr.date_of_births) AS atleast_1_date_of_births_matches,
            arrays_overlap(ofac.aliases, gbr.aliases) AS atleast_1_aliases_matches,
            arrays_overlap(ofac.id_numbers, gbr.id_numbers) AS atleast_1_id_numbers_matches
            FROM
            ofac
            INNER JOIN gbr ON ofac.type = gbr.type AND
            (
            lower(ofac.name) = lower(gbr.name)
            OR arrays_overlap(ofac.aliases, gbr.aliases) = True
            OR arrays_overlap(ofac.id_numbers, gbr.id_numbers) = True
            OR (
                arrays_overlap(ofac.date_of_births, gbr.date_of_births) = True)
                AND
                (
                    lower(ofac.name) = lower(gbr.name)
                    OR arrays_overlap(ofac.id_numbers, gbr.id_numbers) = True
                    OR arrays_overlap(ofac.aliases, gbr.aliases) = True
                )
            )
            """
    return spark.sql(result_sql)


def main():
    spark = (
        SparkSession.builder.appName("Sanctioned Entities")
        .master("local")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    ofac_df = spark.read \
        .format("json") \
        .option("compression", "gzip") \
        .load(OFAC_DIR)

    gbr_df = spark.read \
        .format("json") \
        .option("compression", "gzip") \
        .load(GBR_DIR)

    # print(gbr_df.schema, ofac_df.schema)
    result = matches(spark, standardize_data(spark, ofac_df),
                     standardize_data(spark, gbr_df))
    result.cache()
    print(f"Total {result.count()} sanctioned entities have matched")
    result.coalesce(1).write.option("header", True).mode(
        "overwrite").csv("results/")


if __name__ == "__main__":
    main()
