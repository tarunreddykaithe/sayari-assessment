from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

ofac_gbr_schema = StructType([
    StructField('addresses',
                ArrayType(StructType([
                    StructField('country', StringType(), True),
                    StructField('postal_code', StringType(), True),
                    StructField('value', StringType(
                    ), True)]), True), True),
    StructField('aliases',
                ArrayType(
                    StructType([
                        StructField('type', StringType(), True),
                        StructField('value', StringType(), True)]), True), True),
    StructField('id', LongType(), True),
    StructField('id_numbers', ArrayType(
        StructType([StructField('comment', StringType(), True),
                    StructField('value', StringType(), True)]), True), True),
    StructField('name', StringType(), True),
    StructField('nationality', ArrayType(
        StringType(), True), True),
    StructField('place_of_birth', StringType(), True),
    StructField('position', StringType(), True),
    StructField('reported_dates_of_birth',
                ArrayType(StringType(), True), True),
    StructField('type', StringType(), True)
])
