from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, round

spark = SparkSession.builder.master('spark://spark-master:7077').appName("AntipovAV_HousePrice").getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/mydb"
con_props = {
    "user": "arantipov",
    "password": "mypassword",
    "driver": "org.postgresql.Driver"
}

df = spark.read.jdbc(url=db_url, table="houseprices", properties=con_props)

query_result = df.filter(df["property_type"].isin('House', 'Flat')).\
                groupBy('city', 'location', 'bedrooms').agg(round(mean("price"), 2).alias("AVG")).\
                orderBy(["city", "location", "bedrooms"], ascending=[True, True, True])

query_result.show()

spark.stop()
