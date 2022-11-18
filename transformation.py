from pyspark.sql import SparkSession, functions as F, Window as W
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField,ArrayType,LongType,StringType,DoubleType,IntegerType
from pyspark.sql.functions import split, explode,col
from pyspark.sql.functions import lit


spark = SparkSession.builder.appName('earthquake_api').getOrCreate()

earthquake_df = spark.read\
    .format('json')\
    .option('multiLine', 'true')\
    .load('data.json')

earthquake_df.printSchema()

earthquake_df = earthquake_df.drop("count").drop("errorCode").drop("errors").drop("friendlyError").drop("httpStatus").drop("noun").drop( "result").drop("verb")

earthquake_df = earthquake_df.withColumn('data', F.explode('data'))

earthquake_df.printSchema()

earthquake_df = earthquake_df\
            .withColumn('id', F.col('data').getItem('id'))\
            .withColumn('magnitude', F.col('data').getItem('magnitude').cast('int'))\
            .withColumn('type', F.col('data').getItem('type'))\
            .withColumn('title', F.col('data').getItem('title'))\
    	    .withColumn('date', F.col('data').getItem('date'))\
    	    .withColumn('time', F.col('data').getItem('time'))\
    	    .withColumn('updated', F.col('data').getItem('updated').cast('int'))\
    	    .withColumn('status', F.col('data').getItem('status'))\
    	    .withColumn('latitude', F.col('data').getItem('latitude').cast('int'))\
    	    .withColumn('longitude', F.col('data').getItem('longitude').cast('int'))\
    	    .withColumn('place', F.col('data').getItem('place'))\
    	    .withColumn('location', F.col('data').getItem('location'))\
    	    .withColumn('continent', F.col('data').getItem('continent'))\
    	    .withColumn('country', F.col('data').getItem('country'))\

earthquake_df = earthquake_df\
                .select('id','magnitude', 'type', 'title', 'date', 'time', 'updated', 'status', 'latitude','longitude', 'place','location','continent','country')

# Transformation 
# 1. Find the average of magnitude of earthquake.

average_magnitude_df = earthquake_df.agg({'magnitude':'avg'})
average_magnitude_df.select(average_magnitude_df['avg(magnitude)'])
average_magnitude_df.show()


average_magnitude_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/kafka', driver= 'org.postgresql.Driver',
                                                    dbtable='earthquake_table_1', user='memosha',password='1234').mode('overwrite').save()


# 2. Highest magnitude for each country in the month of November in 2022.

highest_magnitude_november_df = earthquake_df\
                    .groupBy(F.year('date').alias('year'), F.month('date').alias('month'), 'location')\
                    .agg(F.max('magnitude').alias('highest_magnitude'))\
                    .filter('month == 11')\
                    .filter('year == 2022')\
                    .orderBy('location', 'year')

highest_magnitude_november_df.show()


highest_magnitude_november_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/kafka', driver= 'org.postgresql.Driver',
dbtable='earthquake_table_2', user='memosha',password='1234').mode('overwrite').save()

# 3. Find the status of earthquake on the basis of each place.

status_place_df = earthquake_df.withColumn('status',F.collect_set('status').over(W.partitionBy('place'))).select('place', 'status',lit(2022).alias("dates")).distinct()
status_place_df.show()

status_place_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/kafka', driver= 'org.postgresql.Driver',
dbtable='earthquake_table_3', user='memosha',password='1234').mode('overwrite').save()

# # 4. List all the country whose magnitude is less than 5.

magnitude_less_than_5_df = earthquake_df.filter(earthquake_df["magnitude"] < 5).select('country', 'magnitude').distinct()
magnitude_less_than_5_df.show()

magnitude_less_than_5_df.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/kafka', driver= 'org.postgresql.Driver',
dbtable='earthquake_table_4', user='memosha',password='1234').mode('overwrite').save()


# write to json files
# {‘split’, ‘records’, ‘index’, ‘columns’, ‘values’, ‘table’}.

average_magnitude_df.toPandas().to_json('data/average_magnitude.json', orient='records') # orient 'records' is necessary for our data format
highest_magnitude_november_df.toPandas().to_json('data/highest_magnitude_november.json', orient='records')
status_place_df.toPandas().to_json('data/status_place.json', orient='records')

# spark-submit --driver-class-path /usr/lib/jvm/java-11-openjdk-amd64/lib/postgresql-42.5.0.jar transformation.py