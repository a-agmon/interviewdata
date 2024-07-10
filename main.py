import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

iceberg_runtime = "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.5.2"



def get_spark_conf():
    conf = SparkConf()
    conf.setAppName("icebergtest")
    conf.set("spark.jars.packages", f"{iceberg_runtime}")
    conf.set("spark.sql.execution.pyarrow.enabled", "true")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set('spark.sql.catalog.icebergcat', 'org.apache.iceberg.spark.SparkCatalog')
    conf.set('spark.sql.catalog.icebergcat.type', 'hadoop')
    conf.set('spark.sql.catalog.icebergcat.warehouse', 'iceberg-warehouse')
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    return conf


if __name__ == '__main__':
    print('starting pyspark')
    print(pyspark.__version__)
    '''
    instructions: 
    1. This URL contains JSON data of shows: https://api.tvmaze.com/shows
    2. Read this JSON data into a data frame, 
    3. select only the columns: name, status, type
    4. Write the table as an Iceberg table
    5. Iceberg tables also have metadata tables, please select and show the files that are part of the table
    6. the get_spark_conf() runs the configuration required to run Iceberg within pySpark. Use it.
    7. Note the versions of Spark that are used here. 
    The version of Spark you have installed and pointed to in SPARK_HOME must be the same version of pyspark
    the loaded libraries here assume Spark 3.4.0
        
    '''



    
