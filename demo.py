from pyspark.sql import SparkSession
import datetime

spark = SparkSession \
    .builder \
    .appName('airflow_demo') \
    .getOrCreate()

sales = spark.read \
     .format('csv') \
     .options(header='true').load("/home/anakha/Downloads/Data.csv")
sales.show()

products = sales.select("product_id","product_name","price").dropDuplicates(["product_id"])
products.show()

# time = datetime.datetime.now().minute
# path = "/home/anakha/Desktop/test/output/" + str(time)
# products.write.csv(path,header="true")
