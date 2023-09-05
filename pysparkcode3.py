import pyspark
from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DoubleType
from datetime import datetime
from pyspark.sql import functions as fc
import argparse
import os 


config = {
	"bucket" : "gs://dataproc-staging-us-central1-285051978607-oi1dpnsd",
	"target_table" : "e-observer-396621.bgdataset.bgtable5",
	"numPartitions" : 4,
	"partitioncolm" :"in_date",
	"target_partition_size": None
}




def load_file_schema():
	schema = StructType([StructField('S_no',IntegerType(), True),\
				StructField('Organization_Id',StringType(), True),\
				StructField('Name',IntegerType(), True),\
				StructField('Website',StringType(), True),\
				StructField('Country',StringType(), True),\
				StructField('Description',StringType(), False),\
				StructField('Founded',StringType(), False),\
				StructField('Industry',StringType(), False),\
				StructField('Number_of_employees',StringType(), False)])

	return schema

def get_bucket_list_items():
	gcs_client = storage.Client()
	# bucket = gcs_client.bucket('dataproc-staging-us-central1-285051978607-oi1dpnsd')
	bucket_name = config['bucket']
	bucket_name = bucket_name.replace("gs://","")
	bucket = gcs_client.bucket(bucket_name)
	print("#"*40)
	print(list(bucket.list_blobs()))
	print("#"*40)


def get_spark():
	spark = SparkSession.builder.appName("load_csv_bigquery")\
								.config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.2")\
								.getOrCreate()
	spark.conf.set("spark.hadoop.avro.compression","snappy")
	spark.conf.set("spark.sql.parquet.compression.codec","snappy")
	spark.conf.set("spark.sql.shuffle.partitions","4")
	# spark.conf.set("spark.default.parallelism","") # default core
	spark.conf.set("spark.sql.files.maxPartitionBytes","151217724") # default 128mb
	# .config('spark.hadoop.fs.gs.impl','com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem').config('spark.hadoop.fs.gs.auth.service.account.enable','false')
	bucket_name = config['bucket']
	spark.conf.set('temporaryGcsBucket',bucket_name)
	spark.conf.set("spark.datasource.bigquery.intermediateFormat", "orc")

	return spark


def load_data(schema,file_format=None,file_location=None,load_partition=False,schema=False:
	spark = get_spark()
	bucket_name = config['bucket']
	if file_location :
		file_location =str(file_location)
		file_location = os.path.join(bucket_name,file_location)
	if load_partition and schema :
		numPartitions = int(config['numPartitions'])
		df_in = spark.read.option("numPartitions",numPartitions)\
							.format(file_format)\
							.schema(schema)\
							.option("header", True)\
							.load(file_location)
	elif schema:
		df_in = spark.read.format(file_format)\
							.schema(schema)\
							.option("header", True)\
							.option('inferSchema',False)\
							.load(file_location)
	else:
		df_in = spark.read.format(file_format)\
							.option("header", True)\
							.option('inferSchema',False)\
							.load(file_location)
		
			
	return df_in

def main(file_format,file_name,load_partition,schema):
	get_bucket_list_items()
	schema = load_file_schema()
	input_df = load_data(schema,file_format,file_name,load_partition,schema)
	input_df.show(5)
	partition_by_date=datetime.today().strftime('%Y%m%d')
	input_df = input_df.withColumn("in_date",fc.current_date())
	input_df.show(5)
	# input_df = input_df.filter("Founded > 2003")
	write_to_bigquery(input_df)

	


def write_to_bigquery(output_df):
	bucket_name = config['bucket']
	partitioncolm=config["partitioncolm"]
	target_table_name = config["target_table"]

	if config['target_partition_size'] is not None:
		repartition_size = config['target_partition_size']
		output_df = output_df.repartion(int(repartition_size))

	output_df.write.format('bigquery')\
				.option("table",target_table_name)\
				.option("temporaryGcsBucket",bucket_name)\
				.option("partitionField",str(partitioncolm))\
				.option("createDisposition","CREATE_IF_NEEDED")\
				.option("writeDisposition","WRITE_TRUNCATE")\
				.mode("overwrite")\
				.save()
			


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-file_format",type=str,default=None)
	parser.add_argument("-file_name",type=str,default=None)
	parser.add_argument("-load_partition",
						action="store_true",
						help="""this should be passed when input file 
								has to be partitioned for parallel processing while loading""")
	parser.add_argument("-schema",
						action="store_true",
						help="""this is to take predefined schema or not""")
	args= parser.parse_args()
	file_format = args.file_format if args.file_format else None
	file_name = args.file_name if args.file_name else ""
	load_partition = args.load_partition if args.load_partition else False
	main(file_format,file_name,load_partition,schema)

 
