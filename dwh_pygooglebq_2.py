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
	"target_table" : "e-observer-396621.bgdataset.bgtable10",
	"numPartitions" : 4,
	"partitioncolm" :"in_date",
	"target_partition_size": None,
	"concat_field1":"PERIOD_DIM",
	"concat_field2":"REP_ITEM_DIM",
	"struct_field_2":"VALUE",
	"remove_list_of_items":['PERIOD_DIM','REP_ITEM_DIM','VALUE']
}




def load_file_schema():
	schema = StructType([StructField('TRNID',IntegerType(), True),\
				StructField('REFNR',StringType(), True),\
				StructField('CONTEXT',IntegerType(), True),\
				StructField('PARTID',StringType(), True),\
				StructField('G_PRODUCT_DIM',StringType(), True),\
				StructField('MAISKEY_DIM',StringType(), False),\
				StructField('PROPOSITION_DIM',StringType(), False),\
				StructField('ORG_UNIT_DIM',StringType(), False),\
				StructField('LEGAL_ENTITY_DIM',StringType(), False),\
				StructField('VAL_TYPE_DIM',StringType(), False),\
				StructField('SOURCE_DIM',StringType(), False),\
				StructField('DIS_CH_DIM',StringType(), False),\
				StructField('BU_UNIT_DIM',StringType(), False),\
				StructField('REF_DATE_DIM',StringType(), False),\
				StructField('GB_ID',StringType(), False),\
				StructField('PRICEMODEL_ID',StringType(), False),\
				StructField('CD_PRODUCT_ID',StringType(), False),\
				StructField('FINOBJTYPE_ID',StringType(), False),\
				StructField('CONDTYPE_ID',StringType(), False),\
				StructField('STARGETGROUP_ID',StringType(), False),\
				StructField('NACECODE_ID',StringType(), False),\
				StructField('GROUP_ID',StringType(), False),\
				StructField('LIFECYCLE_ID',StringType(), False),\
				StructField('SEC_ID',StringType(), False),\
				StructField('CRF_ID',StringType(), False),\
				StructField('DPD_ID',StringType(), False),\
				StructField('HOMPORT_ID',StringType(), False),\
				StructField('B2APP_ID',StringType(), False),\
				StructField('CLSEG_ID',StringType(), False),\
				StructField('AMMETH_ID',StringType(), False),\
				StructField('INSTMETH_ID',StringType(), False),\
				StructField('CLDOM_ID',StringType(), False),\
				StructField('INTEREST_PERIOD_ID',StringType(), False),\
				StructField('REF_DATE_YEAR',StringType(), False),\
				StructField('REF_DATE_MONTH',StringType(), False),\
				StructField('PERIOD_DIM',StringType(), False),\
				StructField('REP_ITEM_DIM',StringType(), False),\
				StructField('VALUE',StringType(), False),\
				StructField('WEKN',StringType(), False)])

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


def load_data(schema=False,file_format=None,file_location=None,load_partition=False):
	spark = get_spark()
	bucket_name = config['bucket']
	if file_location :
		file_location =str(file_location)
		file_location = os.path.join(bucket_name,file_location)
	if load_partition and schema:
		schema = load_file_schema()
		numPartitions = int(config['numPartitions'])
		df_in = spark.read.option("numPartitions",numPartitions)\
							.format(file_format)\
							.schema(schema)\
							.option("header", True)\
							.load(file_location)
	elif schema:
		schema = load_file_schema()
		df_in = spark.read.format(file_format)\
							.schema(schema)\
							.option("header", True)\
							.option('inferSchema',False)\
							.load(file_location)
	elif "json" in file_format :
		df_in = spark.read.format(file_format)\
							.option("multiline",True)\
							.option("header", True)\
							.option('inferSchema',False)\
							.load(file_location)
	else:
		df_in = spark.read.format(file_format)\
							.option("header", True)\
							.option('inferSchema',False)\
							.option('delimiter',";")\
							.load(file_location)

			
	return df_in

def main(file_format,file_name,load_partition,schema):
	get_bucket_list_items()
	input_df = load_data(schema,file_format,file_name,load_partition)
	input_df.show(5)
	partition_by_date=datetime.today().strftime('%Y%m%d')
	# input_df = input_df.withColumn("in_date",fc.current_date())
	# input_df = input_df.withColumn("concat_column",fc.struct(fc.concat(fc.col(config['concat_field1']),fc.col(config['concat_field2'])).alias("concat_field"),config['struct_field_2']))
	column_names = input_df.columns 
	for i in config['remove_list_of_items']:
		column_names.remove(i)

	input_df = input_df.withColumn("concat_column",fc.concat(fc.col(config['concat_field1']),fc.col(config['concat_field2'])).alias("concat_field"))
	input_df = input_df.groupBy(*column_names).agg(fc.collect_list("concat_column").alias("list_of_period_dim"),fc.collect_list("VALUE").alias("list_of_value"))
	input_df = input_df.withColumn("list_of_period_dim" ,fc.struct(fc.col('list_of_period_dim'),fc.col('list_of_value')))
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
				.option("createDisposition","CREATE_IF_NEEDED")\
				.option("writeDisposition","WRITE_TRUNCATE")\
				.mode("overwrite")\
				.save()
			



# if __name__ == "__main__":
# 	# parser = argparse.argumentparser()
# 	# parser.add_argument("-file_format",type=str,default=none)
# 	# parser.add_argument("-file_name",type=str,default=none)
# 	# parser.add_argument("-load_partition",
# 	# 					action="store_true",
# 	# 					help="""this should be passed when input file 
# 	# 							has to be partitioned for parallel processing while loading""")
# 	# parser.add_argument("-schema",
# 	# 					action="store_true",
# 	# 					help="""this is to take predefined schema or not """)
# 	# args= parser.parse_args()
# 	# file_format = args.file_format if args.file_format else none
# 	# file_name = args.file_name if args.file_name else ""
# 	# load_partition = args.load_partition if args.load_partition else false
# 	file_format = "csv"
# 	file_name = "dwh.gmars.read.rds"
# 	load_partition = True
# 	schema = False
# 	main(file_format,file_name,load_partition,schema)

 

if __name__ == "__main__":
	parser = argparse.argumentparser()
	parser.add_argument("-file_format",type=str,default=none)
	parser.add_argument("-file_name",type=str,default=none)
	parser.add_argument("-load_partition",
						action="store_true",
						help="""this should be passed when input file 
								has to be partitioned for parallel processing while loading""")
	parser.add_argument("-schema",
						action="store_true",
						help="""this is to take predefined schema or not """)
	args= parser.parse_args()
	file_format = args.file_format if args.file_format else None
	file_name = args.file_name if args.file_name else ""
	load_partition = args.load_partition if args.load_partition else False
	schema = args.schema if args.schema else False
	main(file_format,file_name,load_partition,schema)
 