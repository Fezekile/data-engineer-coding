#========import all pyspark modules and python packages
from ctypes import wstring_at
from datetime import date
from os import access
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


#=======import all glue mudules and packages

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
#=======================getting the job name
args = getResolvedOptions(sys.argv, ['glue-job-name'])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['glue-job-name'], args)
# ============pyspark configuration and jdbc configuration for postgresql connection
spark = SparkSession \
    .builder \
    .appName("") \
    .config("spark.jars", "postgresql-42.2.14.jar") \
    .getOrCreate()

jdbcDF = spark.read.format("jdbc").\
options(
         url='mycluster.cluster-123456789012.us-east-1.rds.amazonaws.com', 
         user='postgres',
         password='5Y67bg#r#',
         driver='org.postgresql.Driver').\
        load()
#-========Viewing the schema 
jdbcDF.printSchema()
#===========creating temporal views for the database schema
jdbcDF.createGlobalTempView("bank")
jdbcDF.createGlobalTempView("branch")
jdbcDF.createGlobalTempView("worker")
jdbcDF.createGlobalTempView("client")
jdbcDF.createGlobalTempView("account")
jdbcDF.createGlobalTempView("loans")
#=======================getting the required fields from the database =================
df_agg = spark.sql(
    "select ba.name, lo.loan_date, lo.amount\
    from loans as lo\
    join account as acc\
    on lo.accunt_idaccount = acc.idaccount\
    join client as cl\
    on cl.idclient = acc.cient_idclient\
    join branch as br \
    br.idbranch = cl.branch_idbranch\
    join bank as ba\
    on br.bank_idbank = ba.idbank\
    "
)
#===============calculating the moving average for all branches with loan taken over 3 months 
windospec = Window().parttionBy(['name']).orderBy('loan_date').rowsBetween(-90,0)  # 90 days is equal to 3 months
bankloan_moving  = df_agg.withColumn('Moving_avg_loan_amount',F.mean('amount').over(windospec)) # moving avarage of all branche

##  Manditory Logging Functions:
log_data = []
log_types = {
    'debug': 'DEBUG',
    'warn': 'WARN',
    'info': 'INFO',
    'success': 'SUCCESS',
    'error': 'ERROR'
}
def get_current_datetime():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def do_log (dl_appname, dl_logtype, dl_message):
### REMOVE
###     log_data.append([get_current_datetime(), dl_appname, dl_logtype, '{0} {1}'.format(dl_appname, dl_message)])
    print ('###LogTable###', dl_logtype, get_current_datetime(), '{0} {1}'.format(dl_appname, dl_message))
 
 
outputfile = "s3://" + bucket_name + "/" + filekey

## Write target s3 bucket Data.
bankloan_moving.coalesce(1).write.mode("append").format("csv").partitionBy("name","year","month").save(outputfile)
#======writing the logs to the specified s3 bucket
do_log(app_name, log_types.get('info'), 'Done!')
do_log(app_name, log_types.get('info'), 'WRITE = s3://' + sourcebucket + "/" + filekey + run_id + "/" + get_current_datetime())


