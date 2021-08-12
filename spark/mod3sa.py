import re
from pyspark.sql.functions import col,lit
from pyspark.sql import SparkSession
import pyspark
import getpass
import os
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
import logging
from datetime import datetime


formatter = logging.Formatter('%(asctime)s %(message)s',datefmt='%Y-%m-%d')

def setup_logger(name,log_file,level=logging.INFO):
    handler = logging.FileHandler(log_file,mode='w')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger



def logset_temp(log):
    logr = setup_logger('temp','/home/talentum/zomato_etl/logs/module3.log')
    logr.info(log)
    logging.shutdown()

def logset(log,log_name):
    file_name='/home/talentum/zomato_etl/logs/'+str(log_name)+".log"
    logr = setup_logger('main',file_name)
    logr.info(log)

if __name__ == '__main__':

    st_time=datetime.now()
    start_time=st_time.strftime('%Y-%m-%d %H:%M:%S')
    spark = SparkSession.builder.master("local").appName("Spark Module 3").enableHiveSupport().getOrCreate()
    job_id = spark.sparkContext.applicationId
    job_id = job_id.split('-')[1]

    job_step='Load-Zomato-Summary-Table'

    job_status='RUNNING'

    end_time='NA'

    log = job_id + " " + job_step + " " + str(start_time) + " " + str(end_time) + " " + job_status
    logset_temp(log)

    try:
        print("Fetching data from zomato table")
        print()
        query = " SELECT z.`restaurant id`, nvl( z.`restaurant name`,'NA') AS `restaurant name`,z.`country code`, nvl(z.`city`, 'NA') AS `city`, nvl(z.`address`, 'NA') AS `address`, nvl(z.`locality`,'NA') AS `locality`, nvl(z.`locality verbose`,'NA') AS `locality verbose`, nvl(z.`longitude`, 'NA') AS `longitude`, nvl(z.`latitude`, 'NA') AS `latitude`, nvl(z.`cuisines`,'NA') AS `cuisines`, z.`average cost for two` , nvl(z.`currency`,'NA') AS  `currency`, z.`has table booking`, z.`has online delivery`, z.`is delivering now`, z.`switch to order menu`, z.`price range`, nvl(z.`aggregate rating`,'NA') AS  `aggregate rating` , nvl(z.`rating text`,'NA') AS `rating text`, nvl(z.`votes`,'NA') AS `votes`,z.`filedate` as `p_filedate`, nvl(d.`country`,'NA') AS `p_country_name`, case when z.cuisines like '%Andhra%' or z.cuisines like '%Goan%' or z.cuisines like '%Hyderabadi%' or z.cuisines like '%Bengali%' or z.cuisines like '%Bihari%' or z.cuisines like '%Indian%' or z.cuisines like '%Chettinad%' or z.cuisines like '%Gujarati%' or z.cuisines like '%Rajasthani%' or z.cuisines like '%Maharashtrian%' or z.cuisines like '%Mangalorean%' or z.cuisines like '%Mithai%' or z.cuisines like '%Mughlai%' or z.cuisines like '%Kerala%' then 'Indian' else 'World Cuisines' end as m_cuisines, case when `aggregate rating` between 1.9 and 2.4 and `rating text` = 'Poor' then 'Red' when `aggregate rating` between 2.5 and 3.4 and `rating text`='Average' then 'Amber' when `aggregate rating` between 3.5 and 3.9 and `rating text`='Good' then 'Light Green' when `aggregate rating` between 4.0 and 4.4 and `rating text`='Very Good' then 'Green' when `aggregate rating` between 4.5 and 5 and `rating text`='Excellent' then 'Gold' when `aggregate rating` = 0 and `rating text`='Not rated' then 'NA' end as m_rating_colour, from_unixtime(unix_timestamp()) AS `create_datetime` from a_gokul.zomato z, a_gokul.dim_country d WHERE z.cuisines is NOT NULL AND z.`country code` = d.`country code`"

        user = getpass.getuser()


        df = spark.sql(query)
        df = df.withColumn('user_id', lit(user))

        df = df.dropDuplicates()

        for c in df.columns:
            df = df.withColumnRenamed(c, c.replace(" ", ""))



        print()
        print("*** Loading dataframe to Hive ***")
        print()
        #spark.sql("drop table a_gokul.zomato_summary")
        df.write.format("orc").mode("overwrite").partitionBy("p_filedate", "p_country_name").saveAsTable("a_gokul.zomato_sum_temp")


    except NameError:
        job_status = "FAILED"
        print('Variable not found')
    except OSError:
        job_status = "FAILED"
        print('Execution Interrupted')
    except:
        job_status = "FAILED"
        print()
        print('Unsuccessfull! Please Check columns in Query ')
        print()
    else :
        print()
        print("********** Loaded Zomato_Summary ***********")
        print()
        job_status = "SUCCESS"




    end_time= datetime.now()
    end_time=end_time.strftime('%Y-%m-%d %H:%M:%S')

    final_log="log_"+ str(st_time.strftime('%d%m%Y_%H%M'))
    log = job_id + " " + job_step + " " + str(start_time) + " " + str(end_time) + " " + job_status
    logset_temp(log)
    logset(log,final_log)