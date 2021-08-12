import unittest
import re
import logging
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col


class mod1saTest(unittest.TestCase):
    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d')

    def setup_logger(name, log_file, level=logging.INFO):
        handler = logging.FileHandler(log_file, mode='w')
        handler.setFormatter(mod1saTest.formatter)

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        return logger

    def logset_temp(log):
        logr = mod1saTest.setup_logger('temp', '/home/talentum/zomato_etl/logs/module1.log')
        logr.info(log)
        logging.shutdown()

    def logset(log, log_name):
        file_name = '/home/talentum/zomato_etl/logs/' + str(log_name) + ".log"
        logr = mod1saTest.setup_logger('main', file_name)
        logr.info(log)

    def test_mod1sa(self):

        st_time = datetime.now()
        start_time = st_time.strftime('%Y-%m-%d %H:%M:%S')
        spark = SparkSession.builder.appName("Spark Module 1").enableHiveSupport().getOrCreate()

        job_id = spark.sparkContext.applicationId
        job_id = job_id.split('-')[1]

        job_step = 'Json_to_Csv'

        job_status = 'RUNNING'

        end_time = 'NA'

        log = job_id + " " + job_step + " " + str(start_time) + " " + str(end_time) + " " + job_status
        mod1saTest.logset_temp(log)

        try:

            for i in range(1, 6):
                print()
                print('******** Loading file ' + str(i), ' ********')
                print()

                source_file_name = 'file' + str(i) + '.json'
                json_loc = 'file:///home/talentum/zomato_etl/source/json/' + source_file_name;
                print()
                print('******** Converting...  ********')
                print()
                df_json = spark.read.json(json_loc)
                df = df_json.select(F.explode('restaurants.restaurant'))
                df_csv = df.select(col('col.R.res_id').alias("Restaurant_ID"), col('col.name').alias("Restaurant Name"),
                                   col('col.location.country_id').alias("Country Code"),
                                   col('col.location.city').alias("City"),
                                   col('col.location.address').alias("Address"),
                                   col('col.location.locality').alias("Locality"),
                                   col('col.location.locality_verbose').alias("Locality Verbose"),
                                   col('col.location.longitude').alias("Longitude"),
                                   col('col.location.latitude').alias("Latitude"),
                                   col('col.cuisines').alias("Cuisines"),
                                   col('col.average_cost_for_two').alias("Average Cost for two"),
                                   col('col.currency').alias("Currency"),
                                   col('col.has_table_booking').alias("Has table booking"),
                                   col('col.has_online_delivery').alias("Has Online delivery"),
                                   col('col.is_delivering_now').alias("Is delivering now"),
                                   col('col.Switch_to_order_menu').alias("Switch to order menu"),
                                   col('col.price_range').alias("Price range"),
                                   col('col.user_rating.aggregate_rating').alias("Aggregate rating"),
                                   col('col.user_rating.rating_text').alias("Rating text"),
                                   col('col.user_rating.votes').alias("Votes"))

                csv_hdfs_loc = 'csv_content/csv' + str(i)

                df_csv.write.format('csv').option("delimeter", "\t").save(csv_hdfs_loc, header=True)
                print()
                print('******** Converted file', i, 'to csv ********')
                print()

                if i==1:
                    expected_value = 308322
                    value = df_csv.select('Restaurant_ID').collect()[0].Restaurant_ID
                    self.assertEqual(value, expected_value)


            i = i + 1

        except NameError:
            job_status = "FAILED"
            print('Variable not found')
        except OSError:
            job_status = "FAILED"
            print('Execution Interrupted')
        except:
            job_status = "FAILED"
            print()
            print('Unsuccesfull!! , Check path Or File already Exist')
            print()
        else:
            print()
            print("********** Conversion Successful ***********")
            print()
            job_status = "SUCCESS"

        end_time = datetime.now()
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')

        final_log = "log_" + str(st_time.strftime('%d%m%Y_%H%M'))
        log = job_id + " " + job_step + " " + str(start_time) + " " + str(end_time) + " " + job_status
        mod1saTest.logset_temp(log)
        mod1saTest.logset(log, final_log)



if __name__ == '__main__':
    unittest.main()


