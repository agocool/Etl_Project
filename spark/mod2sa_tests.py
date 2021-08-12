import unittest
import os
import os
import re
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
import logging
from datetime import datetime


class mod2saTest(unittest.TestCase):
    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%Y-%m-%d')

    def setup_logger(name, log_file, level=logging.INFO):
        handler = logging.FileHandler(log_file, mode='w')
        handler.setFormatter(mod2saTest.formatter)

        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.addHandler(handler)
        return logger

    def logset_temp(log):
        logr = mod2saTest.setup_logger('temp', '/home/talentum/zomato_etl/logs/module1.log')
        logr.info(log)
        logging.shutdown()

    def logset(log, log_name):
        file_name = '/home/talentum/zomato_etl/logs/' + str(log_name) + ".log"
        logr = mod2saTest.setup_logger('main', file_name)
        logr.info(log)

    def test_mod2sa(self):
        st_time = datetime.now()
        start_time = st_time.strftime('%Y-%m-%d %H:%M:%S')
        spark = SparkSession.builder.appName("Spark Module 2").enableHiveSupport().getOrCreate()
        job_id = spark.sparkContext.applicationId
        job_id = job_id.split('-')[1]

        job_step = 'Load-CSV-To-Zomato-Table'

        job_status = 'RUNNING'

        end_time = 'NA'

        log = job_id + " " + job_step + " " + str(start_time) + " " + str(end_time) + " " + job_status
        mod2saTest.logset_temp(log)

        try:

            file_loc = '/home/talentum/zomato_etl/source/csv/'
            for filename in os.listdir(file_loc):
                filedate = (filename.split('_')[1]).split('.')[0]
                query = "LOAD DATA INPATH '/user/talentum/zomato_etl_a.gokul/zomato_ext/zomato/zomato_" + str(
                    filedate) + ".csv' " + "OVERWRITE INTO TABLE a_gokul.zomato PARTITION (filedate=" + str(
                    filedate) + ")"

                spark.sql(query)

            query = "LOAD DATA LOCAL INPATH '/home/talentum/zomato_raw_files/country_code.csv' Overwrite INTO TABLE a_gokul.dim_country"
            spark.sql(query)

            row_count = spark.sql("select count(*) as count from a_gokul.zomato where filedate=20190609")
            expected_value = 1183
            value = row_count.first()['count']
            self.assertEqual(value, expected_value)

        except NameError:
            job_status = "FAILED"
            print('Variable not found')
        except OSError:
            job_status = "FAILED"
            print('Execution Interrupted')
        except AnalysisException:
            job_status = "SUCCESS"
            print()
            print('CSV Files Already Loaded Into The Table')
            print()
        except:
            job_status = "FAILED"
            print()
            print('Unsuccessfull! Please Check the Path ')
            print()
        else:
            print()
            print("********** Loaded CSV Successfully ***********")
            print()
            job_status = "SUCCESS"

        end_time = datetime.now()
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')

        final_log = "log_" + str(st_time.strftime('%d%m%Y_%H%M'))
        log = job_id + " " + job_step + " " + str(start_time) + " " + str(end_time) + " " + job_status
        mod2saTest.logset_temp(log)
        mod2saTest.logset(log, final_log)







if __name__ == '__main__':
    unittest.main()


