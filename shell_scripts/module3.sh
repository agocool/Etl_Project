#!/bin/bash

module_name="Module3"

#Setting Up the Log file location
log_file_path=/home/talentum/zomato_etl/logs/module3.log

#Update_hive function to load the status into hive table zomato_summary_log
function hive_operation(){

		spark_commad=$1
		declare -a file_handler
		file_handler=(`cat $log_file_path`)
		
		start=$(echo ${file_handler[3]} ${file_handler[4]})
	 	end=$(echo ${file_handler[5]} ${file_handler[6]})

		#emailing status
		echo -e "Module 3 :" ${file_handler[7]}  "\nApplication Id :" ${file_handler[1]} "\nStart time :" $start "\nEnd time :" $end | mail -s "Module 3 Execution Status" humanbeing7222@gmail.com
		echo		
		echo "Updating log table"		
		echo
		echo
		#Beeline command to load status log into the zomato_summary_log table
		beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p Hive@123 --incremental=true -e "insert into table a_gokul.zomato_summary_log values('${file_handler[1]}','${file_handler[2]}','$spark_command','$start','$end','${file_handler[7]}');"
		
		}
#Spark submit function to excute the spark module1 application
function spark_submit() {

	echo "Module 3 Execution Started"

	
	spark_command="spark-submit --master yarn --deploy-mode client --driver-memory 2g --num-executors 2 --executor-memory 2g /home/talentum/zomato_etl/spark/Mod3sa/mod3sa.py"
	echo
	echo "**********************************                           ******************************************" 
	echo	
	$spark_command
	beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p Hive@123 --incremental=true  -f /home/talentum/zomato_etl/hive/ddl/zomato_summary_ddl.hive
	beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p Hive@123 --incremental=true  -f /home/talentum/zomato_etl/hive/dml/zomato_summary_dml.hive
	echo
	echo
	echo "**********************************************************************************************************"
		
	hive_operation "$spark_command"
}




#Array to handle log file content
declare -a file_handler
if test -f "$log_file_path"; then

	file_handler=(`cat $log_file_path`)

	#Case to check status
	case "${file_handler[7]}" in
		"SUCCESS")
			spark_submit
			;;
		"FAILED")
			echo "Previous Application Failed! Initiating Spark Application."
			spark_submit
			;;                                                
		"RUNNING")                                                
			echo "Wait! Application is still running."                                                                                           
			;;                                                
		*)                                                                           
			echo "Starting Module 3"  
                        echo                   
			echo "Starting........ "	
			echo		                                               
			spark_submit                                                
			;;                                                
	esac                                                
else                                                
	echo "Log file not found, Initiating Spark Application"                                                
	spark_submit                                                
fi;


mv /home/talentum/zomato_etl/source/json/* /home/talentum/zomato_etl/archive/



