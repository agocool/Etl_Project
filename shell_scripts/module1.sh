#!/bin/bash

module_name="Module1"

#Setting Up the Log file location
log_file_path=/home/talentum/zomato_etl/logs/module1.log

#Update_hive function to load the status into hive table zomato_summary_log
function update_hive(){

		spark_commad=$1
		declare -a file_handler
		file_handler=(`cat $log_file_path`)
		echo "Module 1 " 
		
		start=$(echo ${file_handler[3]} ${file_handler[4]})
	 	end=$(echo ${file_handler[5]} ${file_handler[6]})
		
		#emailing status
		echo -e "Module 1 :" ${file_handler[7]}  "\nApplication Id :" ${file_handler[1]} "\nStart time :" $start "\nEnd time :" $end | mail -s "Module 1 Execution Status" humanbeing7222@gmail.com

		#Beeline command to load status log into the zomato_summary_log table
		beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p Hive@123 --incremental=true -e "insert into table a_gokul.zomato_summary_log values('${file_handler[1]}','${file_handler[2]}','$spark_command','$start','$end','${file_handler[7]}');"
		}
#Spark submit function to excute the spark module1 application
function spark_submit() {
	echo
	echo "Module 1 Execution Started"
	echo
	spark_command="spark-submit --master yarn --deploy-mode client --driver-memory 2g --driver-cores 1 --num-executors 3 --executor-cores 1 --executor-memory 2g /home/talentum/zomato_etl/spark/Mod1sa/mod1sa.py"
	$spark_command
	update_hive "$spark_command"
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
			echo
			echo "Previous Application Failed! Initiating again."
			echo
			spark_submit
			;;                                                
		"RUNNING")        
			echo                                        
			echo "Wait! Application is still running."   
			echo                                                                                        
			;;                                                
		*)          
			echo                                                                 
			echo "Starting Spark Module 1"
			echo                                                
			echo 
			echo			
			rm "$log_file_path" 
			echo "Starting........ "
			echo			                                               
			spark_submit                                                
			;;                                                
	esac                                                
else                                                
	echo "Log file not found, Initiating Spark Application"                                                
	spark_submit                                                
fi;



#checking if files are loaded
loader_path=/home/talentum/zomato_etl/source/csv/zomato_20190613.csv
echo
echo "Checking if the Files are loaded into Local file System"
echo
if [ ! -f $loader_path ]; then
	echo
	echo "Moving the converted files to Local file system"
	echo
	temp_source=/user/talentum/csv_content/csv
	tp="/part*"
	temp_dest=/home/talentum/zomato_etl/source/csv/
	for ((i=1;i<=5;i++))
	do 
	   final_source=$temp_source$i$tp
	   if [ $i -eq 1 ]
	   then
		file_name='zomato_20190609.csv'
		
	   elif [ $i == 2 ]
	   then
		file_name='zomato_20190610.csv'
	   elif [ $i == 3 ]
	   then
		file_name='zomato_20190611.csv'
	   elif [ $i == 4 ]
	   then
		file_name='zomato_20190612.csv'
	   elif [ $i == 5 ]
	   then
		file_name='zomato_20190613.csv'
	   fi

	   final_dest=$temp_dest$file_name

	   hdfs dfs -copyToLocal $final_source $final_dest
	done	
     else
	 echo
         echo "CSV Already loaded into local file system"
	 echo 
fi

hdfs dfs -rm -r csv_content
echo
echo "checking if CSV files have been moved to zomato_etl_a.gokul/zomato "
echo
path=/user/talentum/zomato_etl_a.gokul/zomato_ext/zomato/zomato_20190613.csv
if hdfs dfs -test -e $path; then
     echo
     echo " Files already moved "
     echo	
   else
      echo "Moving the converted files from Local file system"
      echo 
      sleep 2s
      hdfs dfs -put /home/talentum/zomato_etl/source/csv/zomato* zomato_etl_a.gokul/zomato_ext/zomato 	
fi  
