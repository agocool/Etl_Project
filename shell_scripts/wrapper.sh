#!/bin/bash





echo 
echo "			     Zomato Project                            "
echo
echo
echo "*********************************************************************"
echo "*                                                                   *" 
echo " 1. Execute Module 1 [ Json_to_Csv ]                                *"      
echo "*                                                                   *" 
echo "*********************************************************************"
echo "*                                                                   *" 
echo " 2. Execute Module 2 [ Load-CSV-To-Zomato-Table ]                   *"      
echo "*                                                                   *" 
echo "*********************************************************************"
echo "*                                                                   *" 
echo " 3. Execute Module 3 [ Load-Zomato-Summary-Table ]                  *"      
echo "*                                                                   *" 
echo "*********************************************************************"
echo "*                                                                   *" 
echo " 4. Execute All Module In a Sequence                                *"      
echo "*                                                                   *" 
echo "*********************************************************************"
echo "*                                                                   *" 
echo " 5. Exit                                                            *"      
echo "*                                                                   *" 
echo "*********************************************************************"
echo

if [ $# -eq 0 ]; then
	read -p "Please Select the option No. :" option
else option=$1
fi



case $option in 
 
	1)
	  /home/talentum/zomato_etl/script/module1.sh
	  /home/talentum/zomato_etl/script/purge.sh

	;;

	2)
	  /home/talentum/zomato_etl/script/module2.sh
	  /home/talentum/zomato_etl/script/purge.sh

	;;

	3)
	 /home/talentum/zomato_etl/script/module3.sh
         /home/talentum/zomato_etl/script/purge.sh



	;;

	4) echo 
	 /home/talentum/zomato_etl/script/module1.sh
	 
	 log_file_path=/home/talentum/zomato_etl/logs/module1.log
	 file_handler=(`cat $log_file_path`)
	 if [[ "${file_handler[7]}" != "SUCCESS" ]]; then
		echo
		echo "    Module 1 Failed     "
		exit 1
		
	 else 
		echo 
		echo "    Moving To Next Module    "
		echo
	 fi

	 /home/talentum/zomato_etl/script/module2.sh
	 
         log_file_path=/home/talentum/zomato_etl/logs/module2.log
	 file_handler=(`cat $log_file_path`)
	 if [[ "${file_handler[7]}" != "SUCCESS" ]]; then
		echo
		echo "    Module 2 Failed     "
		exit 1
		
	 else 
		echo 
		echo "    Moving To Next Module    "
		echo
	 fi

	 /home/talentum/zomato_etl/script/module3.sh
         
	 log_file_path=/home/talentum/zomato_etl/logs/module3.log
	 file_handler=(`cat $log_file_path`)
	 if [[ "${file_handler[7]}" != "SUCCESS" ]]; then
		echo
		echo "    Module 3 Failed     "
		
		
	 else 
		echo 
		echo "    All 3 Modules Executed Succesfully     "
		echo
	 fi


	 /home/talentum/zomato_etl/script/purge.sh
	
	;;

	5)
	exit 1

	;;
		
	*)
	 echo "No such Option Exists"
	 echo " Try Again !!"
	 echo
	 /home/talentum/zomato_etl/script/wrapper.sh
	 exit 1
	;;
esac












































