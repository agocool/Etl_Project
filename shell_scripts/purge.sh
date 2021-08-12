#!/bin/bash


current_date=$(date +'%Y-%m-%d')


log_file_path=/home/talentum/zomato_etl/logs/

mypattern="log_*_*.log"
for f in `ls $log_file_path`;
do
    file_name="$(basename "$f")"	
    if [[ $file_name == $mypattern ]] ; then
      
	logging_date=$( awk -F '_' '{print $2}' <<< $file_name )
	logging_date=$(echo "$logging_date" | sed 's/\(..\)\(..\)/\1-\2-/')  
	log_date=$(awk -F '-' '{print $3"-"$2"-"$1}'  <<< $logging_date )	
	
	diff=$((($( date -d $current_date +%s) - $( date -d $log_date +%s)) / 86400))
	if (( $diff  > 1 && $diff <=7 )) ; then 
		echo "Deleting Log :-" $file_name
		rm $log_file_path$file_name
	fi		
    fi 	
done

echo 
echo "*****************************************"
echo "*            Purge Completed!           * "
echo "*****************************************"
