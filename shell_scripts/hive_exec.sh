#!/bin/bash


echo "Running the the Hive Executor"
echo
echo
echo "*********************************************************************************"
echo
echo "******************************** Zomato table ddl operation ************************************************"

beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p Hive@123 --incremental=true -f /home/talentum/zomato_etl/hive/ddl/zomato_ddl.hive

echo "******************************** done ************************************************" 
echo
echo
echo "*********************************************************************************"
echo
echo "******************************** zomato table dml operation ************************************************"

beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p Hive@123 --incremental=true -f /home/talentum/zomato_etl/hive/dml/zomato_dml.hive

echo "******************************** done ************************************************" 


echo
echo "*********************************************************************************"
echo
echo "******************************** dim_country table ddl operation ************************************************"

beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p Hive@123 --incremental=true -f /home/talentum/zomato_etl/hive/ddl/dim_country_ddl.hive

echo "******************************** done ************************************************" 
echo
echo
echo "*********************************************************************************"
echo
echo "******************************** dim_country table dml operation ************************************************"

beeline -u jdbc:hive2://localhost:10000 -n hiveuser -p Hive@123 --incremental=true -f /home/talentum/zomato_etl/hive/dml/dim_country_dml.hive

echo "******************************** done ************************************************" 
echo
