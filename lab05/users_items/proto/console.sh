spark-submit --conf spark.users_items.input_dir=/user/andrey.blednykh2/visits --conf spark.users_items.output_dir=/user/andrey.blednykh2/users-items --conf spark.users_items.update=0 --class users_items users_items_2.11-1.0.jar

kafka-console-consumer.sh --bootstrap-server spark-master-1:6667 --topic andrey_blednykh2
