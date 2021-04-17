spark-submit --class train mlproject_2.11-1.0.jar
spark-submit --class test mlproject_2.11-1.0.jar

kafka-console-consumer.sh --bootstrap-server spark-master-1:6667 --topic andrey_bledykh2
kafka-console-consumer.sh --bootstrap-server spark-master-1:6667 --topic andrey_blednykh2_lab07_out