cd lab04b/agg
sbt package
spark-submit --class agg --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 ./target/scala-2.11/agg_2.11-1.0.jar