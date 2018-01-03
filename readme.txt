I

ssh root@maprdemo -p 2222

 maprcli table create -path /mapr/maprdemo.mapr.io/apps/payments -tabletype json -defaultreadperm p -defaultwriteperm p

 hadoop fs -put payments.csv /tmp

maprcli stream create -path /mapr/maprdemo.mapr.io/apps/paystream -produceperm p -consumeperm p -topicperm p
maprcli stream topic create -path /mapr/maprdemo.mapr.io/apps/paystream -topic payments  



 
java -cp maprdb-spark-payment/target/mapr-spark-payment-1.0.jar:./maprdb-spark-payment/target/* streaming.SparkKafkaConsumer

java -cp ./target/mapr-spark-payment-1.0.jar:./target/* streaming.SparkKafkaConsumer

java -cp ./target/mapr-spark-payment-1.0.jar:./target/* streams.MsgProducer

java -cp ./target/mapr-spark-payment-1.0.jar:./target/* streams.MsgConsumer

java -cp ./target/mapr-spark-payment-1.0.jar:./target/* sparkmaprdb.QueryPayment
