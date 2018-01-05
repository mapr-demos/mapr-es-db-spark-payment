#  Streaming ETL Pipeline to Transform, Store and Explore Healthcare Dataset using Spark, JSON, MapR-DB, MapR-ES, Drill

## Introduction

This example will show you how to work with MapR-ES, Spark Streaming, and MapR-DB JSON :

* Publish using the Kafka API  Medicare Open payments data from a CSV file into MapR-ES 
* Consume and transform the streaming data with Spark Streaming and the Kafka API.
* Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector.
* Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL.
* Query the MapR-DB document database using Apache Drill. 
* Query the MapR-DB document database using Java and the OJAI library.

**Prerequisites**

* MapR Converged Data Platform 6.0 with Apache Spark and Apache Drill OR [MapR Container for Developers](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).
* JDK 8
* Maven 3.x (and or IDE such as Netbeans or IntelliJ )

## Setting up MapR Container For Developers

The MapR Container For Developers is a docker image that enables you to quickly deploy a MapR environment on your developer machine.

Installation, Setup and further information can be found [**here**](https://maprdocs.mapr.com/home/MapRContainerDevelopers/MapRContainerDevelopersOverview.html).

#### 1. Create MapR-ES Stream, Topic, and MapR-DB table 

from your mac log in to the docker container:
```
$ssh root@maprdemo -p 2222
```
In the docker container use the mapr command line interface to create streams and tables
```
$ maprcli stream create -path /mapr/maprdemo.mapr.io/apps/paystream -produceperm p -consumeperm p -topicperm p
$ maprcli stream topic create -path /mapr/maprdemo.mapr.io/apps/paystream -topic payments  

$ maprcli table create -path /mapr/maprdemo.mapr.io/apps/payments -tabletype json -defaultreadperm p -defaultwriteperm p
  
```

> Refer to [**stream create command**](https://maprdocs.mapr.com/home/ReferenceGuide/stream_create.html) and
 [**table create command**](https://maprdocs.mapr.com/home/ReferenceGuide/table-create.html) 
 for more details about the CLI command.

#### 2. Run the Java client to publish events to the topic and the Spark Streaming client to consume events

**Build the project**

from your mac use either maven or your IDE to build the project

```
$ mvn clean install
```

This creates the following jars.
- `mapr-es-db-spark-payment/target/mapr-es-db-spark-payment-1.0.jar`
- `mapr-es-db-spark-payment/target/mapr-es-db-spark-payment-1.0-jar-with-dependencies.jar`

**Run the java publisher and the Spark consumer**

From your mac in the mapr-es-db-spark-payment directory you can run the java client to publish with the following command 
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* streams.MsgProducer
```
This client will read lines from the file in ./data/payments.csv and publish them to the topic /apps/paystream:payments. 
You can optionally pass the file and topic as input parameters <file topic> 

You can wait for the java client to finish, or from a separate mac terminal you can run the spark streaming consumer with the following command, 
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-spark-payment-1.0-jar-with-dependencies.jar:./target/* streaming.SparkKafkaConsumer
```
This spark streaming client will consume from the topic /apps/paystream:payments and write to the table /apps/payments.
You can optionally pass the topic and table as input parameters <topic table> 

#### 3. Run the Spark SQL client to load and query data from MapR-DB with Spark-DB Connector

You can wait for the java client and spark consumer to finish, or from a separate mac terminal you can run the spark sql with the following command, 
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* sparkmaprdb.QueryPayment
```

optionally from your mac you can log in to the docker container:

```
$ ssh root@maprdemo -p 2222
```

start the spark shell with this command

```
$ /opt/mapr/spark/spark-2.1.0/bin/spark-shell --master local[2]
```
copy paste  from the scripts/sparkshell file to query MapR-DB

#### 4. Working with Drill-JDBC

From your mac in the mapr-es-db-spark-payment directory you can run the java client to query the MapR-DB table using Drill-JDBC 
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* maprdb.DRILL_SimpleQuery
```

#### 5. Working with OJAI

OJAI the Java API used to access MapR-DB JSON, leverages the same query engine as MapR-DB Shell and Apache Drill. 

From your mac in the mapr-es-db-spark-payment directory you can run the java client to query the MapR-DB table using OJAI
or you can run from your IDE :

```
$ java -cp ./target/mapr-es-db-spark-payment-1.0.jar:./target/* maprdb.OJAI_SimpleQuery
```

#### 6. Using the MapR-DB shell and Drill from your mac client 
Refer to [**connecting clients **](https://maprdocs.mapr.com/home/MapRContainerDevelopers/ConnectingClients.html) for 
information on setting up the Drill client

**Use MapR DB Shell to Query the Payments table**

In this section you will  use the DB shell to query the Payments JSON table

To access MapR-DB from your mac client or logged into the container, you can use MapR-DB shell:

```
$ /opt/mapr/bin/mapr dbshell
```

To learn more about the various commands, run help or help <command> , for example help insert.

```
$ maprdb mapr:> jsonoptions --pretty true --withtags false
```
**find 5 documents**
```
maprdb mapr:> find /apps/payments --limit 5
```
Note that queries by _id will be faster because _id is the primary index

**Query document with Condition _id starts with 98485 (physician id)**
```
maprdb mapr:> find /apps/payments --where '{ "$like" : {"_id":"98485%"} }' --f _id,amount
```
**Query document with Condition _id has february**
```
find /apps/payments --where '{ "$like" : {"_id":"%_02/%"} }' --f _id,amount
```
**find all payers=**
```
maprdb mapr:> find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
```

**Use Drill Shell to query MapR-DB**

From your mac terminal connect to Drill as user mapr through JDBC by running sqlline:
/opt/mapr/drill/drill-1.11.0/bin/sqlline -u "jdbc:drill:drillbit=localhost" -n mapr

**Query document with Condition _id has february**

**Who are top 5 Physician Ids by Amount**
```
0: jdbc:drill:drillbit=localhost> select physician_id, sum(amount) as revenue from dfs.`/apps/payments` group by physician_id order by revenue desc limit 5;
```
**What are top 5 nature of payments by Amount**
```
0: jdbc:drill:drillbit=localhost> select nature_of_payment,  sum(amount) as total from dfs.`/apps/payments` group by nature_of_payment order by total desc limit 5;
```
**Query for payments for physician id**
```
0: jdbc:drill:drillbit=localhost> select _id,  amount from dfs.`/apps/payments` where _id like '98485%';
```
**Query for payments in february**
```
0: jdbc:drill:drillbit=localhost> select _id,  amount from dfs.`/apps/payments` where _id like '%[_]02/%';
```
**Queries on payer**
```
0: jdbc:drill:drillbit=localhost> select _id, amount, payer from dfs.`/apps/payments` where payer='CorMatrix Cardiovascular Inc.';

0: jdbc:drill:drillbit=localhost> select _id, amount, payer from dfs.`/apps/payments` where payer like '%Dental%';

0: jdbc:drill:drillbit=localhost> select  distinct(payer) from dfs.`/apps/payments` ;
```


#### 7. Adding a secondary index to improve queries

Let's now add indices to the payments table.

In a docker container terminal window:

```
$ maprcli table index add -path /apps/payments -index idx_payer -indexedfields 'payer:1'
```
In MapR-DB Shell, try queries on payments payers and compare with previous query performance:
```
maprdb mapr:> find /apps/payments --where '{ "$eq" : {"payer":"Mission Pharmacal Company"} }' --f _id,payer,amount,nature_of_payment
```
In Drill try 
```
0: jdbc:drill:drillbit=localhost> select _id, amount, payer from dfs.`/apps/payments` where payer='CorMatrix Cardiovascular Inc.';

0: jdbc:drill:drillbit=localhost> select _id, amount, payer from dfs.`/apps/payments` where payer like '%Dental%';

0: jdbc:drill:drillbit=localhost> select  distinct(payer) from dfs.`/apps/payments` ;

```
##Cleaning Up

You can delete the topic and table using the following command from a container terminal:
```
maprcli stream topic delete -path /mapr/maprdemo.mapr.io/apps/paystream -topic payment
maprcli table delete -path /mapr/maprdemo.mapr.io/apps/payments

```
## Conclusion

In this example you have learned how to:

* Publish using the Kafka API  Medicare Open payments data from a CSV file into MapR-ES 
* Consume and transform the streaming data with Spark Streaming and the Kafka API
* Transform the data into JSON format and save to the MapR-DB document database using the Spark-DB connector
* Query and Load the JSON data from the MapR-DB document database using the Spark-DB connector and Spark SQL 
* Query the MapR-DB document database using Apache Drill 
* Query the MapR-DB document database using Java and the OJAI library



You can also look at the following examples:

* [mapr-db-60-getting-started](https://github.com/mapr-demos/mapr-db-60-getting-started) to learn Discover how to use DB Shell, Drill and OJAI to query and update documents, but also how to use indexes.
* [Ojai 2.0 Examples](https://github.com/mapr-demos/ojai-2-examples) to learn more about OJAI 2.0 features
* [MapR-DB Change Data Capture](https://github.com/mapr-demos/mapr-db-cdc-sample) to capture database events such as insert, update, delete and react to this events.





