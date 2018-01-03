package streaming

import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.streaming._
import com.mapr.db.spark.sql._

import org.apache.spark.streaming.{ Seconds, StreamingContext, Time }
import org.apache.spark.streaming.dstream._

import org.apache.spark.streaming.kafka09.{ ConsumerStrategies, KafkaUtils, LocationStrategies }
import scala.util.Try

/*

*/
object SparkKafkaConsumer {

  /*
5 Physician_Profile_ID as physician_id, 
31 Date_of_Payment as date_payment, 
45 Record_ID as record_id, 
27 Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer,  
30 amount, 
19 Physician_Specialty, 
34 Nature_of_Payment_or_Transfer_of_Value as Nature_of_payment 
*/
  case class Payment(physician_id: String, date_payment: String, record_id: String, payer: String, amount: Double, physician_specialty: String, nature_of_payment: String) extends Serializable

  case class PaymentwId(_id: String, physician_id: String, date_payment: String, payer: String, amount: Double, physician_specialty: String,
    nature_of_payment: String) extends Serializable

  def parsePayment(str: String): Payment = {
    val td = str.split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)")
    Payment(td(5).replaceAll("\"", ""), td(31).replaceAll("\"", ""),
      td(45).replaceAll("\"", ""), td(27).replaceAll("\"", ""),
      Try(td(30).toDouble) getOrElse 0.0,
      td(19).replaceAll("\"", ""),
      td(34).replaceAll("\"", ""))
  }

  def parsePaymentwID(str: String): PaymentwId = {
    val pa = parsePayment(str)
    val id = pa.physician_id + '_' + pa.date_payment + '_' + pa.record_id
    PaymentwId(id, pa.physician_id, pa.date_payment, pa.payer, pa.amount, pa.physician_specialty, pa.nature_of_payment)
  }

  def main(args: Array[String]) = {
    var tableName: String = "/mapr/maprdemo.mapr.io/apps/payments"
    var topicc: String = "/mapr/maprdemo.mapr.io/apps/paystream:payments"

    if (args.length == 2) {
      tableName = args(0)
      topicc = args(1)
    } else {
      System.out.println("Using hard coded parameters unless you specify the consume topic and table. <topic table>   ")
    }

    val groupId = "testgroup"
    val offsetReset = "earliest"  //  "latest"
    val pollTimeout = "5000"

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka

    val sparkConf = new SparkConf()
      .setAppName(SparkKafkaConsumer.getClass.getName).setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    ssc.sparkContext.setLogLevel("ERROR")
    val topicsSet = topicc.split(",").toSet

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      "spark.kafka.poll.time" -> pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )

    val valuesDStream: DStream[String] = messagesDStream.map(_.value())

    val pDStream: DStream[PaymentwId] = valuesDStream.map(parsePaymentwID)

    pDStream.foreachRDD { (rdd: RDD[PaymentwId], time: Time) =>
      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count
        println("count received " + count)
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val ds = spark.createDataset(rdd)
        println("show 20 rows of dataset")
        ds.show
        ds.createOrReplaceTempView("payments")
        println("top physician specialties by amount paid")
        spark.sql("select physician_specialty, count(*) as cnt, sum(amount) as total from payments group by physician_specialty order by total desc").show()
        ds.saveToMapRDB(tableName, createTable = false, idFieldPath = "_id")

      }
    }

    ssc.start()
    ssc.awaitTermination()

    ssc.stop(stopSparkContext = true, stopGracefully = true)
  }

}
