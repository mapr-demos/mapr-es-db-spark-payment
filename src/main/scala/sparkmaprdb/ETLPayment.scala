package sparkmaprdb

import org.apache.spark._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import com.mapr.db._
import com.mapr.db.spark._
import com.mapr.db.spark.impl._
import com.mapr.db.spark.sql._

object ETLPayment {

  case class Payment(physician_id: String, date_payment: String, record_id: String, payer: String, amount: Double, physician_specialty: String, nature_of_payment: String) extends Serializable

  case class PaymentwId(_id: String, physician_id: String, date_payment: String, payer: String, amount: Double, physician_specialty: String,
    nature_of_payment: String) extends Serializable

  val schema = StructType(Array(
    StructField("_id", StringType, true),
    StructField("physician_id", StringType, true),
    StructField("date_payment", StringType, true),
    StructField("payer", StringType, true),
    StructField("amount", DoubleType, true),
    StructField("physician_specialty", StringType, true),
    StructField("nature_of_payment", StringType, true)
  ))

  def createPaymentwId(p: Payment): PaymentwId = {
    val id = p.physician_id + '_' + p.date_payment + '_' + p.record_id
    PaymentwId(id, p.physician_id, p.date_payment, p.payer, p.amount, p.physician_specialty, p.nature_of_payment)
  }

  def main(args: Array[String]) {

    var pfile = "hdfs:///tmp/payments.csv"
    var tableName: String = "/mapr/maprdemo.mapr.io/apps/payments"

    if (args.length == 2) {
      pfile = args(0)
      tableName = args(1)
    } else {
      System.out.println("Using hard coded parameters unless you specify the consume import file and table. <file table>   ")
    }

    val spark: SparkSession = SparkSession.builder().appName("uber").master("local[*]").getOrCreate()
    val toDouble = udf[Double, String](_.toDouble)
    val df = spark.read.option("header", "true").csv(pfile)

    val df2 = df.withColumn("amount", toDouble(df("Total_Amount_of_Payment_USDollars")))
    df2.first
    df2.createOrReplaceTempView("payments")

    import spark.implicits._
    val ds: Dataset[Payment] = spark.sql("select Physician_Profile_ID as physician_id, Date_of_Payment as date_payment, Record_ID as record_id, Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name as payer,  amount, Physician_Specialty, Nature_of_Payment_or_Transfer_of_Value as Nature_of_payment from payments ").as[Payment]
    ds.cache
    ds.count
    ds.createOrReplaceTempView("payments")
    ds.show
    spark.sql("select physician_specialty, count(*) as cnt, sum(amount)as total from payments group by physician_specialty order by total desc").show()

    ds.filter($"amount" > 1000).show()
    ds.groupBy("Nature_of_payment").count().orderBy(desc("count")).show()

    val ds2: Dataset[PaymentwId] = ds.map(payment => createPaymentwId(payment))
    ds2.saveToMapRDB(tableName, createTable = false, idFieldPath = "_id")

    val pdf: Dataset[PaymentwId] = spark.sparkSession.loadFromMapRDB[PaymentwId](tableName, schema).as[PaymentwId]

    pdf.filter(_.physician_id == "214250").show

    pdf.select("_id", "physician_id").show

    pdf.groupBy("Nature_of_payment").count().orderBy(desc("count")).show()

  }
}

