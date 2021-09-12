package com.viettel.dashboard.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}
import scala.io.Source

object UtilFunctions extends Serializable {
  val sparkAppConf = new SparkConf
  //Set all Spark Configs
  val sparkProps = new Properties
  val connectionProps = new Properties
  sparkProps.load(Source.fromFile("src/main/resources/spark.conf").bufferedReader())
  connectionProps.load(Source.fromFile("src/main/resources/application.conf").bufferedReader())
  JdbcDialects.registerDialect(HiveDialect)

  def getFirstDateCurrentMonth(): String = {
    val calender = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    calender.add(Calendar.MONTH, 0)
    calender.set(Calendar.DAY_OF_MONTH, calender.getActualMinimum(Calendar.DAY_OF_MONTH))
    val miliscFormat = new Date(calender.getTimeInMillis)
    dateFormat.format(miliscFormat)
  }

  def getLastDatePreviousMonth(): String = {
    val calender = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    calender.add(Calendar.MONTH, -1)
    calender.set(Calendar.DAY_OF_MONTH, calender.getActualMaximum(Calendar.DAY_OF_MONTH))
    val miliscFormat = new Date(calender.getTimeInMillis)
    dateFormat.format(miliscFormat)
  }

  def getFirstDatePreviousMonth(): String = {
    val calender = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    calender.add(Calendar.MONTH, -1)
    calender.set(Calendar.DAY_OF_MONTH, calender.getActualMinimum(Calendar.DAY_OF_MONTH))
    val miliscFormat = new Date(calender.getTimeInMillis)
    dateFormat.format(miliscFormat)
  }

  def getPreviousDateStr(): String = {
    val calender = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    calender.add(Calendar.DAY_OF_YEAR, -1)
    val miliscFormat = new Date(calender.getTimeInMillis)
    dateFormat.format(miliscFormat)
  }

  def getTwoDaysPriorDateStr(): String = {
    val calender = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    calender.add(Calendar.DAY_OF_YEAR, -2)
    val miliscFormat = new Date(calender.getTimeInMillis)
    dateFormat.format(miliscFormat)
  }

  def getSparkAppConf(props: Properties, sparkAppConf: SparkConf): SparkConf = {
    import scala.collection.JavaConverters._
    props.asScala.foreach(kv => sparkAppConf.set(kv._1, kv._2))
    sparkAppConf
  }

  def getServiceName(className: String): String = {
    className.replaceAll("""(?=^)(\w+)\s(\w+.\w+.\w+.\w+.)""", "").replaceAll("""(?:\$)(?=$)""", "")
  }

  def getNewestDailyData(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", connectionProps.getProperty("hive.driver"))
      .option("url", connectionProps.getProperty("hive.url"))
      .option("dbtable", s"(select * from $tableName where input_time = '$getPreviousDateStr')")
      .option("user", connectionProps.getProperty("hive.username"))
      .option("password", connectionProps.getProperty("hive.password").replaceAll("""^\"|\"$""", ""))
      .option("fetchsize", 10000)
      .load()
  }

  def getSubInfrastructureDataPreviousMonth(spark: SparkSession): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", connectionProps.getProperty("hive.driver"))
      .option("url", connectionProps.getProperty("hive.url"))
      .option("dbtable", s"(select * from sub_infrastructure where input_time = '$getLastDatePreviousMonth')")
      .option("user", connectionProps.getProperty("hive.username"))
      .option("password", connectionProps.getProperty("hive.password").replaceAll("""^\"|\"$""", ""))
      .option("fetchsize", 10000)
      .load()
  }

  def getDayBeforeNewestDailyData(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", connectionProps.getProperty("hive.driver"))
      .option("url", connectionProps.getProperty("hive.url"))
      .option("dbtable", s"(select * from $tableName where input_time = '$getTwoDaysPriorDateStr')")
      .option("user", connectionProps.getProperty("hive.username"))
      .option("password", connectionProps.getProperty("hive.password").replaceAll("""^\"|\"$""", ""))
      .option("fetchsize", 10000)
      .load()
  }

  def getNewestMonthlyData(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("driver", connectionProps.getProperty("hive.driver"))
      .option("url", connectionProps.getProperty("hive.url"))
      .option("dbtable", s"(select * from $tableName where input_time = '$getFirstDatePreviousMonth')")
      .option("user", connectionProps.getProperty("hive.username"))
      .option("password", connectionProps.getProperty("hive.password").replaceAll("""^\"|\"$""", ""))
      .option("fetchsize", 10000)
      .load()
  }

  def saveDataToDatabase(tableName: String, dataFrame: DataFrame): Unit = {
    dataFrame.write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://10.240.192.150:3306/vts_report")
      .option("dbtable", s"$tableName")
      .option("user", "vts_report")
      .option("password", "vts_report#123")
      .mode(SaveMode.Append)
      .save()
  }

  def getProductCodeData(spark: SparkSession): DataFrame = {
    spark.read.option("header", "true").csv("src/main/resources/d_vts_product.csv")
  }
}
