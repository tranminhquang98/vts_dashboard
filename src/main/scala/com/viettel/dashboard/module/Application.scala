package com.viettel.dashboard.module

import com.viettel.dashboard.revenueImpl.TempDailyServiceRevenue
import com.viettel.dashboard.utils.RunService
import com.viettel.dashboard.utils.UtilFunctions.{getServiceName, getSparkAppConf, sparkAppConf, sparkProps}
import org.apache.spark.sql.SparkSession

object Application extends App with Serializable with RunService {
  logger.info("Starting Spark Application")
  val spark = SparkSession.builder()
    .config(getSparkAppConf(sparkProps, sparkAppConf))
    .getOrCreate()

//    logger.info("Service: " + getServiceName(SaleRevenue.getClass.toString) + " is starting")
//    val status = if (run(spark, SaleRevenue)) "Done" else "Error"
//    logger.info("Service: " + getServiceName(SaleRevenue.getClass.toString) + ", status: " + status)

//  logger.info("Service: " + getServiceName(MonthlyServiceRevenue.getClass.toString) + " is starting")
//  val status2 = if (run(spark, MonthlyServiceRevenue)) "Done" else "Error"
//  logger.info("Service: " + getServiceName(MonthlyServiceRevenue.getClass.toString) + ", status: " + status2)

  logger.info("Service: " + getServiceName(TempDailyServiceRevenue.getClass.toString) + " is starting")
  val status3 = if (run(spark, TempDailyServiceRevenue)) "Done" else "Error"
  logger.info("Service: " + getServiceName(TempDailyServiceRevenue.getClass.toString) + ", status: " + status3)


  logger.info("Finished Spark Application")
  //scala.io.StdIn.readLine()
  spark.stop()
}
