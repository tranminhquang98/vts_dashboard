package com.viettel.dashboard.module

import com.viettel.bi.spark.ext.api.SparkCommand
import com.viettel.dashboard.revenueImpl.MonthlyServiceRevenue
import com.viettel.dashboard.utils.RunService
import com.viettel.dashboard.utils.UtilFunctions.getServiceName
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object IntegratedApplication extends SparkCommand with RunService {
  @transient override val logger: Logger = Logger.getLogger(getClass.getName)

  override def exec(sparkSession: SparkSession, args: Array[String], um: mutable.HashMap[String, Object]): Unit = {
    logger.info("Service: " + getServiceName(MonthlyServiceRevenue.getClass.toString) + " is starting")
    val status2 = if (run(sparkSession, MonthlyServiceRevenue)) "Done" else "Error"
    logger.info("Service: " + getServiceName(MonthlyServiceRevenue.getClass.toString) + ", status: " + status2)

    //  logger.info("Service: " + getServiceName(TempDailyServiceRevenue.getClass.toString) + " is starting")
    //  val status3 = if (run(spark, TempDailyServiceRevenue)) "Done" else "Error"
    //  logger.info("Service: " + getServiceName(TempDailyServiceRevenue.getClass.toString) + ", status: " + status3)


    logger.info("Finished Spark Application")
    //scala.io.StdIn.readLine()
    sparkSession.stop()
  }
}
