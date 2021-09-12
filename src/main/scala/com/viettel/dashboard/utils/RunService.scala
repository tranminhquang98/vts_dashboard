package com.viettel.dashboard.utils

import com.viettel.dashboard.revenue.Revenue
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

trait RunService extends Serializable {
  @transient val logger: Logger = Logger.getLogger(getClass.getName)

  def run(spark: SparkSession, service: Revenue): Boolean = {
    try {
      service.aggregateRevenue(spark)
      true
    } catch {
      case e: Exception =>
        logger.error("Error: ", e)
        false
    }
  }
}
