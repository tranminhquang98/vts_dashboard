package com.viettel.dashboard.revenue

import org.apache.spark.sql.SparkSession

trait Revenue extends Serializable {
  def aggregateRevenue(spark: SparkSession): Unit = {
  }
}
