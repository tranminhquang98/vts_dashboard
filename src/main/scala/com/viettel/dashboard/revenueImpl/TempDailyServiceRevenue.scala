package com.viettel.dashboard.revenueImpl

import com.viettel.dashboard.revenue.Revenue
import com.viettel.dashboard.utils.UtilFunctions.{getFirstDateCurrentMonth, getNewestDailyData, getNewestMonthlyData, saveDataToDatabase}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TempDailyServiceRevenue extends Serializable with Revenue{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  override def aggregateRevenue(spark: SparkSession): Unit = {
    val tableDestination = "f_vts_services_revenue_temp"
    val sub_usage_charge = getNewestDailyData(spark, "sub_usage_charge")
    sub_usage_charge.createOrReplaceTempView("sub_usage_charge")

    //LeasedLine
    val sub_infrastructure = getNewestDailyData(spark, "sub_infrastructure")
    sub_infrastructure.createOrReplaceTempView("sub_infrastructure")
    val sub_infrastructure_with_max_id = spark.sql(
      """
        |SELECT *
        |FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY sub_id ORDER BY sub_infrastructure_id DESC) AS rank_infra_id FROM sub_infrastructure)
        |WHERE rank_infra_id=1
        |""".stripMargin)
    sub_infrastructure_with_max_id.createOrReplaceTempView("sub_infrastructure_with_max_id")

    val f_vts_services_revenue_temp_ll = spark.sql(
      s"""
         |SELECT date, province_code, product_code, SUM(revenue) revenue FROM (
         |SELECT
         |	  to_date(a.input_time, 'yyyyMMdd') AS date,
         |	  b.province AS province_code,
         |	  '01' AS product_code,
         |	  nvl(CAST(a.dom_charge AS DECIMAL(20,6)),0) +nvl(CAST(a.ser_charge AS DECIMAL(20,6)),0) + nvl(CAST(a.int_charge AS DECIMAL(20,6)),0) AS revenue
         |FROM sub_usage_charge a
         | 	LEFT JOIN sub_infrastructure_with_max_id b ON a.sub_id = b.sub_id
         |WHERE 1 = 1
         |	  AND a.bill_cycle = '$getFirstDateCurrentMonth'
         |	  AND a.telecom_service_id = '5')
         |GROUP BY date, province_code, product_code
         |""".stripMargin)
    //f_vts_services_revenue_temp_ll.printSchema()
    //println(f_vts_services_revenue_temp_ll.count())
    //println(sub_usage_charge.filter(col("telecom_service_id")==="5").count())

    //Kenh truyen
    val collection_management = getNewestMonthlyData(spark, "collection_management")
    collection_management.select("contract_id", "pay_area_code", "applied_cycle", "status", "input_time").filter(col("status")==="1").createOrReplaceTempView("collection_management")
    val f_vts_services_revenue_temp_kt = spark.sql(
      s"""
         |SELECT date, province_code, product_code, SUM(revenue) revenue FROM (
         |SELECT
         |	 to_date(a.input_time, 'yyyyMMdd') AS date,
         |	 substr(b.pay_area_code,0,4) AS province_code,
         |	 '02' AS product_code,
         |	 nvl(CAST(a.dom_charge AS DECIMAL(20,6)),0) +nvl(CAST(a.ser_charge AS DECIMAL(20,6)),0) + nvl(CAST(a.int_charge AS DECIMAL(20,6)),0) AS revenue
         |FROM sub_usage_charge a
         |	 LEFT JOIN collection_management b ON a.contract_id = b.contract_id
         |WHERE 1=1
         |	 AND a.telecom_service_id IN ('11','12','29'))
         |GROUP BY date, province_code, product_code
         |""".stripMargin)
    //f_vts_services_revenue_temp_kt.printSchema()
    //println(f_vts_services_revenue_temp_kt.count())
    //println(sub_usage_charge.filter(col("telecom_service_id").isin("11","12","29")) .count())

    val f_vts_services_revenue_temp = f_vts_services_revenue_temp_ll.union(f_vts_services_revenue_temp_kt)
    //println(f_vts_services_revenue_temp.count())
    //f_vts_services_revenue_temp.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").mode(SaveMode.Overwrite).csv(s"data\\f_vts_services_revenue_temp\\$getPreviousDateStr")

    saveDataToDatabase(tableDestination, f_vts_services_revenue_temp)
  }

}
