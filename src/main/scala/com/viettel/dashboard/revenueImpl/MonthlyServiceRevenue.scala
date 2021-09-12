package com.viettel.dashboard.revenueImpl

import com.viettel.dashboard.revenue.Revenue
import com.viettel.dashboard.utils.UtilFunctions._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object MonthlyServiceRevenue extends Serializable with Revenue{
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  override def aggregateRevenue(spark: SparkSession): Unit = {
    val tableDestination = "f_vts_services_revenue_close_month"
    //LeasedLine
    val sub_infrastructure = getSubInfrastructureDataPreviousMonth(spark)
    sub_infrastructure.createOrReplaceTempView("sub_infrastructure")
    val sub_infrastructure_with_max_id = spark.sql(
      """
        |SELECT *
        |FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY sub_id ORDER BY sub_infrastructure_id DESC) AS rank_infra_id FROM sub_infrastructure)
        |WHERE rank_infra_id=1
        |""".stripMargin)
    sub_infrastructure_with_max_id.createOrReplaceTempView("sub_infrastructure_with_max_id")

    //Kenh truyen
    val collection_management = getNewestMonthlyData(spark, "collection_management")
    collection_management.select("contract_id", "pay_area_code", "applied_cycle", "status", "input_time").filter(col("status")==="1" && col("applied_cycle")===s"$getFirstDatePreviousMonth").createOrReplaceTempView("collection_management")

    //Fetch all tables needed
    val tables = List("sub_usage_charge_freeze", "sub_promotion_detail", "adjustment_detail", "rp_allocate_prepaid", "rp_allocate_reserve")
    val reader = spark.read
      .format("jdbc")
      .option("driver", connectionProps.getProperty("hive.driver"))
      .option("url", connectionProps.getProperty("hive.url"))
      .option("user", connectionProps.getProperty("hive.username"))
      .option("password", connectionProps.getProperty("hive.password").replaceAll("""^\"|\"$""", ""))
      .option("fetchsize", 10000)

    tables.foreach(tableName => {
      reader.option(s"dbtable", s"(select * from $tableName where input_time = '$getFirstDatePreviousMonth')").load().createOrReplaceTempView(tableName)
    })

    //1. Cuoc thu tu khach hang
    //1.1. Cuoc phat sinh chot thang
    val money_arise = spark.sql(
      s"""
        |SELECT date,province_code, product_code, ROUND(SUM(money_arise)) as money_arise from (
        |SELECT
        |	to_date('$getFirstDatePreviousMonth', 'yyyyMMdd') AS date,
        |	CASE
        |		WHEN telecom_service_id = '5' THEN c.province
        |     	WHEN telecom_service_id in ('11','12','29') THEN substr(b.pay_area_code,0,4)
        |	END AS province_code,
        |	CASE
        |		WHEN telecom_service_id = '5' THEN '01'
        |     	WHEN telecom_service_id in ('11','12','29') THEN '02'
        |	END AS product_code,
        |	(NVL(CAST(a.dom_charge AS DECIMAL(20,6)),0) + NVL(CAST(a.ser_charge AS DECIMAL(20,6)),0)) * 1.1 as money_arise
        |FROM sub_usage_charge_freeze a
        |	LEFT JOIN collection_management b ON a.contract_id = b.contract_id
        |	LEFT JOIN sub_infrastructure_with_max_id c ON a.sub_id = c.sub_id
        |WHERE 1=1
        |	AND a.bill_cycle = '$getFirstDatePreviousMonth'
        |) GROUP BY date,province_code, product_code
        |ORDER BY province_code asc
        |""".stripMargin)

    //1.2. Cuoc khuyen mai
    val money_promotion = spark.sql(
      s"""
        |select date,province_code, product_code, SUM(money_promotion) as money_promotion from (
        |SELECT
        |	to_date('$getFirstDatePreviousMonth', 'yyyyMMdd') AS date,
        |	CASE
        |		WHEN telecom_service_id = '5' THEN c.province
        |     	WHEN telecom_service_id in ('11','12','29') THEN substr(b.pay_area_code,0,4)
        |	END AS province_code,
        |	CASE
        |		WHEN telecom_service_id = '5' THEN '01'
        |     	WHEN telecom_service_id in ('11','12','29') THEN '02'
        |	END AS product_code,
        |	1.1 * CAST(a.amount AS DECIMAL(20,6)) as money_promotion
        |FROM sub_promotion_detail a
        |	LEFT JOIN collection_management b ON a.contract_id = b.contract_id
        |	LEFT JOIN sub_infrastructure_with_max_id c ON a.sub_id = c.sub_id
        |WHERE 1 = 1
        |	AND a.bill_cycle = '$getFirstDatePreviousMonth'
        |) GROUP BY date,province_code, product_code
        |ORDER BY province_code asc
        |""".stripMargin)

    //1.3. Cuoc dieu chinh
    val money_adjustment = spark.sql(
      s"""
        |SELECT date, province_code, product_code, ROUND(sum(money_adjustment)) money_adjustment from (
        |SELECT
        | 	to_date('$getFirstDatePreviousMonth', 'yyyyMMdd') AS date,
        |  	CASE
        |    	WHEN telecom_service_id IN ('11','12','29') THEN substr(b.pay_area_code,0,4)
        |    	WHEN telecom_service_id = '5' THEN c.province
        |  	END AS province_code,
        |  	CASE
        |    	WHEN telecom_service_id IN ('11','12','29') THEN '02'
        |    	WHEN telecom_service_id = '5' THEN '01'
        |  	END AS product_code,
        |  	CAST(a.amount as DECIMAL(20,6)) as money_adjustment
        |FROM adjustment_detail a
        |  	LEFT JOIN collection_management b ON a.contract_id = b.contract_id
        |  	LEFT JOIN sub_infrastructure_with_max_id c ON a.sub_id = c.sub_id
        |WHERE 1=1
        |	AND a.bill_cycle = '$getFirstDatePreviousMonth'
        |) GROUP BY date, province_code, product_code
        |ORDER BY province_code asc
        |""".stripMargin)

    //1.4. Cuoc dong truoc phan bo
    val money_allocate = spark.sql(
      s"""
        |SELECT
        |to_date('$getFirstDatePreviousMonth','yyyyMMdd') date,
        |province_code,
        |CASE
        |     WHEN telecom_service_id in ('11','12','29') THEN '02'
        |     WHEN telecom_service_id = '5' THEN '01'
        |END AS product_code,
        |ROUND(sum(money_allocate)) money_allocate
        |FROM (
        |SELECT
        |nvl(a.sub_id, b.sub_id) sub_id,
        |nvl(a.province, b.province) province_code,
        |nvl(a.telecom_service_id, b.telecom_service_id) telecom_service_id,
        |nvl(CAST(a.allocate_value_not_vat AS DECIMAL(20,6)),0) + nvl(CAST(b.reserve_usage_not_vat AS DECIMAL(20,6)),0) + nvl(CAST(b.prom_reserve_usage_not_vat AS DECIMAL(20,6)),0) money_allocate
        |FROM rp_allocate_prepaid a
        |FULL OUTER JOIN rp_allocate_reserve b
        |ON a.sub_id = b.sub_id) GROUP BY province_code, product_code ORDER BY province_code DESC
        |""".stripMargin)

    money_adjustment.createOrReplaceTempView("money_adjustment")
    money_allocate.createOrReplaceTempView("money_allocate")
    money_arise.createOrReplaceTempView("money_arise")
    money_promotion.createOrReplaceTempView("money_promotion")

    val f_vts_services_revenue_close_month = spark.sql(
      """
        |SELECT date, province_code, product_code, sum(money_arise) money_arise, sum(money_promotion) money_promotion, sum(money_adjustment) money_adjustment, sum(money_allocate) money_allocate, sum(revenue) revenue FROM (
        |SELECT
        |coalesce(a.date,b.date,c.date,d.date) date,
        |coalesce(a.province_code,b.province_code,c.province_code,d.province_code) province_code,
        |coalesce(a.product_code,b.product_code,c.product_code,d.product_code) product_code,
        |nvl(c.money_arise,0) money_arise,
        |nvl(d.money_promotion,0) money_promotion,
        |nvl(a.money_adjustment,0) money_adjustment,
        |nvl(b.money_allocate,0) money_allocate,
        |(nvl(c.money_arise,0) - nvl(d.money_promotion,0) + nvl(a.money_adjustment,0) + nvl(b.money_allocate,0)) revenue
        |FROM money_adjustment a
        |FULL OUTER JOIN money_allocate b ON (a.date = b.date AND a.province_code = b.province_code AND a.product_code = b.product_code)
        |FULL OUTER JOIN money_arise c ON (a.date = c.date AND a.province_code = c.province_code AND a.product_code = c.product_code)
        |FULL OUTER JOIN money_promotion d ON (a.date = d.date AND a.province_code = d.province_code AND a.product_code = d.product_code))
        |GROUP BY date, province_code, product_code
        |""".stripMargin)

    //f_vts_services_revenue_close_month.show
    //f_vts_services_revenue_close_month.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").mode(SaveMode.Overwrite).csv(s"data\\f_vts_services_revenue_close_month\\monthly_service_revenue\\$getFirstDatePreviousMonth")

    saveDataToDatabase(tableDestination, f_vts_services_revenue_close_month)
  }
}
