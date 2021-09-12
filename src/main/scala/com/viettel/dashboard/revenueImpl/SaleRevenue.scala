package com.viettel.dashboard.revenueImpl

import com.viettel.dashboard.revenue.Revenue
import com.viettel.dashboard.utils.UtilFunctions.{getNewestDailyData, getProductCodeData, saveDataToDatabase}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object SaleRevenue extends Serializable with Revenue {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  override def aggregateRevenue(spark: SparkSession): Unit = {
    val tableDestination = "f_vts_sale_revenue"
    val shopDF = getNewestDailyData(spark, "shop")
    val rp_daily_revenueDF = getNewestDailyData(spark, "rp_daily_revenue")
    val productCodeDF = getProductCodeData(spark)

    shopDF.createOrReplaceTempView("shop")
    rp_daily_revenueDF.createOrReplaceTempView("rp_daily_revenue")
    productCodeDF.createOrReplaceTempView("product_code")

    lazy val saleRevenueDF = spark.sql(
      """
        |SELECT
        |to_date(a.sale_trans_date, 'yyyyMMdd') date,
        |b.province province_code,
        |c.product_code product_code,
        |sum(nvl(CAST(a.amount AS DECIMAL(20,6)),0) - nvl(CAST(a.discount_amount_tax AS DECIMAL(20,6)), ROUND((nvl(CAST(a.discount_amount AS DECIMAL(20,6)),0) + nvl(CAST(a.discount_amount AS DECIMAL(20,6)),0) * nvl(CAST(a.price_vat AS DECIMAL(20,6)),0) / 100) / 2) * 2) - nvl(CAST(a.vat_amount AS DECIMAL(20,6)),0)) revenue
        |FROM rp_daily_revenue a, shop b, product_code c
        |WHERE a.shop_id = b.shop_id
        |AND array_contains(split(c.accounting_code,';'), a.accounting_code)
        |AND a.sale_trans_status = '3'
        |AND a.accounting_code in ('04.03',
        |      '09.10.10',
        |      '09.10.12',
        |      '09.10.13',
        |      '09.10.05',
        |      '09.10.07',
        |      '09.10.11',
        |      '09.10.01',
        |      '09.10.08',
        |      '09.10.15',
        |      '09.10.06',
        |      '09.10.17',
        |      '09.10.09',
        |      '09.10.03',
        |      '09.10.14',
        |      '09.10.18',
        |      '09.18.08',
        |      '09.18.07',
        |      '09.18.06',
        |      '09.18.04',
        |      '09.18.01',
        |      '09.18.03',
        |      '09.18.09',
        |      '09.18.05',
        |      '09.18.02',
        |      '02.10.1.6',
        |      '02.10.1.5',
        |      '09.13.11',
        |      '03.05.02',
        |      '09.25.21',
        |      '09.03.01',
        |      '09.14.01',
        |      '09.14.02',
        |      '02.10.6',
        |      '06.04',
        |      '09.25.15',
        |      '09.19.01',
        |      '09.25.22',
        |      '09.25.06',
        |      '09.20.01',
        |      '09.25.02',
        |      '09.11.01')
        |GROUP BY date, b.province, c.product_code
      """.stripMargin)

    //saleRevenueDF.coalesce(1).write.option("header", "true").option("encoding", "UTF-8").mode(SaveMode.Overwrite).csv(s"data/f_vts_sale_revenue/$getPreviousDateStr")

    saveDataToDatabase(tableDestination, saleRevenueDF)
  }
}
