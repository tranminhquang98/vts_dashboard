package com.viettel.dashboard.utils

import org.apache.spark.sql.jdbc.JdbcDialect

object HiveDialect extends JdbcDialect with Serializable {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:hive2")

  override def quoteIdentifier(colName: String): String = s"`$colName`"
}
