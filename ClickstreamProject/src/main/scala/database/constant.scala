package database

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object constant {
  val jdbcUrl = "jdbc:mysql://localhost:3306/clickstream_data_events"
  val jdbcUser = "root"
  val jdbcPassword = "Admin@123"
}
