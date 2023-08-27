package database

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.FileReader
import DatabaseConnection._
import  service.DataPipeline
object DatabaseWrite {
  //val joinedDF=FileReader.readJoinedDataSet()
  def writeToMySQL(dataFrame: DataFrame, tableName: String): Unit = {
   try {
     dataFrame.write
       .format("jdbc")
       .mode("overwrite") //for overwriting existing table
       .option("driver", "com.mysql.cj.jdbc.Driver") //specifying type of driver
       .option("url", constant.jdbcUrl)
       .option("dbtable", tableName) // give table name to insert data
       .option("user", constant.jdbcUser)
       .option("password", constant.jdbcPassword)
       .save()
   }
    catch {
      case  e: Exception=>
        DataPipeline.logger.error("error occurred while loading into mysql table ", e)

   }
  }
  }
