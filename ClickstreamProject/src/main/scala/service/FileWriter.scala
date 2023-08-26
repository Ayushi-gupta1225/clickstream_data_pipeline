package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import transform.ConvertToLowercase

import service.DataPipeline

object FileWriter {
  def fileWriter(df1rename:DataFrame, df2rename:DataFrame,outputPath:String): DataFrame = {
    try {
      // Join both the dataframes
      df1rename.printSchema()
      df2rename.printSchema()
      val finalDF: DataFrame = df1rename.join(df2rename, Seq("item_id"))

      // Show the final DataFrame
      finalDF.show()

      // Write the final dataframe to a csv file
      finalDF.repartition(1).write.mode("overwrite").option("header", "true").csv(outputPath)
      finalDF
    } catch {
      case e: Exception =>

        DataPipeline.logger.error("no dataframe to load in mysql table")
        // You can handle the error here, such as returning an empty DataFrame or rethrowing the exception
       null // Returning one of the input DataFrames as an example
    }
  }
}
