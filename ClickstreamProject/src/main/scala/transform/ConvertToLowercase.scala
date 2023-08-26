package transform

//import dataevents.data_cleaning.convertToLowercase
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.DataPipeline

object ConvertToLowercase {
  def convertToLowercase(df1duplicate:DataFrame,df2duplicate:DataFrame):(DataFrame,DataFrame)={
   try {
      val df1lowercase = df1duplicate.withColumn("redirection_source", lower(col("redirection_source")))
      val df2lowercase = df2duplicate.withColumn("department_name", lower(col("department_name")))
      (df1lowercase, df2lowercase)
    }
    catch {
      case e:Exception=>
        DataPipeline.logger.error("error occurred while converting columns into lowercase",e)

        (df1duplicate,df2duplicate)
    }
  }
}
