package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.DataPipeline

object RenameColumn {
  def renameColumn(df1lowercase:DataFrame,df2lowercase:DataFrame):(DataFrame,DataFrame)={
    try{
      val df1rename: DataFrame = df1lowercase.withColumnRenamed("id", "Entity_id")
        .withColumnRenamed("device_type", "device_type_t")
        .withColumnRenamed("session_id", "visitor_session_c")
        .withColumnRenamed("redirection_source", "redirection_source_t") //.printSchema()
      val df2rename: DataFrame = df2lowercase.withColumnRenamed("item_price", "item_unit_price_a")
        .withColumnRenamed("product_type", "product_type_c")
        .withColumnRenamed("department_name", "department_n")
      (df1rename, df2rename)
    }
    catch{
      case e:Exception=>
        DataPipeline.logger.error("error occurred while renaming columns ",e)
        (df1lowercase,df2lowercase)
    }
  }
}
