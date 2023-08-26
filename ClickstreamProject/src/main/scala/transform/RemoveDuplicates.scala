package transform

//import dataevents.data_cleaning.removeDuplicates
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import service.DataPipeline
import org.apache.spark.sql.functions._



object RemoveDuplicates {
  def removeDuplicates(df1removenull:DataFrame,df2removenull:DataFrame):(DataFrame,DataFrame)={
    try {


      val df1Duplicates = df1removenull.dropDuplicates("id")
      val df2Duplicates = df2removenull.dropDuplicates("item_id")

      val clickstreamDuplicates=df1removenull.except(df1Duplicates)
      val itemsetDuplicates=df2removenull.except(df2Duplicates)

      val clickstreamremovedDuplicates=ConfigFactory.load("application.conf").getString("output.clickstreamduplicatespath")
      val itemsetremovedDuplicates=ConfigFactory.load("application.conf").getString("output.itemsetduplicatespath")

      clickstreamDuplicates.repartition(1).write.option("header", "true").mode("overwrite").csv(clickstreamremovedDuplicates)
      itemsetDuplicates.repartition(1).write.option("header", "true").mode("overwrite").csv(itemsetremovedDuplicates)


      (df1Duplicates, df2Duplicates)
    } catch {
      case e: Exception =>
//

        DataPipeline.logger.error("An error occured during removal of duplicate records",e)
        // You can handle the error here, such as returning default DataFrames or rethrowing the exception
        (df1removenull, df2removenull) // Returning original DataFrames as an example
    }
  }
}
