package transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import  service.DataPipeline
import org.apache.spark.sql.functions._

object NullCheck {
  def nullCheck(df1cast:DataFrame,df2cast:DataFrame):(DataFrame,DataFrame)={

    try {
      val df1notnull = df1cast.na.drop(Seq("id"))
      val df2notnull = df2cast.na.drop(Seq("item_id"))

      //defining placeholders for null values in columns
      val placeholderClickstream=Map(
        "id"->" ",
        "event_timestamp"->"%",
        "device_type"->"%",
        "session_id"-> "%",
        "visitor_id"->"%",
        "item_id"->"%",
        "redirection_source"->"%"
      )

      val placeholderItemset= Map(
        "item_id" -> "%",
        "item_price" -> 0.0,
        "product_type" -> "%",
        "department_name" -> "%"
      )
      val replacedNullClickstream=df1notnull.na.fill(placeholderClickstream)
      val replacedNullItemset=df2notnull.na.fill(placeholderItemset)


     // for showing null records in seperate file
      val removedRecordsFromDf1 = df1cast.except(df1notnull)
      val removedRecordsFromDf2 = df2cast.except(df2notnull)

      val clickstreamnulls=ConfigFactory.load("application.conf").getString("output.clickstreamnullpath")
      val itemsetnulls=ConfigFactory.load("application.conf").getString("output.itemsetnullpath")

      // Store Removed Records in Error Files
      removedRecordsFromDf1.repartition(1).write.option("header","true").mode("overwrite").csv(clickstreamnulls)
      removedRecordsFromDf2.repartition(1).write.option("header","true").mode("overwrite").csv(itemsetnulls)

      return (replacedNullClickstream,replacedNullItemset  )
    }
    catch {
      case e: Exception =>
//
        DataPipeline.logger.error("An error occured due to failure of null removal. ",e )
        (df1cast,df2cast)
    }



  }
}
