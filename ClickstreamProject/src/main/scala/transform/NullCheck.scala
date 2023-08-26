package transform

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql._
import  service.DataPipeline
import org.apache.spark.sql.functions._

object NullCheck {
  def nullCheck(df1cast:DataFrame,df2cast:DataFrame):(DataFrame,DataFrame)={

    var df1notnull: DataFrame = null
    var df2notnull: DataFrame = null

    try {
      val df1notnull = df1cast.na.drop(Seq("id"))
      val df2notnull = df2cast.na.drop(Seq("item_id"))

      val removedRecordsFromDf1 = df1cast.except(df1notnull)
      val removedRecordsFromDf2 = df2cast.except(df2notnull)

      val clickstreamnulls=ConfigFactory.load("application.conf").getString("output.clickstreamnullpath")
      val itemsetnulls=ConfigFactory.load("application.conf").getString("output.itemsetnullpath")

      // Store Removed Records in Error Files
      removedRecordsFromDf1.repartition(1).write.option("header","true").mode("overwrite").csv(clickstreamnulls)
      removedRecordsFromDf2.repartition(1).write.option("header","true").mode("overwrite").csv(itemsetnulls)

      return (df1notnull, df2notnull)
    }
    catch {
      case e: Exception =>
//
        DataPipeline.logger.error("An error occured due to failure of null removal. ",e )
        df1notnull = null
        df2notnull = null
    }

    if (df1notnull != null && df2notnull != null) {
      (df1notnull, df2notnull)
    } else {
      (df1cast,df2cast)
    }

  }
}
