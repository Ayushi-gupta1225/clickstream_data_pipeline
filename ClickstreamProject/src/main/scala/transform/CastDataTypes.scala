package transform

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import service.{DataPipeline, FileReader}

object CastDataTypes {
  def castDataTypes(df1:DataFrame,df2:DataFrame):(DataFrame,DataFrame)={
//df.column.Datatype
try
    {
      val df1cast = df1.withColumn("event_timestamp",
        to_timestamp(col("event_timestamp"), "MM/dd/yyyy HH:mm"))
      val df2cast: DataFrame = df2.withColumn("item_price", col("item_price").cast("Double"))
      (df1cast, df2cast)
    }
    catch {
      case e:Exception=>
        DataPipeline.logger.error("error occurred while casting datatypes",e)
        (df1,df2)
    }
  }
}
