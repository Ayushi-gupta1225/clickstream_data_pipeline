package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import service.DataPipeline
import utils.sparksession
import org.apache.spark.sql.functions._

object FileReader {


  //to read application config file
  def readConfig(): SparkConf = {
    val config = ConfigFactory.load("application.conf")
    val sparkAppName = config.getString("spark.appName")
    val sparkMaster = config.getString("spark.master")
    new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
  }

  //to read the input csv file
  def readDataFrame(spark:SparkSession, inputpath:String): DataFrame= {
    try {
      val dataframe = spark.read.option("header", "true").option("inferSchema", "true").csv(inputpath)
      dataframe
    } catch {
      case e: Exception =>
        //
        DataPipeline.logger.error("error occurred while reading datasets",e)
         null

    }
  }
}
