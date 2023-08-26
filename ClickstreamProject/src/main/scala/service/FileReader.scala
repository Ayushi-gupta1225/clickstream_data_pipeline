package service

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import service.DataPipeline
import utils.sparksession
import org.apache.spark.sql.functions._

object FileReader {

  //Method to convert the output file to DF
//  def readJoinedDataSet():DataFrame= {
//    val spark = sparksession.sparkSession()
//    val OutPutPath = ConfigFactory.load("application.conf").getString("output.path")
//    val JoinedDF = spark.read.option("header", "true").option("inferSchema", "true").csv(OutPutPath)
//    JoinedDF
//  }

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
        DataPipeline.logger.error("error occurred while reading datasets")
        // You can handle the error here, such as returning an empty DataFrame or rethrowing the exception
         //Returning an empty DataFrame as an example
    }
  }
}
