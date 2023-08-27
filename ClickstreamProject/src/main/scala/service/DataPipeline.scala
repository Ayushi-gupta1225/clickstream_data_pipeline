package service

import com.typesafe.config.ConfigFactory

import org.slf4j.{Logger,LoggerFactory}
import transform.{CastDataTypes, ConvertToLowercase, NullCheck, RemoveDuplicates, RenameColumn}
import utils.sparksession

import java.util.logging.Logger

object DataPipeline {
  val logger:Logger=LoggerFactory.getLogger(getClass)
  def dataPipeline():Unit={
    val spark=sparksession.sparkSession()

    //reading input datasets
    val inputPath_clickstream = ConfigFactory.load("application.conf").getString("input.path1")
    val inputPath_itemset = ConfigFactory.load("application.conf").getString("input.path2")

    // Write processed data to output path
    val outputPath = ConfigFactory.load("application.conf").getString("output.path")

    // creating two dataframes
    val clickstream_DF=FileReader.readDataFrame(spark,inputPath_clickstream)
    val itemSet_DF=FileReader.readDataFrame(spark,inputPath_itemset)


    //applying all tranformations
    val (df1cast,df2cast)=CastDataTypes.castDataTypes(clickstream_DF,itemSet_DF)
    val (df1removenull,df2removenull)=NullCheck.nullCheck(df1cast,df2cast)
    val (df1duplicates,df2duplicates)=RemoveDuplicates.removeDuplicates(df1removenull,df2removenull)
    val (df1lowercase,df2lowercase)=ConvertToLowercase.convertToLowercase(df1duplicates,df2duplicates)
    val (df1rename,df2rename)=RenameColumn.renameColumn(df1lowercase,df2lowercase)

    // this is the combined dataset
    val joinedDF = FileWriter.fileWriter(df1rename,df2rename,outputPath)
   //DatabaseWrite.writeToMySQL(joinedDF, "cdp")
   joinedDF.show()
  }
}
