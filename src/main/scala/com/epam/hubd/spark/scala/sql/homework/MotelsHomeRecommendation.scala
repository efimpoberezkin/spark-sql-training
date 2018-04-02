package com.epam.hubd.spark.scala.sql.homework

import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.util.Try

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"
  val ERROR_TAG: String = "ERROR_"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    //    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation"))
    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords
      .repartition(1)
      .write
      .format(Constants.CSV_FORMAT)
      .option("header", "false")
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertDate
    sqlContext.udf.register("convertDate", convertDate)

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched
      .repartition(1)
      .write
      .format(Constants.CSV_FORMAT)
      .option("header", "false")
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    sqlContext
      .read
      .parquet(bidsPath)
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    rawBids
      .withColumnRenamed("HU", "ErrorType") // HU is the name of the third column by which erroneous record is recognizable
      .where("ErrorType like '" + ERROR_TAG + "%'")
      .groupBy("BidDate", "ErrorType")
      .count
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    val schema = Constants.EXCHANGE_RATES_HEADER

    sqlContext
      .read
      .format(Constants.CSV_FORMAT)
      .option("header", "false")
      .schema(schema)
      .load(exchangeRatesPath)
      .select(schema.fields(0).name, schema.fields(3).name)
  }

  def getConvertDate: UserDefinedFunction = udf(
    (date: String) => DateTime.parse(date, Constants.INPUT_DATE_FORMAT).toString(Constants.OUTPUT_DATE_FORMAT)
  )

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    rawBids
      .where("HU not like '" + ERROR_TAG + "%'") // HU is the name of the third column by which erroneous record is recognizable
      .createOrReplaceTempView("rawBids")

    val transposedBids =
      rawBids
        .sqlContext.sql(
        """
         select MotelID, BidDate, 'US' as LoSa, US as Price from rawBids
         union all
         select MotelID, BidDate, 'MX' as LoSa, MX as Price from rawBids
         union all
         select MotelID, BidDate, 'CA' as LoSa, CA as Price from rawBids
        """)
        .toDF("MotelID", "BidDate", "LoSa", "Price")
        .filter(row => isDouble(row.getString(3)))

    transposedBids
      .join(exchangeRates, transposedBids("BidDate") === exchangeRates("ValidFrom"), "inner")
      .select(
        col("MotelID"),
        callUDF("convertDate", col("BidDate")) as "BidDate",
        col("LoSa"),
        round(expr("Price * ExchangeRate"), 3) as "Price"
      )
  }

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {
    sqlContext
      .read
      .parquet(motelsPath)
      .select("MotelID", "MotelName")
  }

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    bids
      .join(motels, "MotelID")
      .select(
        col("MotelID"), col("MotelName"), col("BidDate"), col("LoSa"), col("Price"),
        rank.over(Window.partitionBy(col("MotelID"), col("BidDate")).orderBy(desc("Price"))) as "rank"
      )
      .where("rank = 1")
      .select("MotelID", "MotelName", "BidDate", "LoSa", "Price")
      .distinct
  }

  def isDouble(s: String): Boolean = Try(s.toDouble).isSuccess
}
