package com.epam.hubd.spark.scala.sql.homework

import java.io.File

import com.epam.hubd.spark.scala.core.util.RddComparator
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.sql.homework.MotelsHomeRecommendationTest._
import com.holdenkarau.spark.testing.RDDComparisons
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._
import org.junit.rules.TemporaryFolder
import org.scalatest.FunSuite

/**
  * Created by Csaba_Bejan on 8/22/2016.
  */
class MotelsHomeRecommendationTest
  extends FunSuite
    with RDDComparisons {

  val _temporaryFolder = new TemporaryFolder

  @Rule
  def temporaryFolder = _temporaryFolder

  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.gz.parquet"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.gz.parquet"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/expected_sql"

  private var outputFolder: File = null

  @Before
  def setup() = {
    outputFolder = temporaryFolder.newFolder("output")
  }

  @Test
  def shouldCollectErroneousRecords() = {
    val rawBidsSchema = StructType(
      Array(StructField("MotelID", StringType),
        StructField("BidDate", StringType),
        StructField("HU", StringType))
    )
    val rawBids = sc.parallelize(
      Seq(
        Row("1", "06-05-02-2016", "ERROR_1"),
        Row("2", "15-04-08-2016", "0.89"),
        Row("3", "07-05-02-2016", "ERROR_2"),
        Row("4", "06-05-02-2016", "ERROR_1"),
        Row("5", "06-05-02-2016", "ERROR_2")
      )
    )
    val rawBidsDF = sqlContext.createDataFrame(rawBids, rawBidsSchema)

    val expected = sc.parallelize(
      Seq(
        ("06-05-02-2016", "ERROR_2", "1"),
        ("07-05-02-2016", "ERROR_2", "1"),
        ("06-05-02-2016", "ERROR_1", "2")
      )
    )
    val expectedDF = sqlContext.createDataFrame(expected)

    val actualDF = MotelsHomeRecommendation.getErroneousRecords(rawBidsDF)

    assert(expectedDF.except(actualDF).count, 0)
    assert(actualDF.except(expectedDF).count, 0)
  }

  @Test
  def shouldTransformRawBids() = {
    sqlContext.udf.register("convertDate", MotelsHomeRecommendation.getConvertDate)

    val rawBidsSchema = StructType(
      Array(StructField("MotelID", StringType),
        StructField("BidDate", StringType),
        StructField("HU", StringType),
        StructField("UK", StringType),
        StructField("NL", StringType),
        StructField("US", StringType),
        StructField("MX", StringType),
        StructField("AU", StringType),
        StructField("CA", StringType))
    )
    val rawBids = sc.parallelize(
      Seq(
        Row("1", "06-05-02-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63"),
        Row("2", "07-05-02-2016", "1.35", "2.02", "1.13", "", "1.33", "2.18", ""),
        Row("3", "07-05-02-2016", "ERROR_1", "2.02", "1.13", "", "1.33", "2.18", "")
      )
    )
    val rawBidsDF = sqlContext.createDataFrame(rawBids, rawBidsSchema)

    val exchangeRatesSchema = StructType(
      Array(StructField("ValidFrom", StringType),
        StructField("ExchangeRate", DoubleType))
    )
    val exchangeRates = sc.parallelize(
      Seq(
        Row("06-05-02-2016", 0.9),
        Row("07-05-02-2016", 0.8)
      )
    )
    val exchangeRatesDF = sqlContext.createDataFrame(exchangeRates, exchangeRatesSchema)

    val expected = sc.parallelize(
      Seq(
        ("1", "2016-02-05 06:00", "US", 0.612),
        ("1", "2016-02-05 06:00", "MX", 1.431),
        ("1", "2016-02-05 06:00", "CA", 1.467),
        ("2", "2016-02-05 07:00", "MX", 1.064)
      )
    )
    val expectedDF = sqlContext.createDataFrame(expected)

    val actualDF = MotelsHomeRecommendation.getBids(rawBidsDF, exchangeRatesDF)

    assert(expectedDF.except(actualDF).count, 0)
    assert(actualDF.except(expectedDF).count, 0)
  }

  @Test
  def shouldEnrichDataAndFindMaximum() = {
    val bidsSchema = StructType(
      Array(StructField("MotelID", StringType),
        StructField("BidDate", StringType),
        StructField("LoSa", StringType),
        StructField("Price", DoubleType)
      )
    )
    val bids = sc.parallelize(
      Seq(
        Row("0000001", "2016-06-02 11:00", "MX", 1.50),
        Row("0000001", "2016-06-02 11:00", "US", 1.50),
        Row("0000001", "2016-06-02 11:00", "CA", 1.15),
        Row("0000001", "2016-06-02 13:00", "US", 1.70),
        Row("0000003", "2016-06-02 11:00", "CA", 1.80),
        Row("0000002", "2016-06-02 12:00", "MX", 1.10),
        Row("0000002", "2016-06-02 12:00", "US", 1.20),
        Row("0000002", "2016-06-02 12:00", "CA", 1.30)
      )
    )
    val bidsDF = sqlContext.createDataFrame(bids, bidsSchema)

    val motelsSchema = StructType(
      Array(StructField("MotelID", StringType),
        StructField("MotelName", StringType))
    )
    val motels = sc.parallelize(
      Seq(
        Row("0000001", "Fantastic Hostel"),
        Row("0000002", "Majestic Ibiza Por Hostel"),
        Row("0000003", "Olinda Windsor Inn")
      )
    )
    val motelsDF = sqlContext.createDataFrame(motels, motelsSchema)

    val expected = sc.parallelize(
      Seq(
        ("0000001", "Fantastic Hostel", "2016-06-02 11:00", "MX", 1.50),
        ("0000001", "Fantastic Hostel", "2016-06-02 11:00", "US", 1.50),
        ("0000001", "Fantastic Hostel", "2016-06-02 13:00", "US", 1.70),
        ("0000003", "Olinda Windsor Inn", "2016-06-02 11:00", "CA", 1.80),
        ("0000002", "Majestic Ibiza Por Hostel", "2016-06-02 12:00", "CA", 1.30)
      )
    )
    val expectedDF = sqlContext.createDataFrame(expected)

    val actualDF = MotelsHomeRecommendation.getEnriched(bidsDF, motelsDF)

    assert(expectedDF.except(actualDF).count, 0)
    assert(actualDF.except(expectedDF).count, 0)
  }

  @Test
  def shouldFilterErrorsAndCreateCorrectAggregates() = {

    runIntegrationTest()

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  @After
  def teardown(): Unit = {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sqlContext, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
    assertRDDEquals(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}

object MotelsHomeRecommendationTest {
  var sc: SparkContext = null
  var sqlContext: HiveContext = null

  @BeforeClass
  def beforeTests() = {
    sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test"))
    sqlContext = new HiveContext(sc)
  }

  @AfterClass
  def afterTests() = {
    sc.stop
  }
}
