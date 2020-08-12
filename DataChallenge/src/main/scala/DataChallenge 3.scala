import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

import scala.util.Try

object DataChallenge {

    val temperatureDataFilePath = "data/paytmteam-de-weather-challenge-b01d5ebbf02d/data/2019/part-*.csv.gz"
    val countryListDataFilePath = "data/paytmteam-de-weather-challenge-b01d5ebbf02d/countrylist.csv"
    val stationListDataFilePath = "data/paytmteam-de-weather-challenge-b01d5ebbf02d/stationlist.csv"

    val TIME_FORMAT = "yyyyMMdd"
    val options = Map(
        ("delimiter" -> ","),
        ("header" -> "true"),
        ("inferSchema" -> "true"),
        ("nullValue" -> "N/A"),
        ("treatEmptyValuesAsNulls" -> "true"),
    )

    val STN_NO = "STN_NO"
    val YEAR = "year"
    val DATE = "date"
    val WIND_SPEED = "WDSP"
    val IS_TORNADOR_OR_FU_CLOUD = "is_tornado_or_funnel_cloud"
    val TEMPERATURE = "temperature"
    val COUNTRY_ABBR = "COUNTRY_ABBR"
    val COUNTRY_FULL = "COUNTRY_FULL"
    val AVG_TEMPERATURE = "average_temperature"
    val AVG_WIND_SPEED = "average_wind_speed"
    val LAG_DATE = "lag_date"
    val TIME_DIFF = "time_diff"
    val IS_NEW_TORNADO = "is_new_tornado"
    val TORNADO_GAP = 86400000 // one day
    val TORNADO_DATE_FROM_START = "tornado_date_from_start"

    val toEpochTimeUdf: UserDefinedFunction = udf((t: Integer) => {
        val tStr = t.toString
        val format = new SimpleDateFormat(TIME_FORMAT)
        Try(format.parse(tStr)).map(x => x.getTime).getOrElse(-1L)
    })

    val getYearFromTimeUdf: UserDefinedFunction = udf((t: Integer) => {
        val tStr = t.toString
        tStr.substring(0, 4)
    })

    val toIfTornadoOrFunnelCloud: UserDefinedFunction = udf((num: Integer) => {
        // 6th digit is 1 indicate FRSHTT is tornado or funnel cloud
        val numStr = num.toString
        numStr.length == 6 && numStr.charAt(5) == '1'
    })

    def main(args: Array[String]): Unit = {
        // Session Configurations
        val spark = SparkSession.builder()
          .appName("DataChallenge")
          .master("local[*]").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        val temperatureDF = spark.read.format("csv")
          .options(options)
          .load(temperatureDataFilePath)

        val countryListDF = spark.read.format("csv")
          .options(options)
          .load(countryListDataFilePath)
        countryListDF.show(false)
        val stationListDF = spark.read.format("csv")
          .options(options)
          .load(stationListDataFilePath)
        stationListDF.show(false)

        temperatureDF.show(false)
        temperatureDF.printSchema()

        // Information need:
        // country - STN(temp table) FK -> stationTable (STN_NO),
        //                  get COUNTRY_ABBR FK -> CountryListTable to get country
        // temperature -
        // year
        // date
        // wind speed
        // tornadoes/funnel cloud - FRSHTT, 6th digit

        val temperatureProcessedDF = temperatureDF
          .withColumn(STN_NO, col("STN---"))
          .withColumn(YEAR, getYearFromTimeUdf(col("YEARMODA")))
          .withColumn(DATE, toEpochTimeUdf(col("YEARMODA")))
          .withColumn(WIND_SPEED, when(col("WDSP")===999.9, lit(null)).otherwise(col("WDSP")))
          .withColumn(TEMPERATURE, when(col("TEMP")===99999, lit(null)).otherwise(col("TEMP")))
          .withColumn(IS_TORNADOR_OR_FU_CLOUD, toIfTornadoOrFunnelCloud(col("FRSHTT")))
          .select(
              col(STN_NO),
              col(YEAR),
              col(DATE),
              col(TEMPERATURE),
              col(WIND_SPEED),
              col("FRSHTT"),
              col(IS_TORNADOR_OR_FU_CLOUD)
          )

        val joinedTableDF = temperatureProcessedDF
          .join(stationListDF, STN_NO)
          .join(countryListDF, COUNTRY_ABBR)
          .cache()

        // 1. Which country had the hottest average mean temperature over the year?
        // Answer: DJ: DJIBOUTI
        val avgTempDF = joinedTableDF.groupBy(COUNTRY_FULL, YEAR)
          .agg(avg(TEMPERATURE).as(AVG_TEMPERATURE))

        avgTempDF.orderBy(desc(AVG_TEMPERATURE)).take(1).foreach(println)

        // 2. Which country had the coldest average mean temperature over the year?
        // ans: ANTARCTICA
        avgTempDF.orderBy(asc(AVG_TEMPERATURE)).take(1).foreach(println)

        //  3. Which country had the second highest average mean wind speed over the year?
        // * window function also works here
        // ans: [BERMUDA, 2020, 16.9]

        joinedTableDF.groupBy(COUNTRY_FULL, YEAR)
          .agg(avg(WIND_SPEED).as(AVG_WIND_SPEED))
          .orderBy(desc(AVG_WIND_SPEED))
          .take(2).foreach(println)

        // 4. Which country had the most consecutive days of tornadoes/funnel cloud formations?
        // ans: UNITED STATES
        // discussion, if by country, shall we consider tornado by station instead?
        // Else the country with larger area will certainly see longer consecutive days
        val partitionByCountryOrderByTime = Window.partitionBy(COUNTRY_ABBR).orderBy(DATE)

        joinedTableDF
            .where(col(IS_TORNADOR_OR_FU_CLOUD) === 1)
          .withColumn(LAG_DATE,
              lag(DATE,1, 0).over(partitionByCountryOrderByTime))
          .withColumn(TIME_DIFF, col(DATE).minus(LAG_DATE))
          .withColumn(IS_NEW_TORNADO, when(col(TIME_DIFF) <= TORNADO_GAP, 0).otherwise(1))
          .select(
              col(COUNTRY_FULL),
              // get days from the 1st day see the tornado
              sum(IS_NEW_TORNADO).over(partitionByCountryOrderByTime).as(TORNADO_DATE_FROM_START)
          ).orderBy(desc(TORNADO_DATE_FROM_START)).take(5).foreach(println)
    }
}
