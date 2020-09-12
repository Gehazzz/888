package com.naya

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType, TimestampType}

object Report extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("888-streaming")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  sparkSession.sparkContext.setLogLevel("error")

  val accum = sparkSession.sparkContext.doubleAccumulator("overal")

  import sparkSession.implicits._
  import Cleaner._
  clearDirectory("src/main/resources/bet_intermediate/checkpoint")
  clearDirectory("src/main/resources/bet_intermediate/report")
  clearDirectory("src/main/resources/bet_intermediate/overall")


  val schema = new StructType()
    .add("event_id", LongType)
    .add("event_time", TimestampType)
    .add("player_id", StringType)
    .add("bet", DoubleType)
    .add("game_name", StringType)
    .add("country", StringType)
    .add("win", DoubleType)
    .add("online_time_secs", LongType)
    .add("currency_code", StringType)

  val inputDFReportPreCalculation: DataFrame = sparkSession
    .readStream.schema(schema)
    .format("json")
    .json("src/main/resources/bet_source/*")
    .withWatermark("event_time", "5 seconds")

  val inputDFOverallPreCalculation: DataFrame = sparkSession
    .readStream.schema(schema)
    .format("json")
    .json("src/main/resources/bet_source/*")
    .withWatermark("event_time", "5 seconds")

  val inputDFReportToWrite = inputDFReportPreCalculation
    .withColumn("win", when($"currency_code".equalTo("EUR"), $"win".divide(1.1)))
    .withColumn("bet", when($"currency_code".equalTo("EUR"), $"bet".divide(1.1)))
    .groupBy($"game_name", window($"event_time", "5 seconds"))
    .agg(sum($"win" - $"bet") as "profit",
      avg($"bet") as "avg_bet",
      max($"bet") as "max_bet",
      min($"bet") as "min_bet",
      count($"bet")  as "bet_count",
      sum($"bet") as "total_bet",
      avg($"win") as "avg_win",
      max($"win") as "max_win",
      min($"win") as "min_win",
      count($"win")  as "win_count",
      sum($"win") as "total_win")
    .withColumnRenamed("window", "prev_window")
    .withColumn("event_time", date_format(current_timestamp, "yyyy-MM-dd HH:mm:ss"))

  inputDFReportToWrite
    .writeStream
    .outputMode(OutputMode.Append())
    .format("json")
    .option("checkpointLocation", "src/main/resources/bet_intermediate/checkpoint/report")
    .option("path", "src/main/resources/bet_intermediate/report")
    .start()

  val inputDFOverallToWrite = inputDFOverallPreCalculation
    .withColumn("win", when($"currency_code".equalTo("EUR"), $"win".divide(1.1)))
    .withColumn("bet", when($"currency_code".equalTo("EUR"), $"bet".divide(1.1)))
    .groupBy(window($"event_time", "5 seconds"))
    .agg(sum($"win" - $"bet") as "profit_total")
    .withColumnRenamed("window", "prev_window")
    .withColumn("event_time", date_format(current_timestamp, "yyyy-MM-dd HH:mm:ss"))

  inputDFOverallToWrite
    .writeStream
    .outputMode(OutputMode.Append())
    .format("json")
    .option("checkpointLocation", "src/main/resources/bet_intermediate/checkpoint/overall")
    .option("path", "src/main/resources/bet_intermediate/overall")
    .start()

  val overallSchema = new StructType()
    .add("prev_window", new StructType().add("start", TimestampType).add("end", TimestampType))
    .add("profit_total", DoubleType)
    .add("event_time", TimestampType)

  val reportSchema = new StructType()
    .add("game_name", StringType)
    .add("prev_window", new StructType().add("start", TimestampType).add("end", TimestampType))
    .add("profit", DoubleType)
    .add("avg_bet", DoubleType)
    .add("max_bet", DoubleType)
    .add("min_bet", DoubleType)
    .add("bet_count", LongType)
    .add("total_bet", DoubleType)
    .add("avg_win", DoubleType)
    .add("max_win", DoubleType)
    .add("min_win", DoubleType)
    .add("win_count", LongType)
    .add("total_win", DoubleType)
    .add("event_time", TimestampType)

  val inputDFReport: DataFrame = sparkSession
    .readStream.schema(reportSchema)
    .format("json")
    .load("src/main/resources/bet_intermediate/report/*")
    .withWatermark("event_time", "5 seconds")

  val inputDFOverall: DataFrame = sparkSession
    .readStream.schema(overallSchema)
    .format("json")
    .load("src/main/resources/bet_intermediate/overall/*")
    .withWatermark("event_time", "5 seconds")
    .drop($"event_time")

  var result = inputDFReport.join(inputDFOverall, "prev_window")
    .withColumnRenamed("prev_window", "window")

  result
    .writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .option("numRows", 10)
    .option("truncate", "false")
    .start()
    .awaitTermination()
}
