package com.naya

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructType, TimestampType}

object SuspiciousActivity extends App {
  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("888-streaming")
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  sparkSession.sparkContext.setLogLevel("error")

  import sparkSession.implicits._

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

  val inputDF: DataFrame = sparkSession
    .readStream.schema(schema)
    .format("json")
    .json("src/main/resources/bet_source/*")
    .withWatermark("event_time", "5 seconds")

  var suspiciousDF = inputDF
    .withColumn("win", when($"currency_code".equalTo("EUR"), $"win".divide(1.1)))
    .withColumn("currency_code", when($"currency_code".equalTo("EUR"), lit("USD")))
    .groupBy($"player_id", window($"event_time", "5 seconds"))
    .agg(approx_count_distinct($"country")  as "bets_made_from_countries_count", sum($"win") as "win", sum($"bet") as "bet", sum($"online_time_secs") as "online_time_secs")
    .withColumn("win_bet_ratio", $"win" / $"bet" )
    .withColumn("event_time", date_format(current_timestamp,"yyyy-MM-dd HH:mm:ss"))
    .filter($"win" / $"bet" > 0.1 || $"bets_made_from_countries_count" > 1 || $"online_time_secs" > 18000)
    .withColumn("reasons",array_remove(array(
      when($"win" / $"bet" > 0.1, lit(" win_bet_ratio")).otherwise(""),
      when($"bets_made_from_countries_count" > 1,lit("bet_multiple_countries")).otherwise(""),
      when($"online_time_secs" > 18000, lit("online_5h")).otherwise("")
    ), ""))

  suspiciousDF
    .writeStream
    .outputMode(OutputMode.Append())
    .format("console")
    .option("numRows", 10)
    .option("truncate", "false")
    .start()
    .awaitTermination()
}
