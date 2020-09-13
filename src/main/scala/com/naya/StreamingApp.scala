package com.naya

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object StreamingApp{
  def main(args: Array[String]) {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("888-streaming")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("error")

    val accum = sparkSession.sparkContext.doubleAccumulator("overal")
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

    val inputDFstep1: DataFrame = sparkSession
      .readStream.schema(schema)
      //.option("maxFilesPerTrigger", 1)

      .format("json")
      .json("src/main/resources/bet_source/*")
      .withWatermark("event_time", "5 seconds")


    var reportDF = inputDFstep1

      .groupBy($"game_name", window($"event_time", "5 seconds"))

      //.agg(approx_count_distinct($"country")  as "count")
      .agg(sum($"win") as "win")
      .withColumn("event_time", date_format(current_timestamp, "yyyy-MM-dd HH:mm:ss"))
      .map(row => {
        //accum.value
        accum.add(row.getAs("win").asInstanceOf[Double])
        row
      })(RowEncoder(
        new StructType()
          .add("game_name", StringType)
          .add("window", new StructType().add("start", TimestampType).add("end", TimestampType))
          .add("win", DoubleType)
          .add("event_time", StringType)
          //.add("total", DoubleType)
      )).toDF()
    //.withColumn("total", lit(accum.sum))

    /*    .withColumn("event_time", date_format(current_timestamp,"yyyy-MM-dd HH:mm:ss"))
    .withColumn("win_overal", sum($"win") over(Window.orderBy($"event_time")))*/

    reportDF
      .writeStream
      //.foreachBatch((df:DataFrame, id:Long) => {df.groupBy($"window").agg(sum($"win")).writeStream.format("console").start().awaitTermination()})
      //.trigger(Trigger.ProcessingTime("5 seconds"))

      .outputMode(OutputMode.Append())
      //.format("console")
      .format("json")
      .option("numRows", 10)
      //.option("truncate", "false")
      //.option("maxFilesPerTrigger", 1)
    .option("checkpointLocation", "src/main/resources/bet_intermediate/checkpoint")
    .option("path", "src/main/resources/bet_intermediate")

      .start()
      //.awaitTermination()
      println(accum.value)

    sparkSession
      .readStream.schema(
      new StructType()
      .add("game_name", StringType)
      .add("window", new StructType().add("start", TimestampType).add("end", TimestampType))
      .add("win", DoubleType)
      .add("event_time", TimestampType)
    )
      .format("json")
      .json("src/main/resources/bet_intermediate/*")
      .withWatermark("event_time", "5 seconds")
      .groupBy(window($"event_time", "5 seconds")).agg(sum($"win"))
      .withColumn("total", lit(accum.value))
      .writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("numRows", 10)
      .option("truncate", "false")
      .start()
      .awaitTermination()

  }
}
