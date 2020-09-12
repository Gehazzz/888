package com.naya

import java.io.{BufferedWriter, File, FileWriter, StringWriter}
import java.time.LocalDateTime

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.mutable.ListBuffer
import scala.util.Random

object EventGenerator extends App {
  BetEventGenerator.clearDirectory("src/main/resources/bet_source")
  BetEventGenerator.generate("src/main/resources/bet_source/", 80, 4, 2000)
  BetEventGenerator.clearDirectory("src/main/resources/bet_source")
}

object BetEventGenerator{
  val mapper: ObjectMapper = new ObjectMapper().registerModule(new JavaTimeModule).registerModule(DefaultScalaModule)

  val games: List[String] = List("baccarat", "poker", "blackjack", "canasta", "cribbage", "faro", "monte", "rummy", "whist")
  val countries: List[String] = List("US", "FR")
  val currencies: List[String] = List("USD", "EUR")

  def generate(rootDir: String, numberOfFiles: Int, eventsPerFile: Int, delay: Long): Unit = {
    for ( i <- 0 to numberOfFiles){
      writeFile(rootDir + i + "_events.txt", generateBetEvents(eventsPerFile).map(betToJson))
      Thread.sleep(delay)
    }
  }

  private def generateBetEvents(eventsToGenerate: Int): Seq[Bet] = {
    var events = new ListBuffer[Bet]()
    for ( i <- 0 to eventsToGenerate){
        events += createBetEvent(i)
    }
    events
  }

  private def createBetEvent(i: Int): Bet = {
    val eventId: Long = i
    val eventTime: LocalDateTime = LocalDateTime.now().plusSeconds(i)
    val playerId: Long = Random.nextInt(10)
    val bet: Double = getRandomValue(10, 200, 2)
    val gameName: String = getGameName
    val countryIndex = Random.nextInt(2)
    val country: String = countries(countryIndex)
    val win: Double = getRandomValue(1, 10, 2)
    val onlineTimeSecs: Long =  Random.nextInt(5000)
    val currencyCode: String = currencies(countryIndex)
    Bet(eventId, eventTime, playerId, bet, gameName, country, win, onlineTimeSecs, currencyCode)
  }

  private def getRandomValue(lowerBound: Int, upperBound: Int, decimalPlaces: Int): Double = {
    BigDecimal(Random.nextDouble() * (upperBound - lowerBound) + lowerBound).setScale(decimalPlaces, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  private def getGameName: String = {
    val index = Random.nextInt(8)
    var cardGame: String = games(index)
    if (Random.nextInt(100) < 10) {
      cardGame = cardGame + "-demo"
    }
    cardGame
  }

  private def betToJson(event: Bet):String = {
    val out = new StringWriter
    mapper.writeValue(out, event)
    out.toString
  }

  private def writeFile(filename: String, lines: Seq[String]): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    for (line <- lines) {
      bw.write(line)
      bw.newLine()
    }
    bw.close()
  }

  def clearDirectory(path: String): Unit = {
    deleteOnlyFiles(new File(path))
  }

  private def deleteOnlyFiles(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteOnlyFiles)
    } else {
      if (file.exists && !file.delete) {
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
      }
    }
  }
}
