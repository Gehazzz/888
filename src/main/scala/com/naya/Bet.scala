package com.naya

import java.time.LocalDateTime

import com.fasterxml.jackson.annotation.{JsonFormat, JsonProperty}

case class Bet(@JsonProperty("event_id")
               eventId: Long,
               @JsonProperty("event_time")
               @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
               eventTime: LocalDateTime,
               @JsonProperty("player_id")
               playerId: Long,
               bet: Double,
               @JsonProperty("game_name")
               gameName: String,
               country: String,
               win: Double,
               @JsonProperty("online_time_secs")
               onlineTimeSecs: Long,
               @JsonProperty("currency_code")
               currencyCode: String)
