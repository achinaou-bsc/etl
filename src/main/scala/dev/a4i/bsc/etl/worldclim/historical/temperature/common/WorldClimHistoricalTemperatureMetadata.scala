package dev.a4i.bsc.etl.worldclim.historical.temperature.common

import java.time.Month

import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata.*

case class WorldClimHistoricalTemperatureMetadata[P <: Period](
    indicator: Indicator,
    resolution: Resolution,
    period: P
) derives CanEqual

object WorldClimHistoricalTemperatureMetadata:

  enum Indicator derives CanEqual:
    case Average

  type Resolution = 30 | 150 | 300 | 600

  sealed abstract class Period
  object Period:
    case class Annual()              extends Period
    case class Monthly(month: Month) extends Period
