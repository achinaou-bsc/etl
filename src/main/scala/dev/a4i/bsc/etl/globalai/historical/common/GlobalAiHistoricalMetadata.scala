package dev.a4i.bsc.etl.globalai.historical.common

import java.time.Month

import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata.*

case class GlobalAiHistoricalMetadata[P <: Period](
    indicator: Indicator,
    resolution: Resolution,
    period: P
) derives CanEqual

object GlobalAiHistoricalMetadata:

  enum Indicator derives CanEqual:
    case Average

  type Resolution = 30

  sealed abstract class Period
  object Period:
    case class Annual()              extends Period
    case class Monthly(month: Month) extends Period
