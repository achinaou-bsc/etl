package dev.a4i.bsc.etl.worldclim.historical.temperature.extract

import zio.http.URL

import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata.*
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata.Period.*

type WorldClimHistoricalTemperatureDataSource = (
    url: URL,
    metadata: WorldClimHistoricalTemperatureMetadata[Annual]
)

object WorldClimHistoricalTemperatureDataSource:

  val averagePerTenMinutes: WorldClimHistoricalTemperatureDataSource =
    (
      url = URL.decode("https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_10m_tavg.zip").getOrElse(???),
      metadata = WorldClimHistoricalTemperatureMetadata(
        Indicator.Average,
        600,
        Annual()
      )
    )

  val averagePerFiveMinutes: WorldClimHistoricalTemperatureDataSource =
    (
      // url = URL.decode("https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_5m_tavg.zip").getOrElse(???),
      url = URL.decode("http://localhost:9000/wc2.1_5m_tavg.zip").getOrElse(???), // FIXME: Delete
      metadata = WorldClimHistoricalTemperatureMetadata(
        Indicator.Average,
        300,
        Annual()
      )
    )

  val averagePerTwoAndHalfMinutes: WorldClimHistoricalTemperatureDataSource =
    (
      url = URL.decode("https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_2.5m_tavg.zip").getOrElse(???),
      metadata = WorldClimHistoricalTemperatureMetadata(
        Indicator.Average,
        150,
        Annual()
      )
    )

  val averagePerThirtySeconds: WorldClimHistoricalTemperatureDataSource =
    (
      url = URL.decode("https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_30s_tavg.zip").getOrElse(???),
      metadata = WorldClimHistoricalTemperatureMetadata(
        Indicator.Average,
        30,
        Annual()
      )
    )
