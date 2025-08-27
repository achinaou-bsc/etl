package dev.a4i.bsc.etl

import zio.*
import zio.logging.backend.SLF4J

import dev.a4i.bsc.etl.configuration.HttpClient
import dev.a4i.bsc.etl.configuration.PostGISDataStore
import dev.a4i.bsc.etl.globalai.historical.GlobalAiHistoricalETL
import dev.a4i.bsc.etl.worldclim.historical.temperature.WorldClimHistoricalTemperatureETL

object Application extends ZIOAppDefault:

  override val bootstrap: ZLayer[Any, Any, Any] =
    Runtime.removeDefaultLoggers ++ SLF4J.slf4j

  override val run: ZIO[Any, Throwable, Unit] =
    program
      .provide(
        HttpClient.layer,
        PostGISDataStore.layer,
        WorldClimHistoricalTemperatureETL.layer,
        GlobalAiHistoricalETL.layer
      )
      .unit

  private lazy val program: URIO[WorldClimHistoricalTemperatureETL & GlobalAiHistoricalETL, Unit] =
    for
      _ <- ZIO.serviceWithZIO[WorldClimHistoricalTemperatureETL](_.etl)
      _ <- ZIO.serviceWithZIO[GlobalAiHistoricalETL](_.etl)
      _ <- ZIO.log("ETL: Done")
    yield ()
