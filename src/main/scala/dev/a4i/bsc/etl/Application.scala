package dev.a4i.bsc.etl

import zio.*
import zio.logging.backend.SLF4J

import dev.a4i.bsc.etl.configuration.HttpClient
import dev.a4i.bsc.etl.configuration.PostGISDataStore
import dev.a4i.bsc.etl.wad.WADAridityETL
import dev.a4i.bsc.etl.worldclim.WorldClimHistoricalTemperatureETL

object Application extends ZIOAppDefault:

  override val bootstrap: ZLayer[Any, Any, Any] =
    Runtime.removeDefaultLoggers ++ SLF4J.slf4j

  override val run: UIO[ExitCode] =
    program
      .provide(
        HttpClient.layer,
        PostGISDataStore.layer,
        WADAridityETL.layer,
        WorldClimHistoricalTemperatureETL.layer
      )
      .logError
      .exitCode

  private lazy val program: ZIO[WADAridityETL & WorldClimHistoricalTemperatureETL, Throwable, Unit] =
    for
      _ <- ZIO.serviceWith[WADAridityETL](_.etl)
      _ <- ZIO.serviceWith[WorldClimHistoricalTemperatureETL](_.etl)
    yield ()
