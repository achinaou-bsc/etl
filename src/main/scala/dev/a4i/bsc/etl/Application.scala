package dev.a4i.bsc.etl

import zio.*
import zio.logging.backend.SLF4J

import dev.a4i.bsc.etl.configuration.HttpClient
import dev.a4i.bsc.etl.configuration.PostGISDataStore
import dev.a4i.bsc.etl.desertification.DesertificationETL
import dev.a4i.bsc.etl.desertification.extract.DesertificationDataSources
import dev.a4i.bsc.etl.temperature.TemperatureETL
import dev.a4i.bsc.etl.temperature.extract.TemperatureDataSources

object Application extends ZIOAppDefault:

  override val bootstrap: ZLayer[Any, Any, Any] =
    Runtime.removeDefaultLoggers ++ SLF4J.slf4j

  override val run: UIO[ExitCode] =
    program
      .provide(
        HttpClient.layer,
        PostGISDataStore.layer,
        DesertificationETL.layer,
        TemperatureETL.layer
      )
      .logError
      .exitCode

  private lazy val program: ZIO[DesertificationETL & TemperatureETL, Throwable, Unit] =
    for
      _ <- ZIO.serviceWith[DesertificationETL](_.etl(DesertificationDataSources.url))
      _ <- ZIO.serviceWith[TemperatureETL](_.etl(TemperatureDataSources.Average.Historical.tenMinutes))
    yield ()
