package dev.a4i.bsc.etl

import com.augustnagro.magnum.magzio.TransactorZIO
import zio.*
import zio.logging.backend.SLF4J

import dev.a4i.bsc.etl.common.extract.TemperatureDataSources
import dev.a4i.bsc.etl.configuration.Client
import dev.a4i.bsc.etl.configuration.DataSource
import dev.a4i.bsc.etl.temperature.TemperatureETL

object Application extends ZIOAppDefault:

  override val bootstrap: ZLayer[Any, Any, Any] =
    Runtime.removeDefaultLoggers ++ SLF4J.slf4j

  override val run: UIO[ExitCode] =
    program
      .provide(
        Client.layer,
        DataSource.layer,
        TransactorZIO.layer,
        TemperatureETL.layer
      )
      .logError
      .exitCode

  private lazy val program: ZIO[TemperatureETL, Throwable, Unit] =
    for
      temperatureETL: TemperatureETL <- ZIO.service[TemperatureETL]
      _                              <- temperatureETL.etl(TemperatureDataSources.Average.Historical.tenMinutes)
    yield ()
