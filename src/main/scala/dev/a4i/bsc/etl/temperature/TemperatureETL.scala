package dev.a4i.bsc.etl.temperature

import os.*
import zio.*
import zio.http.URL

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.temperature.extract.TemperatureExtractionService
import dev.a4i.bsc.etl.temperature.load.TemperatureLoadingService
import dev.a4i.bsc.etl.temperature.transform.TemperatureTransformationService
import dev.a4i.bsc.etl.configuration.Client
import com.augustnagro.magnum.magzio.TransactorZIO

class TemperatureETL(
    extractionService: TemperatureExtractionService,
    transformationService: TemperatureTransformationService,
    loadingService: TemperatureLoadingService
):

  def etl(url: URL): Task[Unit] =
    val workflow: RIO[Workspace, Unit] =
      for
        rasterDirectory: Path <- extractionService.extract(url)
        vectorDirectory: Path <- transformationService.transform(rasterDirectory)
        _                     <- loadingService.load(vectorDirectory)
      yield ()

    workflow.provide(Workspace.layer)

object TemperatureETL:

  private type Dependencies = Client & TransactorZIO

  val layer: ZLayer[Dependencies, Nothing, TemperatureETL] =
    ZLayer.makeSome[Dependencies, TemperatureETL](
      TemperatureExtractionService.layer,
      TemperatureTransformationService.layer,
      TemperatureLoadingService.layer,
      ZLayer.derive[TemperatureETL]
    )
