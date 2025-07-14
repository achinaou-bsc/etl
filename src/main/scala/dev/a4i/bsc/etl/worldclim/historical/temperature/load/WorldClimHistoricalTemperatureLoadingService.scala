package dev.a4i.bsc.etl.worldclim.historical.temperature.load

import os.*
import zio.*

import dev.a4i.bsc.etl.common.load.LoadingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.VectorReaderService

class WorldClimHistoricalTemperatureLoadingService(
    vectorReaderService: VectorReaderService,
    postGISFeatureWriterService: PostGISFeatureWriterService
) extends LoadingService:

  def load(vectorFile: Path): Task[Unit] =
    ZIO.scoped:
      for
        featureCollection <- vectorReaderService.read(vectorFile)
        _                 <- postGISFeatureWriterService.write("worldclim_historical_temperature", featureCollection)
      yield ()

object WorldClimHistoricalTemperatureLoadingService:

  type Dependencies = VectorReaderService & PostGISFeatureWriterService

  val layer: URLayer[Dependencies, WorldClimHistoricalTemperatureLoadingService] =
    ZLayer.derive[WorldClimHistoricalTemperatureLoadingService]
