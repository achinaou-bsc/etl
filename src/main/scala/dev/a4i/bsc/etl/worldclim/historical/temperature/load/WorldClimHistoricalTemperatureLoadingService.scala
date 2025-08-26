package dev.a4i.bsc.etl.worldclim.historical.temperature.load

import os.*
import zio.*

import dev.a4i.bsc.etl.common.load.LoadingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.VectorReaderService
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata.Indicator
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata.Period.Monthly

class WorldClimHistoricalTemperatureLoadingService(
    vectorReaderService: VectorReaderService,
    postGISFeatureWriterService: PostGISFeatureWriterService
) extends LoadingService:

  def load(metadata: WorldClimHistoricalTemperatureMetadata[Monthly], vectorFile: Path): UIO[Unit] =
    ZIO.scoped:
      for
        qualifier          = metadata.indicator match
                               case Indicator.Average => "average"
        featureCollection <- vectorReaderService.read(vectorFile)
        _                 <- postGISFeatureWriterService.write(
                               s"worldclim_historical_temperature_${qualifier}",
                               featureCollection
                             )
      yield ()

object WorldClimHistoricalTemperatureLoadingService:

  type Dependencies = VectorReaderService & PostGISFeatureWriterService

  val layer: URLayer[Dependencies, WorldClimHistoricalTemperatureLoadingService] =
    ZLayer.derive[WorldClimHistoricalTemperatureLoadingService]
