package dev.a4i.bsc.etl.globalai.historical.load

import os.*
import zio.*

import dev.a4i.bsc.etl.common.load.LoadingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.VectorReaderService
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata.Indicator
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata.Period.Monthly

class GlobalAiHistoricalLoadingService(
    vectorReaderService: VectorReaderService,
    postGISFeatureWriterService: PostGISFeatureWriterService
) extends LoadingService:

  def load(metadata: GlobalAiHistoricalMetadata[Monthly], vectorFile: Path): UIO[Unit] =
    ZIO.scoped:
      for
        qualifier          = metadata.indicator match
                               case Indicator.Average => "average"
        featureCollection <- vectorReaderService.read(vectorFile)
        _                 <- postGISFeatureWriterService.write(
                               s"global_ai_historical_${qualifier}",
                               featureCollection
                             )
      yield ()

object GlobalAiHistoricalLoadingService:

  type Dependencies = VectorReaderService & PostGISFeatureWriterService

  val layer: URLayer[Dependencies, GlobalAiHistoricalLoadingService] =
    ZLayer.derive[GlobalAiHistoricalLoadingService]
