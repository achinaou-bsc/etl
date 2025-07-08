package dev.a4i.bsc.etl.desertification

import os.*
import zio.*
import zio.http.URL

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.DownloadService
import dev.a4i.bsc.etl.common.extract.UnarchivingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.GeoJSONWriterService
import dev.a4i.bsc.etl.common.transform.VectorReaderService
import dev.a4i.bsc.etl.configuration.Client
import dev.a4i.bsc.etl.configuration.PostGISDataStore
import dev.a4i.bsc.etl.desertification.extract.DesertificationExtractionService
import dev.a4i.bsc.etl.desertification.load.DesertificationLoadingService
import dev.a4i.bsc.etl.desertification.transform.DesertificationTransformationService

class DesertificationETL(
    extractionService: DesertificationExtractionService,
    transformationService: DesertificationTransformationService,
    loadingService: DesertificationLoadingService
):

  def etl(url: URL): Task[Unit] =
    val workflow: RIO[Workspace, Unit] =
      for
        rasterDirectory: Path <- extractionService.extract(url)
        vectorDirectory: Path <- transformationService.transform(rasterDirectory)
        _                     <- loadingService.load(vectorDirectory)
      yield ()

    workflow.provide(Workspace.layer)

object DesertificationETL:

  private type Dependencies = Client

  val layer: ZLayer[Dependencies, Nothing, DesertificationETL] =
    ZLayer.makeSome[Dependencies, DesertificationETL](
      DownloadService.layer,
      GeoJSONWriterService.layer,
      PostGISDataStore.layer,
      PostGISFeatureWriterService.layer,
      UnarchivingService.layer,
      VectorReaderService.layer,
      DesertificationExtractionService.layer,
      DesertificationTransformationService.layer,
      DesertificationLoadingService.layer,
      ZLayer.derive[DesertificationETL]
    )
