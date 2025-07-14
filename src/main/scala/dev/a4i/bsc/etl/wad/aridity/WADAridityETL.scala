package dev.a4i.bsc.etl.wad.aridity

import zio.*

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.DownloadService
import dev.a4i.bsc.etl.common.extract.UnarchivingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.GeoJSONWriterService
import dev.a4i.bsc.etl.common.transform.VectorReaderService
import dev.a4i.bsc.etl.configuration.HttpClient
import dev.a4i.bsc.etl.configuration.PostGISDataStore
import dev.a4i.bsc.etl.wad.aridity.load.WADAridityLoadingService
import dev.a4i.bsc.etl.wad.aridity.transform.WADAridityTransformationService
import dev.a4i.bsc.etl.wad.extract.WADDataSources
import dev.a4i.bsc.etl.wad.extract.WADExtractionService

class WADAridityETL(
    extractionService: WADExtractionService,
    aridityTransformationService: WADAridityTransformationService,
    aridityLoadingService: WADAridityLoadingService
):

  def etl: Task[Unit] =
    val workflow: RIO[Workspace, Unit] =
      for
        _                      <- ZIO.log("ETL / WAD / Aridity: Extracting...")
        aridityRasterDirectory <- extractionService.extract(WADDataSources.aridity)
        _                      <- ZIO.log("ETL / WAD / Aridity: Transforming...")
        aridityVectorDirectory <- aridityTransformationService.transform(aridityRasterDirectory)
        _                      <- ZIO.log("ETL / WAD / Aridity: Loading...")
        _                      <- aridityLoadingService.load(aridityVectorDirectory)
      yield ()

    workflow.provide(Workspace.layer)

object WADAridityETL:

  type Dependencies = HttpClient & PostGISDataStore

  val layer: URLayer[Dependencies, WADAridityETL] =
    ZLayer.makeSome[Dependencies, WADAridityETL](
      DownloadService.layer,
      GeoJSONWriterService.layer,
      PostGISFeatureWriterService.layer,
      UnarchivingService.layer,
      VectorReaderService.layer,
      WADExtractionService.layer,
      WADAridityTransformationService.layer,
      WADAridityLoadingService.layer,
      ZLayer.derive[WADAridityETL]
    )
