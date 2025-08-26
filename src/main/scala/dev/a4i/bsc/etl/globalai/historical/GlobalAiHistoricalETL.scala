package dev.a4i.bsc.etl.globalai.historical

import java.time.Month
import java.util.Locale

import os.*
import zio.*

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.DownloadService
import dev.a4i.bsc.etl.common.extract.UnarchivingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.GeoJSONWriterService
import dev.a4i.bsc.etl.common.transform.RasterReaderService
import dev.a4i.bsc.etl.common.transform.RasterToVectorTransformationService
import dev.a4i.bsc.etl.common.transform.VectorReaderService
import dev.a4i.bsc.etl.configuration.HttpClient
import dev.a4i.bsc.etl.configuration.PostGISDataStore
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata.Period.*
import dev.a4i.bsc.etl.globalai.historical.extract.GlobalAiHistoricalDataSource
import dev.a4i.bsc.etl.globalai.historical.extract.GlobalAiHistoricalExtractionService
import dev.a4i.bsc.etl.globalai.historical.load.GlobalAiHistoricalLoadingService
import dev.a4i.bsc.etl.globalai.historical.transform.GlobalAiHistoricalTransformationService

class GlobalAiHistoricalETL(
    extractionService: GlobalAiHistoricalExtractionService,
    transformationService: GlobalAiHistoricalTransformationService,
    loadingService: GlobalAiHistoricalLoadingService
):

  def etl: UIO[Unit] =
    val workflow: URIO[Workspace, Unit] =
      for
        (url, metadata)  = GlobalAiHistoricalDataSource.dataSource
        _               <- ZIO.log("ETL / GlobalAi / Historical: Extracting...")
        rasterDirectory <- extractionService.extract(url)
        rasterFiles     <- findRasterFiles(rasterDirectory, metadata)
        vectorDirectory <- createVectorDirectory(rasterDirectory)
        _               <- ZIO.log("ETL / GlobalAi / Historical: Transforming...")
        vectorFiles     <- ZIO.foreach(rasterFiles): (rasterFile, metadata) =>
                             val Monthly(month)    = metadata.period
                             val geoJSONFile: Path = vectorDirectory / s"${rasterFile.baseName}.geojson"

                             ZIO.log(s"ETL / GlobalAi / Historical: Transforming ${month}...")
                               *> transformationService.transform(metadata, rasterFile, geoJSONFile)
        _               <- ZIO.log("ETL / GlobalAi / Historical: Loading...")
        _               <- ZIO.foreachDiscard(vectorFiles): (vectorFile, metadata) =>
                             ZIO.log(s"ETL / GlobalAi / Historical: Loading ${vectorFile}...")
                               *> loadingService.load(metadata, vectorFile)
      yield ()

    workflow.provide(Workspace.layer)

  private def findRasterFiles(
      directory: Path,
      metadata: GlobalAiHistoricalMetadata[Annual]
  ): UIO[Seq[(Path, GlobalAiHistoricalMetadata[Monthly])]] =
    val extensions: Set[String] = Set("tif", "tiff")

    ZIO
      .attemptBlocking:
        walk(directory)
          .filter(isFile)
          .filter(file => extensions.contains(file.ext.toLowerCase(Locale.ROOT)))
          .sorted
          .map(file =>
            (
              file,
              metadata.copy(period = Monthly(Month.of(file.baseName.split("_").last.toInt)))
            )
          )
          .take(1) // FIXME: Delete
      .orDie

  private def createVectorDirectory(rasterDirectory: Path): URIO[Workspace, Path] =
    for
      workspace      <- ZIO.service[Workspace]
      vectorDirectory = workspace.path / s"${rasterDirectory.last}.vector"
      _              <- ZIO.attemptBlocking(makeDir.all(vectorDirectory)).orDie
    yield vectorDirectory

object GlobalAiHistoricalETL:

  type Dependencies = HttpClient & PostGISDataStore

  val layer: URLayer[Dependencies, GlobalAiHistoricalETL] =
    ZLayer.makeSome[Dependencies, GlobalAiHistoricalETL](
      DownloadService.layer,
      GeoJSONWriterService.layer,
      PostGISFeatureWriterService.layer,
      RasterReaderService.layer,
      RasterToVectorTransformationService.layer,
      UnarchivingService.layer,
      VectorReaderService.layer,
      GlobalAiHistoricalExtractionService.layer,
      GlobalAiHistoricalTransformationService.layer,
      GlobalAiHistoricalLoadingService.layer,
      ZLayer.derive[GlobalAiHistoricalETL]
    )
