package dev.a4i.bsc.etl.worldclim.historical.temperature

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
import dev.a4i.bsc.etl.worldclim.historical.extract.WorldClimHistoricalExtractionService
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata.Period.*
import dev.a4i.bsc.etl.worldclim.historical.temperature.extract.WorldClimHistoricalTemperatureDataSource
import dev.a4i.bsc.etl.worldclim.historical.temperature.load.WorldClimHistoricalTemperatureLoadingService
import dev.a4i.bsc.etl.worldclim.historical.temperature.transform.WorldClimHistoricalTemperatureTransformationService

class WorldClimHistoricalTemperatureETL(
    extractionService: WorldClimHistoricalExtractionService,
    transformationService: WorldClimHistoricalTemperatureTransformationService,
    loadingService: WorldClimHistoricalTemperatureLoadingService
):

  def etl: UIO[Unit] =
    val workflow: URIO[Workspace, Unit] =
      for
        (url, metadata)  = WorldClimHistoricalTemperatureDataSource.averagePerFiveMinutes
        _               <- ZIO.log("ETL / WorldClim / Historical / Temperature: Extracting...")
        rasterDirectory <- extractionService.extract(url)
        rasterFiles     <- findRasterFiles(rasterDirectory, metadata)
        vectorDirectory <- createVectorDirectory(rasterDirectory)
        _               <- ZIO.log("ETL / WorldClim / Historical / Temperature: Transforming...")
        vectorFiles     <- ZIO.foreach(rasterFiles): (rasterFile, metadata) =>
                             val Monthly(month)    = metadata.period
                             val geoJSONFile: Path = vectorDirectory / s"${rasterFile.baseName}.geojson"

                             ZIO.log(s"ETL / WorldClim / Historical / Temperature: Transforming ${month}...")
                               *> transformationService.transform(metadata, rasterFile, geoJSONFile)
        _               <- ZIO.log("ETL / WorldClim / Historical / Temperature: Loading...")
        _               <- ZIO.foreachDiscard(vectorFiles): (vectorFile, metadata) =>
                             ZIO.log(s"ETL / WorldClim / Historical / Temperature: Loading ${vectorFile}...")
                               *> loadingService.load(metadata, vectorFile)
      yield ()

    workflow.provide(Workspace.layer)

  private def findRasterFiles(
      directory: Path,
      metadata: WorldClimHistoricalTemperatureMetadata[Annual]
  ): UIO[Seq[(Path, WorldClimHistoricalTemperatureMetadata[Monthly])]] =
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
          .take(1)
      .orDie

  private def createVectorDirectory(rasterDirectory: Path): URIO[Workspace, Path] =
    for
      workspace      <- ZIO.service[Workspace]
      vectorDirectory = workspace.path / s"${rasterDirectory.last}.vector"
      _              <- ZIO.attemptBlocking(makeDir.all(vectorDirectory)).orDie
    yield vectorDirectory

object WorldClimHistoricalTemperatureETL:

  type Dependencies = HttpClient & PostGISDataStore

  val layer: URLayer[Dependencies, WorldClimHistoricalTemperatureETL] =
    ZLayer.makeSome[Dependencies, WorldClimHistoricalTemperatureETL](
      DownloadService.layer,
      GeoJSONWriterService.layer,
      PostGISFeatureWriterService.layer,
      RasterReaderService.layer,
      RasterToVectorTransformationService.layer,
      UnarchivingService.layer,
      VectorReaderService.layer,
      WorldClimHistoricalExtractionService.layer,
      WorldClimHistoricalTemperatureTransformationService.layer,
      WorldClimHistoricalTemperatureLoadingService.layer,
      ZLayer.derive[WorldClimHistoricalTemperatureETL]
    )
