package dev.a4i.bsc.etl.worldclim.historical.temperature

import java.io.IOException
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

  def etl: Task[Unit] =
    val workflow: RIO[Workspace, Unit] =
      for
        (url, metadata)  = WorldClimHistoricalTemperatureDataSource.averagePerTenMinutes
        rasterDirectory <- extractionService.extract(url)
        rasterFiles     <- findRasterFiles(rasterDirectory, metadata)
        vectorDirectory <- createVectorDirectory(rasterDirectory)
        vectorFiles     <- ZIO.foreach(rasterFiles): (rasterFile, metadata) =>
                             val geoJSONFile: Path = vectorDirectory / s"${rasterFile.baseName}.geojson"
                             transformationService.transform(metadata, rasterFile, geoJSONFile)
        _               <- ZIO.foreachDiscard(vectorFiles)(loadingService.load)
      yield ()

    workflow.provide(Workspace.layer)

  private def findRasterFiles(
      directory: Path,
      metadata: WorldClimHistoricalTemperatureMetadata[Annual]
  ): IO[IOException, Seq[(Path, WorldClimHistoricalTemperatureMetadata[Monthly])]] =
    val extensions: Set[String] = Set("tif", "tiff")

    ZIO.attemptBlockingIO:
      walk(directory)
        .filter(isFile)
        .filter(file => extensions.contains(file.ext.toLowerCase(Locale.ROOT)))
        .map(file =>
          (
            file,
            metadata.copy(period = Monthly(Month.of(file.baseName.split("_").last.toInt)))
          )
        )

  private def createVectorDirectory(rasterDirectory: Path): ZIO[Workspace, IOException, Path] =
    for
      workspace: Workspace <- ZIO.service[Workspace]
      vectorDirectory: Path = workspace.path / s"${rasterDirectory.last}.vector"
      _                    <- ZIO.attemptBlockingIO(makeDir.all(vectorDirectory))
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
