package dev.a4i.bsc.etl.temperature

import java.io.IOException
import java.util.Locale

import com.augustnagro.magnum.magzio.TransactorZIO
import os.*
import zio.*
import zio.http.URL

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.DownloadService
import dev.a4i.bsc.etl.common.extract.UnarchivingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.GeoJSONWriterService
import dev.a4i.bsc.etl.common.transform.RasterReaderService
import dev.a4i.bsc.etl.common.transform.RasterToVectorTransformationService
import dev.a4i.bsc.etl.common.transform.VectorReaderService
import dev.a4i.bsc.etl.configuration.Client
import dev.a4i.bsc.etl.temperature.extract.TemperatureExtractionService
import dev.a4i.bsc.etl.temperature.load.TemperatureLoadingService
import dev.a4i.bsc.etl.temperature.transform.TemperatureTransformationService

class TemperatureETL(
    extractionService: TemperatureExtractionService,
    transformationService: TemperatureTransformationService,
    loadingService: TemperatureLoadingService
):

  def etl(url: URL): Task[Unit] =
    val workflow: RIO[Workspace, Unit] =
      for
        rasterDirectory: Path  <- extractionService.extract(url)
        rasterFiles: Seq[Path] <- findRasterFiles(rasterDirectory)
        vectorDirectory: Path  <- createVectorDirectory(rasterDirectory)
        vectorFiles: Seq[Path] <- ZIO.foreach(rasterFiles): rasterFile =>
                                    val geoJSONFile: Path = vectorDirectory / s"${rasterFile.baseName}.geojson"
                                    transformationService.transform(rasterFile, geoJSONFile)
        _                      <- ZIO.foreachDiscard(vectorFiles)(loadingService.load)
      yield ()

    workflow.provide(Workspace.layer)

  private def findRasterFiles(directory: Path): IO[IOException, Seq[Path]] =
    val extensions: Set[String] = Set("tif", "tiff")

    ZIO.attemptBlockingIO:
      walk(directory)
        .filter(isFile)
        .filter(file => extensions.contains(file.ext.toLowerCase(Locale.ROOT)))

  private def createVectorDirectory(rasterDirectory: Path): ZIO[Workspace, IOException, Path] =
    for
      workspace: Workspace <- ZIO.service[Workspace]
      vectorDirectory: Path = workspace.path / s"${rasterDirectory.last}.vector"
      _                    <- ZIO.attemptBlockingIO(makeDir.all(vectorDirectory))
    yield vectorDirectory

object TemperatureETL:

  private type Dependencies = Client & TransactorZIO

  val layer: ZLayer[Dependencies, Nothing, TemperatureETL] =
    ZLayer.makeSome[Dependencies, TemperatureETL](
      DownloadService.layer,
      GeoJSONWriterService.layer,
      PostGISFeatureWriterService.layer,
      RasterReaderService.layer,
      RasterToVectorTransformationService.layer,
      UnarchivingService.layer,
      VectorReaderService.layer,
      TemperatureExtractionService.layer,
      TemperatureTransformationService.layer,
      TemperatureLoadingService.layer,
      ZLayer.derive[TemperatureETL]
    )
