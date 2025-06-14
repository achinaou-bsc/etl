package dev.a4i.bsc.etl.temperature.transform

import java.io.IOException
import java.io.OutputStream
import java.util.Locale
import scala.jdk.CollectionConverters.*

import org.geotools.api.parameter.GeneralParameterValue
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.geojson.GeoJSONWriter
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.process.ProcessException
import org.geotools.process.raster.PolygonExtractionProcess
import org.jaitools.numeric.Range
import os.Path
import zio.*

import dev.a4i.bsc.etl.common.Workspace

class TemperatureTransformationService:

  def transform(rasterDirectory: Path): ZIO[Workspace, IOException | ProcessException, Path] =
    for
      workspace: Workspace   <- ZIO.service[Workspace]
      vectorDirectory: Path   = workspace.path / s"${rasterDirectory.baseName}.vector"
      rasterFiles: Seq[Path] <- findRasterFiles(rasterDirectory)
      _                      <- ZIO.attemptBlockingIO(os.makeDir.all(vectorDirectory))
      _                      <- ZIO.foreachDiscard(rasterFiles)(vectorizeFile(vectorDirectory))
    yield vectorDirectory

  private def findRasterFiles(directory: Path): IO[IOException, Seq[Path]] =
    val rasterFileExtensions: Set[String] = Set("tif", "tiff")

    ZIO.attemptBlockingIO:
      os
        .walk(directory)
        .filter(file => rasterFileExtensions.contains(file.ext.toLowerCase(Locale.ROOT)))

  private def vectorizeFile(vectorizedDirectory: Path)(rasterFile: Path): IO[IOException | ProcessException, Path] =
    val vectorFile: Path = vectorizedDirectory / s"${rasterFile.baseName}.geojson"

    for
      coverage: GridCoverage2D                   <- readRaster(rasterFile)
      featureCollection: SimpleFeatureCollection <- vectorize(coverage)
      _                                          <- writeVector(vectorFile)(featureCollection)
    yield vectorFile

  private def readRaster(rasterFile: Path): IO[IOException, GridCoverage2D] =
    ZIO.attemptBlockingIO(GeoTiffReader(rasterFile.toIO).read(Array.empty[GeneralParameterValue]))

  private def vectorize(coverage: GridCoverage2D): IO[ProcessException, SimpleFeatureCollection] =
    val classificationRanges: List[Range[Integer]] = List()

    ZIO
      .attempt:
        PolygonExtractionProcess().execute(
          coverage,
          0,
          true,
          null,
          List.empty.asJava,
          classificationRanges.asJava,
          null
        )
      .refineToOrDie[ProcessException]

  private def writeVector(vectorFile: Path)(featureCollection: SimpleFeatureCollection): IO[IOException, Path] =
    ZIO.scoped:
      for
        outputStream: OutputStream <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(os.write.outputStream(vectorFile)))
        writer: GeoJSONWriter      <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(GeoJSONWriter(outputStream)))
        _                          <- ZIO.attemptBlockingIO(writer.writeFeatureCollection(featureCollection))
      yield vectorFile
