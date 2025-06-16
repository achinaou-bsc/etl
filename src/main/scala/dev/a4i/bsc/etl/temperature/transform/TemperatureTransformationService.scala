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
import os.*
import zio.*

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.transform.TransformationService

class TemperatureTransformationService extends TransformationService:

  def transform(rasterDirectory: Path): ZIO[Workspace, IOException | ProcessException, Path] =
    for
      _                      <- ZIO.log("Transforming: Temperature")
      vectorDirectory: Path  <- createVectorDirectory(rasterDirectory)
      rasterFiles: Seq[Path] <- findRasterFiles(rasterDirectory)
      _                      <- ZIO.foreachDiscard(rasterFiles)(vectorizeFile(vectorDirectory))
      _                      <- ZIO.log("Transformed: Temperature")
    yield vectorDirectory

  private def createVectorDirectory(rasterDirectory: Path): ZIO[Workspace, IOException, Path] =
    for
      workspace: Workspace <- ZIO.service[Workspace]
      vectorDirectory: Path = workspace.path / s"${rasterDirectory.last}.vector"
      _                    <- ZIO.attemptBlockingIO(makeDir.all(vectorDirectory))
    yield vectorDirectory

  private def findRasterFiles(directory: Path): IO[IOException, Seq[Path]] =
    val extensions: Set[String] = Set("tif", "tiff")

    ZIO.attemptBlockingIO:
      walk(directory)
        .filter(isFile)
        .filter(file => extensions.contains(file.ext.toLowerCase(Locale.ROOT)))

  private def vectorizeFile(vectorizedDirectory: Path)(rasterFile: Path): IO[IOException | ProcessException, Path] =
    val vectorFile: Path = vectorizedDirectory / s"${rasterFile.baseName}.geojson"

    for
      _                                          <- ZIO.log(s"Vectorizing: $rasterFile -> $vectorFile")
      coverage: GridCoverage2D                   <- readRasterFile(rasterFile)
      featureCollection: SimpleFeatureCollection <- vectorize(coverage)
      _                                          <- writeVectorFile(vectorFile)(featureCollection)
      _                                          <- ZIO.log(s"Vectorized: $rasterFile -> $vectorFile")
    yield vectorFile

  private def readRasterFile(rasterFile: Path): IO[IOException, GridCoverage2D] =
    val acquireReader: IO[IOException, GeoTiffReader] = ZIO.attemptBlockingIO(GeoTiffReader(rasterFile.toIO))
    val releaseReader: GeoTiffReader => UIO[Unit]     = reader => ZIO.succeed(reader.dispose())

    ZIO.scoped:
      for
        reader: GeoTiffReader    <- ZIO.acquireRelease(acquireReader)(releaseReader)
        coverage: GridCoverage2D <- ZIO.attemptBlockingIO(reader.read(Array.empty[GeneralParameterValue]))
      yield coverage

  private def vectorize(coverage: GridCoverage2D): IO[ProcessException, SimpleFeatureCollection] =
    val classificationRanges: List[Range[Integer]] =
      List(
        Range(Int.MinValue, true, 0, false),
        Range(0, true, 10, false),
        Range(10, true, 20, false),
        Range(20, true, 30, false),
        Range(30, true, 40, false),
        Range(40, true, 50, false),
        Range(50, true, Int.MaxValue, true)
      )

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

  private def writeVectorFile(vectorFile: Path)(featureCollection: SimpleFeatureCollection): IO[IOException, Path] =
    ZIO.scoped:
      for
        outputStream: OutputStream <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(write.over.outputStream(vectorFile)))
        writer: GeoJSONWriter      <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(GeoJSONWriter(outputStream)))
        _                          <- ZIO.attemptBlockingIO(writer.writeFeatureCollection(featureCollection))
      yield vectorFile

object TemperatureTransformationService:

  val layer: ZLayer[Any, Nothing, TemperatureTransformationService] =
    ZLayer.derive[TemperatureTransformationService]
