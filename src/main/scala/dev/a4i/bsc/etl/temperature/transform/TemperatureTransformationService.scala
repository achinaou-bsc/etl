package dev.a4i.bsc.etl.temperature.transform

import java.io.IOException
import scala.jdk.CollectionConverters.*

import org.geotools.api.parameter.GeneralParameterValue
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.geojson.GeoJSONWriter
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.gce.geotiff.GeoTiffReader
import org.geotools.process.raster.PolygonExtractionProcess
import os.Path
import zio.*

import dev.a4i.bsc.etl.common.Workspace

class TemperatureTransformationService:

  def transform(rasterDirectory: Path): URIO[Workspace, Path] =
    vectorizeDirectory(rasterDirectory)

  private def vectorizeDirectory(rasterDirectory: Path): URIO[Workspace, Path] =
    for
      workspace: Workspace   <- ZIO.service[Workspace]
      vectorDirectory: Path   = workspace.path / s"${rasterDirectory.baseName}.vector"
      rasterFiles: Seq[Path] <- ZIO.attemptBlockingIO(os.walk(rasterDirectory)).orDie
      _                      <- ZIO.attemptBlockingIO(os.makeDir.all(vectorDirectory)).orDie
      _                      <- ZIO.foreach(rasterFiles)(vectorizeFile(vectorDirectory)).orDie
    yield vectorDirectory

  private def vectorizeFile(vectorizedDirectory: Path)(rasterFile: Path): Task[Path] =
    val vectorFile: Path = vectorizedDirectory / rasterFile.baseName.replace(".tif", ".geojson")

    readRaster(rasterFile)
      .flatMap(vectorize)
      .flatMap(writeVector(vectorFile))

  private def readRaster(rasterFile: Path): IO[IOException, GridCoverage2D] =
    ZIO.attemptBlockingIO(GeoTiffReader(rasterFile.toIO).read(Array.empty[GeneralParameterValue]))

  private def vectorize(coverage: GridCoverage2D): Task[SimpleFeatureCollection] =
    ZIO.attempt:
      PolygonExtractionProcess().execute(
        coverage,
        0,
        true,
        null,
        List.empty.asJava,
        List.empty.asJava,
        null
      )

  private def writeVector(vectorFile: Path)(featureCollection: SimpleFeatureCollection): IO[IOException, Path] =
    ZIO.scoped:
      for
        outputStream <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(os.write.over.outputStream(vectorFile)))
        _            <- ZIO.attemptBlockingIO(GeoJSONWriter(outputStream).writeFeatureCollection(featureCollection))
      yield vectorFile
