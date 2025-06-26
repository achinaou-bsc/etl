package dev.a4i.bsc.etl.desertification.transform

import java.io.IOException
import java.io.OutputStream
import java.net.URL
import java.util.Locale
import scala.jdk.CollectionConverters.*

import org.geotools.api.data.DataStore
import org.geotools.api.data.DataStoreFinder
import org.geotools.api.data.SimpleFeatureSource
import org.geotools.data.geojson.GeoJSONWriter
import org.geotools.data.simple.SimpleFeatureCollection
import os.*
import zio.*

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.transform.TransformationService

class DesertificationTransformationService extends TransformationService:

  def transform(shapefileDirectory: Path): ZIO[Workspace, IOException, Path] =
    for
      _                    <- ZIO.log("Transforming: Desertification")
      workspace: Workspace <- ZIO.service[Workspace]
      shapeFile: Path      <- findShapeFile(shapefileDirectory)
      geoJsonFile: Path    <- convert(workspace.path)(shapeFile)
      _                    <- ZIO.log("Transformed: Desertification")
    yield geoJsonFile

  private def findShapeFile(directory: Path): IO[IOException, Path] =
    val extensions: Set[String] = Set("shp")

    ZIO.attemptBlockingIO:
      walk(directory)
        .filter(isFile)
        .find(file => extensions.contains(file.ext.toLowerCase(Locale.ROOT)))
        .get

  private def convert(outputDirectory: Path)(shapeFile: Path): IO[IOException, Path] =
    val geoJsonFile: Path = outputDirectory / s"${shapeFile.baseName}.geojson"

    ZIO.scoped:
      for
        _                                          <- ZIO.log(s"Converting: $shapeFile -> $geoJsonFile")
        featureCollection: SimpleFeatureCollection <- readShapefileFile(shapeFile)
        _                                          <- writeGeoJsonFile(geoJsonFile)(featureCollection)
        _                                          <- ZIO.log(s"Converted: $shapeFile -> $geoJsonFile")
      yield geoJsonFile

  private def readShapefileFile(shapeFile: Path): ZIO[Scope, IOException, SimpleFeatureCollection] =
    val dataStoreZIO: ZIO[Scope, IOException, DataStore] =
      val parameters: Map[String, URL]        = Map("url" -> shapeFile.toNIO.toUri.toURL)
      val acquire: IO[IOException, DataStore] = ZIO.attemptBlockingIO(DataStoreFinder.getDataStore(parameters.asJava))
      val release: DataStore => UIO[Unit]     = dataStore => ZIO.succeed(dataStore.dispose())

      ZIO.acquireRelease(acquire)(release)

    for
      dataStore: DataStore                       <- dataStoreZIO
      typeName: String                           <- ZIO.attemptBlockingIO(dataStore.getTypeNames.head)
      source: SimpleFeatureSource                <- ZIO.attemptBlockingIO(dataStore.getFeatureSource(typeName))
      featureCollection: SimpleFeatureCollection <- ZIO.attemptBlockingIO(source.getFeatures())
    yield featureCollection

  private def writeGeoJsonFile(
      geoJsonFile: Path
  )(
      featureCollection: SimpleFeatureCollection
  ): ZIO[Scope, IOException, Path] =
    for
      outputStream: OutputStream <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(write.over.outputStream(geoJsonFile)))
      writer: GeoJSONWriter      <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(GeoJSONWriter(outputStream)))
      _                          <- ZIO.attemptBlockingIO(writer.writeFeatureCollection(featureCollection))
    yield geoJsonFile

object DesertificationTransformationService:

  val layer: ZLayer[Any, Nothing, DesertificationTransformationService] =
    ZLayer.derive[DesertificationTransformationService]
