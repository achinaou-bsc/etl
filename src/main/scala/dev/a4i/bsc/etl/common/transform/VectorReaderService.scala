package dev.a4i.bsc.etl.common.transform

import java.io.IOException
import java.net.URL
import scala.jdk.CollectionConverters.*

import org.geotools.api.data.DataStore
import org.geotools.api.data.DataStoreFinder
import org.geotools.api.data.SimpleFeatureSource
import org.geotools.data.simple.SimpleFeatureCollection
import os.Path
import zio.*

class VectorReaderService:

  def read(vectorFile: Path): ZIO[Scope, IOException, SimpleFeatureCollection] =
    val dataStoreZIO: ZIO[Scope, IOException, DataStore] =
      val parameters: Map[String, URL]        = Map("url" -> vectorFile.toNIO.toUri.toURL)
      val acquire: IO[IOException, DataStore] = ZIO.attemptBlockingIO(DataStoreFinder.getDataStore(parameters.asJava))
      val release: DataStore => UIO[Unit]     = dataStore => ZIO.succeed(dataStore.dispose())

      ZIO.acquireRelease(acquire)(release)

    for
      dataStore: DataStore                       <- dataStoreZIO
      typeName: String                           <- ZIO.attemptBlockingIO(dataStore.getTypeNames.head)
      source: SimpleFeatureSource                <- ZIO.attemptBlockingIO(dataStore.getFeatureSource(typeName))
      featureCollection: SimpleFeatureCollection <- ZIO.attemptBlockingIO(source.getFeatures())
    yield featureCollection

object VectorReaderService:

  val layer: ULayer[VectorReaderService] =
    ZLayer.derive[VectorReaderService]
