package dev.a4i.bsc.etl.common.transform

import java.net.URL
import scala.jdk.CollectionConverters.*

import org.geotools.api.data.DataStore
import org.geotools.api.data.DataStoreFinder
import org.geotools.data.simple.SimpleFeatureCollection
import os.Path
import zio.*

class VectorReaderService:

  def read(vectorFile: Path): URIO[Scope, SimpleFeatureCollection] =
    val dataStoreZIO: URIO[Scope, DataStore] =
      val parameters: Map[String, URL]    = Map("url" -> vectorFile.toNIO.toUri.toURL)
      val acquire: Task[DataStore]        = ZIO.attemptBlocking(DataStoreFinder.getDataStore(parameters.asJava))
      val release: DataStore => UIO[Unit] = dataStore => ZIO.succeed(dataStore.dispose())

      ZIO.acquireRelease(acquire)(release).orDie

    for
      dataStore         <- dataStoreZIO
      typeName          <- ZIO.attemptBlocking(dataStore.getTypeNames.head).orDie
      featureSource     <- ZIO.attemptBlocking(dataStore.getFeatureSource(typeName)).orDie
      featureCollection <- ZIO.attemptBlocking(featureSource.getFeatures()).orDie
    yield featureCollection

object VectorReaderService:

  val layer: ULayer[VectorReaderService] =
    ZLayer.derive[VectorReaderService]
