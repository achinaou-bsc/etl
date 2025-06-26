package dev.a4i.bsc.etl.desertification.load

import java.io.IOException
import java.net.URL
import scala.jdk.CollectionConverters.*

import com.augustnagro.magnum.magzio.*
import com.augustnagro.magnum.sql
import org.geotools.api.data.DataStore
import org.geotools.api.data.DataStoreFinder
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import os.*
import zio.*
import zio.stream.ZStream

import dev.a4i.bsc.etl.common.load.LoadingService
import org.geotools.api.data.SimpleFeatureSource

class DesertificationLoadingService(xa: TransactorZIO) extends LoadingService:

  def load(vectorFile: Path): ZIO[Any, Throwable, Unit] =
    for
      _ <- ZIO.log("Loading: Desertification")
      _ <- readAndPersist(vectorFile)
      _ <- ZIO.log("Loaded: Desertification")
    yield ()

  private def readAndPersist(vectorFile: Path): Task[Unit] =
    ZIO.scoped:
      for
        _                                          <- ZIO.log(s"Reading & Persisting: $vectorFile")
        featureCollection: SimpleFeatureCollection <- readVectorFile(vectorFile)
        _                                          <- persistFeatures(featureCollection)
        _                                          <- ZIO.log(s"Read & Persisted: $vectorFile")
      yield ()

  private def readVectorFile(vectorFile: Path): ZIO[Scope, IOException, SimpleFeatureCollection] =
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

  private def persistFeatures(featureCollection: SimpleFeatureCollection): RIO[Scope, Unit] =
    for
      featureIterator: SimpleFeatureIterator <- ZIO.fromAutoCloseable(ZIO.succeed(featureCollection.features))
      _                                      <- ZStream
                                                  .unfold(featureIterator): iterator =>
                                                    Option.when(iterator.hasNext)((iterator.next, iterator))
                                                  .runForeach: feature =>
                                                    ZIO.log(s"Persisting: ${feature.getID}") // xa.transact(sql"???".update.run())
    yield ()

object DesertificationLoadingService:

  val layer: ZLayer[TransactorZIO, Nothing, DesertificationLoadingService] =
    ZLayer.derive[DesertificationLoadingService]
