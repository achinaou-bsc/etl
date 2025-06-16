package dev.a4i.bsc.etl.temperature.load

import java.io.InputStream
import java.io.IOException
import java.util.Locale

import com.augustnagro.magnum.magzio.*
import com.augustnagro.magnum.sql
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.data.geojson.GeoJSONReader
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import os.*
import zio.*

import dev.a4i.bsc.etl.common.load.LoadingService

class TemperatureLoadingService(xa: TransactorZIO) extends LoadingService:

  def load(vectorDirectory: Path): ZIO[Any, Throwable, Unit] =
    for
      _                      <- ZIO.log("Loading: Temperature")
      vectorFiles: Seq[Path] <- findVectorFiles(vectorDirectory)
      _                      <- ZIO.foreachDiscard(vectorFiles)(readAndPersist)
      _                      <- ZIO.log("Loaded: Temperature")
    yield ()

  private def findVectorFiles(directory: Path): ZIO[Any, IOException, Seq[Path]] =
    val extensions: Set[String] = Set("geojson")

    ZIO.attemptBlockingIO:
      walk(directory)
        .filter(isFile)
        .filter(file => extensions.contains(file.ext.toLowerCase(Locale.ROOT)))

  private def readAndPersist(vectorFile: Path): Task[Unit] =
    for
      _                                          <- ZIO.log(s"Reading & Persisting: $vectorFile")
      featureCollection: SimpleFeatureCollection <- readVectorFile(vectorFile)
      _                                          <- persistFeatures(featureCollection)
      _                                          <- ZIO.log(s"Read & Persisted: $vectorFile")
    yield ()

  private def readVectorFile(vectorFile: Path): IO[IOException, SimpleFeatureCollection] =
    ZIO.scoped:
      for
        inputStream: InputStream                   <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(read.inputStream(vectorFile)))
        reader: GeoJSONReader                      <- ZIO.fromAutoCloseable(ZIO.attemptBlockingIO(GeoJSONReader(inputStream)))
        featureCollection: SimpleFeatureCollection <- ZIO.attemptBlockingIO(reader.getFeatures)
      yield featureCollection

  private def persistFeatures(featureCollection: SimpleFeatureCollection): Task[Unit] =
    ZIO.scoped:
      for
        featureIterator: SimpleFeatureIterator <- ZIO.fromAutoCloseable(ZIO.succeed(featureCollection.features))
        features: LazyList[SimpleFeature]       = LazyList.unfold(featureIterator): iterator =>
                                                    Option.when(iterator.hasNext)((iterator.next, iterator))
        _                                      <- ZIO.foreach(features): feature =>
                                                    ZIO.log(s"Persisting: ${feature}") // xa.transact(sql"???".update.run())
      yield ()

object TemperatureLoadingService:

  val layer: ZLayer[TransactorZIO, Nothing, TemperatureLoadingService] =
    ZLayer.derive[TemperatureLoadingService]
