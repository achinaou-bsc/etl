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
      vectorFiles: Seq[Path] <- findVectorFiles(vectorDirectory)
      _                      <- ZIO.foreachDiscard(vectorFiles): vectorFile =>
                                  readVectorFile(vectorFile).flatMap(persistFeatures)
    yield ()

  private def findVectorFiles(directory: Path): ZIO[Any, IOException, Seq[Path]] =
    val vectorFileExtensions: Set[String] = Set("geojson")

    ZIO.attemptBlockingIO:
      walk(directory)
        .filter(isFile)
        .filter(file => vectorFileExtensions.contains(file.ext.toLowerCase(Locale.ROOT)))

  private def readVectorFile(vectorFile: Path): ZIO[Any, IOException, SimpleFeatureCollection] =
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
