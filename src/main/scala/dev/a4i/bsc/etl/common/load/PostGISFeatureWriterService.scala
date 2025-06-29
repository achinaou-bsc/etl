package dev.a4i.bsc.etl.common.load

import com.augustnagro.magnum.magzio.TransactorZIO
import com.augustnagro.magnum.sql
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import zio.*
import zio.stream.ZStream

class PostGISFeatureWriterService(xa: TransactorZIO):

  def write(featureCollection: SimpleFeatureCollection): URIO[Scope, Unit] =
    for
      featureIterator: SimpleFeatureIterator <- ZIO.fromAutoCloseable(ZIO.succeed(featureCollection.features))
      _                                      <- ZStream
                                                  .unfold(featureIterator): iterator =>
                                                    Option.when(iterator.hasNext)((iterator.next, iterator))
                                                  .runForeach: feature =>
                                                    ZIO.log(s"Persisting: ${feature.getID}") // xa.transact(sql"???".update.run())
    yield ()

object PostGISFeatureWriterService:

  val layer: URLayer[TransactorZIO, PostGISFeatureWriterService] =
    ZLayer.derive[PostGISFeatureWriterService]
