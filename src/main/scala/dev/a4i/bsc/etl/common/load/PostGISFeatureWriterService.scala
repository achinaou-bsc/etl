package dev.a4i.bsc.etl.common.load

import java.util.UUID

import org.geotools.api.data.SimpleFeatureStore
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.DefaultTransaction
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import zio.*

import dev.a4i.bsc.etl.configuration.PostGISDataStore

class PostGISFeatureWriterService(dataStore: PostGISDataStore):

  def write(tableName: String, featureCollection: SimpleFeatureCollection): URIO[Scope, Unit] =
    for
      featureType   = getTargetFeatureType(tableName, featureCollection.getSchema)
      _            <- createSchema(featureType)
      featureStore <- ZIO
                        .attemptBlocking:
                          dataStore
                            .getFeatureSource(tableName)
                            .asInstanceOf[SimpleFeatureStore]
                        .orDie
      _            <- usingTransaction: transaction =>
                        ZIO
                          .attemptBlocking:
                            featureStore.setTransaction(transaction)
                            featureStore.addFeatures(featureCollection)
                          .orDie
    yield ()

  private def getTargetFeatureType(tableName: String, sourceFeatureType: SimpleFeatureType): SimpleFeatureType =
    val featureTypeBuilder = new SimpleFeatureTypeBuilder:
      init(sourceFeatureType)

    featureTypeBuilder.setName(tableName)
    featureTypeBuilder.buildFeatureType

  private def createSchema(featureType: SimpleFeatureType): UIO[Unit] =
    ZIO
      .attemptBlocking:
        if !dataStore.getTypeNames.contains(featureType.getTypeName)
        then dataStore.createSchema(featureType)
      .orDie

  private def usingTransaction[R, E, A](use: DefaultTransaction => ZIO[R, E, A]): ZIO[R, E, A] =
    val acquire: UIO[DefaultTransaction] = ZIO.succeed(DefaultTransaction(UUID.randomUUID.toString))

    val commitRelease: DefaultTransaction => UIO[Unit] = transaction =>
      ZIO.succeed:
        transaction.commit()
        transaction.close()

    val rollbackRelease: DefaultTransaction => UIO[Unit] = transaction =>
      ZIO.succeed:
        transaction.rollback()
        transaction.close()

    val release: (DefaultTransaction, Exit[E, A]) => UIO[Unit] = (transaction, exit) =>
      exit.foldZIO(_ => rollbackRelease(transaction), _ => commitRelease(transaction))

    ZIO.acquireReleaseExitWith(acquire)(release)(use)

object PostGISFeatureWriterService:

  type Dependencies = PostGISDataStore

  val layer: URLayer[Dependencies, PostGISFeatureWriterService] =
    ZLayer.derive[PostGISFeatureWriterService]
