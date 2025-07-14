package dev.a4i.bsc.etl.common.load

import java.io.IOException
import java.util.UUID

import org.geotools.api.data.SimpleFeatureStore
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.DefaultTransaction
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import zio.*

import dev.a4i.bsc.etl.configuration.PostGISDataStore

class PostGISFeatureWriterService(dataStore: PostGISDataStore):

  def write(tableName: String, featureCollection: SimpleFeatureCollection): ZIO[Scope, IOException, Unit] =
    for
      sourceFeatureType = featureCollection.getSchema
      targetFeatureType = getTargetFeatureType(tableName, sourceFeatureType)
      _                <- createSchema(tableName, targetFeatureType)
      featureStore     <- ZIO.attemptBlockingIO:
                            dataStore
                              .getFeatureSource(tableName)
                              .asInstanceOf[SimpleFeatureStore]
      _                <- usingTransaction: transaction =>
                            ZIO.attemptBlockingIO:
                              featureStore.setTransaction(transaction)
                              featureStore.addFeatures(featureCollection)
    yield ()

  private def getTargetFeatureType(tableName: String, sourceFeatureType: SimpleFeatureType): SimpleFeatureType =
    val featureTypeBuilder = new SimpleFeatureTypeBuilder:
      init(sourceFeatureType)

    featureTypeBuilder.setName(tableName)
    featureTypeBuilder.buildFeatureType

  private def createSchema(tableName: String, featureType: SimpleFeatureType): IO[IOException, Unit] =
    ZIO.attemptBlockingIO:
      if !dataStore.getTypeNames.contains(tableName)
      then dataStore.createSchema(featureType)

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
