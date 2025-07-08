package dev.a4i.bsc.etl.common.load

import java.io.IOException

import org.geotools.api.data.SimpleFeatureStore
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import zio.*

import dev.a4i.bsc.etl.configuration.PostGISDataStore

class PostGISFeatureWriterService(dataStore: PostGISDataStore):

  def write(tableName: String, featureCollection: SimpleFeatureCollection): ZIO[Scope, IOException, Unit] =
    for
      _                                   <- ZIO.unit
      sourceFeatureType: SimpleFeatureType = featureCollection.getSchema
      targetFeatureType: SimpleFeatureType = getTargetFeatureType(tableName, sourceFeatureType)
      _                                   <- createSchema(tableName, targetFeatureType)
      featureStore: SimpleFeatureStore    <- ZIO.attemptBlockingIO:
                                               dataStore
                                                 .getFeatureSource(tableName)
                                                 .asInstanceOf[SimpleFeatureStore]
      _                                   <- ZIO.attemptBlockingIO:
                                               featureStore.addFeatures(featureCollection)
    yield ()

  private def getTargetFeatureType(tableName: String, sourceFeatureType: SimpleFeatureType): SimpleFeatureType =
    val featureTypeBuilder = SimpleFeatureTypeBuilder()
    featureTypeBuilder.init(sourceFeatureType)
    featureTypeBuilder.setName(tableName)
    featureTypeBuilder.buildFeatureType

  private def createSchema(tableName: String, featureType: SimpleFeatureType): IO[IOException, Unit] =
    ZIO.attemptBlockingIO:
      if !dataStore.getTypeNames.contains(tableName)
      then dataStore.createSchema(featureType)

object PostGISFeatureWriterService:

  type Dependencies = PostGISDataStore

  val layer: URLayer[Dependencies, PostGISFeatureWriterService] =
    ZLayer.derive[PostGISFeatureWriterService]
