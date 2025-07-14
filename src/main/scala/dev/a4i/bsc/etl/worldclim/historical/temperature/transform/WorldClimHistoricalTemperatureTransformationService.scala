package dev.a4i.bsc.etl.worldclim.historical.temperature.transform

import java.io.IOException
import java.time.Month

import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.feature.collection.DecoratingSimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.process.ProcessException
import os.*
import zio.*

import dev.a4i.bsc.etl.common.transform.GeoJSONWriterService
import dev.a4i.bsc.etl.common.transform.RasterReaderService
import dev.a4i.bsc.etl.common.transform.RasterToVectorTransformationService
import dev.a4i.bsc.etl.common.transform.TransformationService
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata.Period.*

class WorldClimHistoricalTemperatureTransformationService(
    rasterReaderService: RasterReaderService,
    rasterToVectorTransformationService: RasterToVectorTransformationService,
    geoJSONWriterService: GeoJSONWriterService
) extends TransformationService:

  def transform(
      metadata: WorldClimHistoricalTemperatureMetadata[Monthly],
      rasterFile: Path,
      geoJSONFile: Path
  ): IO[IOException | ProcessException, Path] =
    ZIO.scoped:
      for
        coverage                     <- rasterReaderService.read(rasterFile)
        featureCollection            <- rasterToVectorTransformationService.transform(coverage)
        featureCollectionWithMetadata = migrate(metadata, featureCollection)
        _                            <- geoJSONWriterService.write(geoJSONFile, featureCollectionWithMetadata)
      yield geoJSONFile

  private def migrate(
      metadata: WorldClimHistoricalTemperatureMetadata[Monthly],
      featureCollection: SimpleFeatureCollection
  ): SimpleFeatureCollection =
    val schema: SimpleFeatureType =
      val featureTypeBuilder: SimpleFeatureTypeBuilder =
        new SimpleFeatureTypeBuilder:
          init(featureCollection.getSchema)

      featureTypeBuilder
        .nillable(false)
        .add("indicator", classOf[String])

      featureTypeBuilder
        .nillable(false)
        .add("resolution", classOf[Int])

      featureTypeBuilder
        .nillable(false)
        .add("month", classOf[Int])

      featureTypeBuilder.buildFeatureType

    val indicator: String = metadata.indicator.toString
    val resolution: Int   = metadata.resolution
    val month: Int        = metadata.period match
      case Monthly(month: Month) => month.getValue

    new DecoratingSimpleFeatureCollection(featureCollection):

      override val getSchema: SimpleFeatureType =
        schema

      override def features: SimpleFeatureIterator =
        val featuresIteratorDelegate: SimpleFeatureIterator = delegate.features
        val featureBuilder: SimpleFeatureBuilder            = SimpleFeatureBuilder(schema)

        new SimpleFeatureIterator:

          override def hasNext: Boolean =
            featuresIteratorDelegate.hasNext

          override def next: SimpleFeature =
            val featureDelegate: SimpleFeature = featuresIteratorDelegate.next
            featureBuilder.reset()
            featureBuilder.addAll(featureDelegate.getAttributes)
            featureBuilder.add(indicator)
            featureBuilder.add(resolution)
            featureBuilder.add(month)
            featureBuilder.buildFeature(featureDelegate.getID)

          override def close: Unit =
            featuresIteratorDelegate.close()

object WorldClimHistoricalTemperatureTransformationService:

  type Dependencies = RasterReaderService & RasterToVectorTransformationService & GeoJSONWriterService

  val layer: URLayer[Dependencies, WorldClimHistoricalTemperatureTransformationService] =
    ZLayer.derive[WorldClimHistoricalTemperatureTransformationService]
