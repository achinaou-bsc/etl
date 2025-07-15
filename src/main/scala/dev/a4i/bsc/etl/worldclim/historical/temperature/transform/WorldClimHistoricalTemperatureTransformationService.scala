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
import dev.a4i.bsc.etl.common.util.FeatureTypeExtensions.*
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata
import dev.a4i.bsc.etl.worldclim.historical.temperature.common.WorldClimHistoricalTemperatureMetadata.Indicator
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
    new DecoratingSimpleFeatureCollection(featureCollection):

      private val attributeNameMapping: Map[String, String] = Map(
        "value" -> metadata.indicator.match
          case Indicator.Average => "average_temperature"
      )

      private val resolutionAttributeName: String = "resolution"
      private val resolutionAttributeValue: Int   = metadata.resolution

      private val monthAttributeName: String = "month"
      private val monthAttributeValue: Int   = metadata.period match
        case Monthly(month: Month) => month.getValue

      override val getSchema: SimpleFeatureType =
        val featureTypeBuilder: SimpleFeatureTypeBuilder = SimpleFeatureTypeBuilder()

        featureTypeBuilder.init(delegate.getSchema.renameAttributes(attributeNameMapping))

        featureTypeBuilder
          .nillable(false)
          .add(resolutionAttributeName, resolutionAttributeValue.getClass)

        featureTypeBuilder
          .nillable(false)
          .add(monthAttributeName, monthAttributeValue.getClass)

        featureTypeBuilder.buildFeatureType

      override def features: SimpleFeatureIterator =
        val swappedAttributeNameMapping: Map[String, String] = attributeNameMapping.map(_.swap)

        val decoratedFeatureType: SimpleFeatureType         = getSchema
        val featuresIteratorDelegate: SimpleFeatureIterator = delegate.features
        val featureBuilder: SimpleFeatureBuilder            = SimpleFeatureBuilder(decoratedFeatureType)

        new SimpleFeatureIterator:

          override def hasNext: Boolean =
            featuresIteratorDelegate.hasNext

          override def next: SimpleFeature =
            val featureDelegate: SimpleFeature = featuresIteratorDelegate.next
            featureBuilder.reset()

            decoratedFeatureType.getAttributeDescriptors.forEach: attributeDescriptor =>
              swappedAttributeNameMapping.get(attributeDescriptor.getLocalName) match
                case Some(originalAttributeName) =>
                  featureBuilder.add(featureDelegate.getAttribute(originalAttributeName))
                case None                        =>
                  attributeDescriptor.getLocalName match
                    case `resolutionAttributeName` => featureBuilder.add(resolutionAttributeValue)
                    case `monthAttributeName`      => featureBuilder.add(monthAttributeValue)
                    case other                     => featureBuilder.add(featureDelegate.getAttribute(other))

            featureBuilder.buildFeature(featureDelegate.getID)

          override def close: Unit =
            featuresIteratorDelegate.close()

object WorldClimHistoricalTemperatureTransformationService:

  type Dependencies = RasterReaderService & RasterToVectorTransformationService & GeoJSONWriterService

  val layer: URLayer[Dependencies, WorldClimHistoricalTemperatureTransformationService] =
    ZLayer.derive[WorldClimHistoricalTemperatureTransformationService]
