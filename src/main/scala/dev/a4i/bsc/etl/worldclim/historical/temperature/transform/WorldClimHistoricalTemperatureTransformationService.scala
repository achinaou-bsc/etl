package dev.a4i.bsc.etl.worldclim.historical.temperature.transform

import java.io.IOException
import java.time.Month

import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.feature.AttributeTypeBuilder
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
    val resolution: Int = metadata.resolution

    val month: Int = metadata.period match
      case Monthly(month: Month) => month.getValue

    val valueAttributeName: String = metadata.indicator match
      case Indicator.Average => "average_temperature"

    val originalSchema: SimpleFeatureType =
      featureCollection.getSchema

    val renamedSchema: SimpleFeatureType =
      val featureTypeBuilder: SimpleFeatureTypeBuilder = SimpleFeatureTypeBuilder()

      featureTypeBuilder.setName(originalSchema.getName)
      featureTypeBuilder.setCRS(originalSchema.getCoordinateReferenceSystem)

      originalSchema.getAttributeDescriptors.forEach: originalAttributeDescriptor =>
        originalAttributeDescriptor.getLocalName match
          case "value" =>
            val attributeTypeBuilder: AttributeTypeBuilder = new AttributeTypeBuilder:
              init(originalAttributeDescriptor)

            val renamedAttributeDescriptor: AttributeDescriptor =
              attributeTypeBuilder.buildDescriptor(
                valueAttributeName,
                originalAttributeDescriptor.getType
              )

            featureTypeBuilder.add(renamedAttributeDescriptor)
          case _       => featureTypeBuilder.add(originalAttributeDescriptor)

      featureTypeBuilder.buildFeatureType

    val finalSchema: SimpleFeatureType =
      val featureTypeBuilder: SimpleFeatureTypeBuilder =
        new SimpleFeatureTypeBuilder:
          init(renamedSchema)

      featureTypeBuilder
        .nillable(false)
        .add("resolution", classOf[Int])

      featureTypeBuilder
        .nillable(false)
        .add("month", classOf[Int])

      featureTypeBuilder.buildFeatureType

    new DecoratingSimpleFeatureCollection(featureCollection):

      override val getSchema: SimpleFeatureType =
        finalSchema

      override def features: SimpleFeatureIterator =
        val featuresIteratorDelegate: SimpleFeatureIterator = delegate.features
        val featureBuilder: SimpleFeatureBuilder            = SimpleFeatureBuilder(finalSchema)

        new SimpleFeatureIterator:

          override def hasNext: Boolean =
            featuresIteratorDelegate.hasNext

          override def next: SimpleFeature =
            val featureDelegate: SimpleFeature = featuresIteratorDelegate.next
            featureBuilder.reset()

            finalSchema.getAttributeDescriptors.forEach: attributeDescriptor =>
              attributeDescriptor.getLocalName match
                case `valueAttributeName` => featureBuilder.add(featureDelegate.getAttribute("value"))
                case "resolution"         => featureBuilder.add(resolution)
                case "month"              => featureBuilder.add(month)
                case other                => featureBuilder.add(featureDelegate.getAttribute(other))

            featureBuilder.buildFeature(featureDelegate.getID)

          override def close: Unit =
            featuresIteratorDelegate.close()

object WorldClimHistoricalTemperatureTransformationService:

  type Dependencies = RasterReaderService & RasterToVectorTransformationService & GeoJSONWriterService

  val layer: URLayer[Dependencies, WorldClimHistoricalTemperatureTransformationService] =
    ZLayer.derive[WorldClimHistoricalTemperatureTransformationService]
