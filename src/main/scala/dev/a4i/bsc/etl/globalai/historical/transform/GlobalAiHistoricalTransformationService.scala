package dev.a4i.bsc.etl.globalai.historical.transform

import java.time.Month
import scala.jdk.CollectionConverters.*

import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.GridCoverageFactory
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.feature.collection.DecoratingSimpleFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.referencing.CRS
import org.jaitools.numeric.Range
import os.*
import zio.*

import dev.a4i.bsc.etl.common.transform.GeoJSONWriterService
import dev.a4i.bsc.etl.common.transform.RasterReaderService
import dev.a4i.bsc.etl.common.transform.RasterToVectorTransformationService
import dev.a4i.bsc.etl.common.transform.TransformationService
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata.Period.*

class GlobalAiHistoricalTransformationService(
    rasterReaderService: RasterReaderService,
    rasterToVectorTransformationService: RasterToVectorTransformationService,
    geoJSONWriterService: GeoJSONWriterService
) extends TransformationService:

  def transform(
      metadata: GlobalAiHistoricalMetadata[Monthly],
      rasterFile: Path,
      geoJSONFile: Path
  ): UIO[(Path, GlobalAiHistoricalMetadata[Monthly])] =
    val noDataValues: Seq[Number] = Seq(0) // Assuming 0 is the no-data value for Global AI dataset

    val classificationRanges: Seq[Range[Integer]] = Seq(
      Range.create(1, true, 300, false),      // Hyper-arid (AI: 0.0001 to 0.03)
      Range.create(300, true, 2000, false),   // Arid (AI: 0.03 to 0.20)
      Range.create(2000, true, 5000, false),  // Semi-arid (AI: 0.20 to 0.50)
      Range.create(5000, true, 6500, false),  // Dry sub-humid (AI: 0.50 to 0.65)
      Range.create(6500, true, 10000, false), // Humid moisture-limited (AI: 0.65 to 1.00)
      Range.create(10000, true, 65535, true)  // Humid energy-limited (AI: >1.00, includes clipped values)
    )

    ZIO.scoped:
      for
        _                            <- ZIO.log("Reading Raster...")
        coverage                     <- rasterReaderService.read(rasterFile)
        patchedCoverage              <- patch(coverage)
        _                            <- ZIO.log("Converting to Vector...")
        featureCollection            <- rasterToVectorTransformationService.transform(
                                          patchedCoverage,
                                          noDataValues,
                                          classificationRanges
                                        )
        _                            <- ZIO.log("Decorating...")
        featureCollectionWithMetadata = migrate(metadata, featureCollection)
        _                            <- ZIO.log("Writing...")
        _                            <- geoJSONWriterService.write(geoJSONFile, featureCollectionWithMetadata)
      yield (geoJSONFile, metadata)

  private def patch(coverage: GridCoverage2D): UIO[GridCoverage2D] =
    ZIO
      .attemptBlocking:
        GridCoverageFactory().create(
          s"${coverage.getName().toString}-patched",
          coverage.getRenderedImage,
          CRS.decode("EPSG:4326", /* longitudeFirst */ true),
          coverage.getGridGeometry.getGridToCRS,
          coverage.getSampleDimensions,
          Array.empty,
          Map.empty.asJava
        )
      .orDie

  private def migrate(
      metadata: GlobalAiHistoricalMetadata[Monthly],
      featureCollection: SimpleFeatureCollection
  ): SimpleFeatureCollection =
    new DecoratingSimpleFeatureCollection(featureCollection):

      private val resolutionAttributeName: String = "resolution"
      private val resolutionAttributeValue: Int   = metadata.resolution

      private val monthAttributeName: String = "month"
      private val monthAttributeValue: Int   = metadata.period match
        case Monthly(month: Month) => month.getValue

      override lazy val getSchema: SimpleFeatureType =
        val featureTypeBuilder: SimpleFeatureTypeBuilder = SimpleFeatureTypeBuilder()

        featureTypeBuilder.init(delegate.getSchema)

        featureTypeBuilder
          .nillable(false)
          .add(resolutionAttributeName, resolutionAttributeValue.getClass)

        featureTypeBuilder
          .nillable(false)
          .add(monthAttributeName, monthAttributeValue.getClass)

        featureTypeBuilder.buildFeatureType

      override def features: SimpleFeatureIterator =
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
              attributeDescriptor.getLocalName match
                case `resolutionAttributeName` => featureBuilder.add(resolutionAttributeValue)
                case `monthAttributeName`      => featureBuilder.add(monthAttributeValue)
                case other                     => featureBuilder.add(featureDelegate.getAttribute(other))

            featureBuilder.buildFeature(featureDelegate.getID)

          override def close: Unit =
            featuresIteratorDelegate.close()

object GlobalAiHistoricalTransformationService:

  type Dependencies = RasterReaderService & RasterToVectorTransformationService & GeoJSONWriterService

  val layer: URLayer[Dependencies, GlobalAiHistoricalTransformationService] =
    ZLayer.derive[GlobalAiHistoricalTransformationService]
