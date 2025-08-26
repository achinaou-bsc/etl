package dev.a4i.bsc.etl.common.transform

import scala.jdk.CollectionConverters.*

import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.process.raster.PolygonExtractionProcess
import org.jaitools.numeric.Range
import zio.*

class RasterToVectorTransformationService:

  def transform(
      coverage: GridCoverage2D,
      noDataValues: Seq[Number] = Seq.empty,
      classificationRanges: Seq[Range[Integer]] = Seq.empty
  ): UIO[SimpleFeatureCollection] =
    ZIO
      .attemptBlocking:
        val effectiveNoDataValues: Seq[Number] =
          if noDataValues.nonEmpty
          then noDataValues
          else
            coverage
              .getSampleDimension(0)
              .getNoDataValues
              .map(_.asInstanceOf[Number])
              .toIndexedSeq

        PolygonExtractionProcess().execute(
          coverage,
          0,
          true,
          null,
          effectiveNoDataValues.asJava,
          classificationRanges.asJava,
          null
        )
      .orDie

object RasterToVectorTransformationService:

  val layer: ULayer[RasterToVectorTransformationService] =
    ZLayer.derive[RasterToVectorTransformationService]
