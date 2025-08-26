package dev.a4i.bsc.etl.common.transform

import java.awt.image.RenderedImage
import java.awt.image.renderable.ParameterBlock
import javax.media.jai.JAI

import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.GridCoverageFactory
import zio.*

class ResolutionReducerService:

  def downsampleByAveraging(coverage: GridCoverage2D, scaleFactor: Int): UIO[GridCoverage2D] =
    ZIO
      .attemptBlocking:
        val image: RenderedImage = coverage.getRenderedImage

        val parameterBlock: ParameterBlock = new ParameterBlock:
          addSource(image)
          add(scaleFactor)
          add(scaleFactor)

        val downsampledImage = JAI.create("SubsampleAverage", parameterBlock)

        GridCoverageFactory().create(
          s"${coverage.getName.toString}-x${scaleFactor}",
          downsampledImage,
          coverage.getEnvelope
        )
      .orDie

object ResolutionReducerService:

  val layer: ULayer[ResolutionReducerService] =
    ZLayer.derive[ResolutionReducerService]
