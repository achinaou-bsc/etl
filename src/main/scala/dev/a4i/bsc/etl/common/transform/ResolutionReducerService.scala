package dev.a4i.bsc.etl.common.transform

import java.awt.image.RenderedImage
import javax.media.jai.JAI
import javax.media.jai.ParameterBlockJAI

import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.GridCoverageFactory
import zio.*

class ResolutionReducerService:

  def downsampleByAveraging(coverage: GridCoverage2D, reductionFactor: Double): UIO[GridCoverage2D] =
    ZIO
      .attemptBlocking:
        val image: RenderedImage = coverage.getRenderedImage

        val scaleFactor: Double = 1d / reductionFactor

        val parameterBlock: ParameterBlockJAI = new ParameterBlockJAI("SubsampleAverage"):
          addSource(image)
          setParameter("scaleX", scaleFactor)
          setParameter("scaleY", scaleFactor)

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
