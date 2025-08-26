package dev.a4i.bsc.etl.common.transform

import org.geotools.api.parameter.GeneralParameterValue
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.io.GridCoverage2DReader
import org.geotools.coverage.grid.io.GridFormatFinder
import os.Path
import zio.*

class RasterReaderService:

  def read(rasterFile: Path): UIO[GridCoverage2D] =
    val coverageReaderZIO: URIO[Scope, GridCoverage2DReader] =
      val acquireReader: Task[GridCoverage2DReader] =
        ZIO.attemptBlocking:
          GridFormatFinder
            .findFormat(rasterFile.toIO)
            .getReader(rasterFile.toIO)

      val releaseReader: GridCoverage2DReader => UIO[Unit] = reader => ZIO.succeed(reader.dispose())

      ZIO.acquireRelease(acquireReader)(releaseReader).orDie

    ZIO.scoped:
      for
        coverageReader: GridCoverage2DReader <- coverageReaderZIO
        coverage: GridCoverage2D             <- ZIO.attemptBlocking(coverageReader.read(Array.empty[GeneralParameterValue])).orDie
      yield coverage

object RasterReaderService:

  val layer: ULayer[RasterReaderService] =
    ZLayer.derive[RasterReaderService]
