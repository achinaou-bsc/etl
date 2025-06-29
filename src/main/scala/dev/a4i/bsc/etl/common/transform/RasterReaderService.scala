package dev.a4i.bsc.etl.common.transform

import java.io.IOException

import os.Path
import zio.*
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.io.GridCoverage2DReader
import org.geotools.coverage.grid.io.GridFormatFinder
import org.geotools.api.parameter.GeneralParameterValue

class RasterReaderService:

  def read(rasterFile: Path): IO[IOException, GridCoverage2D] =
    val coverageReaderZIO: ZIO[Scope, IOException, GridCoverage2DReader] =
      val acquireReader: IO[IOException, GridCoverage2DReader] =
        ZIO.attemptBlockingIO:
          GridFormatFinder
            .findFormat(rasterFile.toIO)
            .getReader(rasterFile.toIO)

      val releaseReader: GridCoverage2DReader => UIO[Unit] = reader => ZIO.succeed(reader.dispose())

      ZIO.acquireRelease(acquireReader)(releaseReader)

    ZIO.scoped:
      for
        coverageReader: GridCoverage2DReader <- coverageReaderZIO
        coverage: GridCoverage2D             <- ZIO.attemptBlockingIO(coverageReader.read(Array.empty[GeneralParameterValue]))
      yield coverage

object RasterReaderService:

  val layer: ULayer[RasterReaderService] =
    ZLayer.derive[RasterReaderService]
