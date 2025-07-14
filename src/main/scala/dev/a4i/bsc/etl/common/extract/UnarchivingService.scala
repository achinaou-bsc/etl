package dev.a4i.bsc.etl.common.extract

import java.io.IOException

import os.*
import zio.*

class UnarchivingService:

  def unarchive(archive: Path, outputDirectory: Path): IO[IOException, Path] =
    for
      directory = outputDirectory / archive.baseName
      _        <- ZIO.attemptBlockingIO(makeDir.all(directory))
      _        <- ZIO.attemptBlockingIO(unzip(archive, directory))
    yield directory

object UnarchivingService:

  val layer: ULayer[UnarchivingService] =
    ZLayer.derive[UnarchivingService]
