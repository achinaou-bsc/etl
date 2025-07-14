package dev.a4i.bsc.etl.common.extract

import java.io.IOException

import os.*
import zio.*

class UnarchivingService:

  def unarchive(archive: Path, outputDirectory: Path): IO[IOException, Path] =
    val directory: Path = outputDirectory / archive.baseName

    for
      _ <- ZIO.log(s"Unarchiving: $archive -> $directory")
      _ <- ZIO.attemptBlockingIO(makeDir.all(directory))
      _ <- ZIO.attemptBlockingIO(unzip(archive, directory))
      _ <- ZIO.log(s"Unarchived: $archive -> $directory")
    yield directory

object UnarchivingService:

  val layer: ULayer[UnarchivingService] =
    ZLayer.derive[UnarchivingService]
