package dev.a4i.bsc.etl.common.extract

import java.io.IOException
import scala.util.matching.Regex

import os.*
import zio.*

class UnarchivingService:

  def unarchive(archive: Path, outputDirectory: Path, includePatterns: Seq[Regex] = Seq.empty): IO[IOException, Path] =
    for
      directory = outputDirectory / archive.baseName
      _        <- ZIO.attemptBlockingIO(makeDir.all(directory))
      _        <- ZIO.attemptBlockingIO(unzip(archive, directory, includePatterns = includePatterns))
    yield directory

object UnarchivingService:

  val layer: ULayer[UnarchivingService] =
    ZLayer.derive[UnarchivingService]
