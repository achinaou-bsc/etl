package dev.a4i.bsc.etl.common.extract

import scala.util.matching.Regex

import os.*
import zio.*

class UnarchivingService:

  def unarchive(archive: Path, outputDirectory: Path, includePatterns: Seq[Regex] = Seq.empty): UIO[Path] =
    for
      directory = outputDirectory / archive.baseName
      _        <- ZIO.attemptBlocking(makeDir.all(directory)).orDie
      _        <- ZIO.attemptBlocking(unzip(archive, directory, includePatterns = includePatterns)).orDie
    yield directory

object UnarchivingService:

  val layer: ULayer[UnarchivingService] =
    ZLayer.derive[UnarchivingService]
