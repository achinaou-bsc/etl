package dev.a4i.bsc.etl.desertification.extract

import java.io.IOException
import scala.util.matching.Regex

import os.*
import os.Path
import zio.*
import zio.http.*
import zio.stream.ZSink

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.ExtractionService

class DesertificationExtractionService(client: Client) extends ExtractionService:

  def extract(url: URL): RIO[Workspace, Path] =
    for
      _               <- ZIO.log("Extracting: Desertification")
      archive: Path   <- download(url)
      directory: Path <- unarchive(archive)
      _               <- ZIO.log("Extracted: Desertification")
    yield directory

  private def download(url: URL): RIO[Workspace, Path] =
    val filenameRegex: Regex = """.*; filename=(.*)""".r

    ZIO.scoped:
      for
        workspace: Workspace <- ZIO.service[Workspace]
        _                    <- ZIO.log(s"Downloading: $url")
        response: Response   <- client.request(Request.get(url))
        archiveName: String   = response.headers
                                  .get("Content-Disposition")
                                  .flatMap:
                                    case filenameRegex(filename) => Some(filename)
                                    case _                       => None
                                  .getOrElse(url.path.segments.last)
        archive: Path         = workspace.path / archiveName
        _                    <- response.body.asStream.run(ZSink.fromPath(archive.toNIO))
        _                    <- ZIO.log(s"Downloaded: $url -> $archive")
      yield archive

  private def unarchive(archive: Path): ZIO[Workspace, IOException, Path] =
    for
      workspace: Workspace <- ZIO.service[Workspace]
      directory: Path       = workspace.path / archive.baseName
      _                    <- ZIO.log(s"Unarchiving: $archive -> $directory")
      _                    <- ZIO.attemptBlockingIO(makeDir.all(directory))
      _                    <- ZIO.attemptBlockingIO(unzip(archive, directory))
      _                    <- ZIO.log(s"Unarchived: $archive -> $directory")
    yield directory

object DesertificationExtractionService:

  val layer: ZLayer[Client, Nothing, DesertificationExtractionService] =
    ZLayer.derive[DesertificationExtractionService]
