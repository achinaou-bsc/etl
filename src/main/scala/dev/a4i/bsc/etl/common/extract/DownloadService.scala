package dev.a4i.bsc.etl.common.extract

import scala.util.matching.Regex

import os.Path
import zio.*
import zio.http.*
import zio.stream.ZSink

class DownloadService(client: Client):

  def download(url: URL, outputDirectory: Path): Task[Path] =
    val filenameRegex: Regex = """.*; filename="?(.*)"?""".r

    ZIO.scoped:
      for
        _                  <- ZIO.log(s"Downloading: $url")
        response: Response <- client.request(Request.get(url))
        archiveName: String = response.headers
                                .get("Content-Disposition")
                                .flatMap:
                                  case filenameRegex(filename) => Some(filename)
                                  case _                       => None
                                .getOrElse(url.path.segments.last)
        archive: Path       = outputDirectory / archiveName
        _                  <- response.body.asStream.run(ZSink.fromPath(archive.toNIO))
        _                  <- ZIO.log(s"Downloaded: $url -> $archive")
      yield archive

object DownloadService:

  val layer: URLayer[Client, DownloadService] =
    ZLayer.derive[DownloadService]
