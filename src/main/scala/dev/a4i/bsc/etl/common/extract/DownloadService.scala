package dev.a4i.bsc.etl.common.extract

import os.Path
import zio.*
import zio.http.*
import zio.stream.ZSink

import dev.a4i.bsc.etl.configuration.HttpClient

class DownloadService(client: HttpClient):

  def download(url: URL, outputDirectory: Path): Task[Path] =
    ZIO.scoped:
      for
        response     <- client.request(Request.get(url))
        filenameRegex = """.*; filename="?(.*)"?""".r
        archiveName   = response.headers
                          .get("Content-Disposition")
                          .flatMap:
                            case filenameRegex(filename) => Some(filename)
                            case _                       => None
                          .getOrElse(url.path.segments.last)
        archive       = outputDirectory / archiveName
        _            <- response.body.asStream.run(ZSink.fromPath(archive.toNIO))
      yield archive

object DownloadService:

  val layer: URLayer[HttpClient, DownloadService] =
    ZLayer.derive[DownloadService]
