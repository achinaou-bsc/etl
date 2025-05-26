package dev.a4i.bsc.etl.temperature.extract

import os.Path
import zio.URIO
import zio.ZIO
import zio.http.Client
import zio.http.Request
import zio.http.Response
import zio.http.URL
import zio.stream.ZSink

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.ExtractionService

class TemperatureExtractionService(client: Client) extends ExtractionService:

  def extract(url: URL): URIO[Workspace, Path] =
    for
      archive: Path   <- download(url)
      directory: Path <- unarchive(archive)
    yield directory

  private def download(url: URL): URIO[Workspace, Path] =
    ZIO.scoped:
      for
        workspace: Workspace <- ZIO.service[Workspace]
        archive: Path         = workspace.path / url.path.segments.last
        response: Response   <- client.request(Request.get(url)).orDie
        _                    <- response.body.asStream.run(ZSink.fromPath(archive.toNIO)).orDie
      yield archive

  private def unarchive(archive: Path): URIO[Workspace, Path] =
    for
      workspace: Workspace <- ZIO.service[Workspace]
      directory: Path       = workspace.path / archive.baseName
      _                    <- ZIO.attemptBlockingIO(os.makeDir.all(directory)).orDie
      _                    <- ZIO.attemptBlockingIO(os.unzip(archive, directory)).orDie
    yield directory
