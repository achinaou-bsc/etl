package dev.a4i.bsc.etl.temperature.extract

import java.io.IOException

import os.{Path, *}
import zio.*
import zio.http.*
import zio.stream.ZSink

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.ExtractionService

class TemperatureExtractionService(client: Client) extends ExtractionService:

  def extract(url: URL): RIO[Workspace, Path] =
    for
      archive: Path   <- download(url)
      directory: Path <- unarchive(archive)
    yield directory

  private def download(url: URL): RIO[Workspace, Path] =
    ZIO.scoped:
      for
        workspace: Workspace <- ZIO.service[Workspace]
        archive: Path         = workspace.path / url.path.segments.last
        response: Response   <- client.request(Request.get(url))
        _                    <- response.body.asStream.run(ZSink.fromPath(archive.toNIO))
      yield archive

  private def unarchive(archive: Path): ZIO[Workspace, IOException, Path] =
    for
      workspace: Workspace <- ZIO.service[Workspace]
      directory: Path       = workspace.path / archive.baseName
      _                    <- ZIO.attemptBlockingIO(makeDir.all(directory))
      _                    <- ZIO.attemptBlockingIO(unzip(archive, directory))
    yield directory
