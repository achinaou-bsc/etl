package dev.a4i.bsc.etl.worldclim.historical.extract

import os.*
import zio.*
import zio.http.URL

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.DownloadService
import dev.a4i.bsc.etl.common.extract.ExtractionService
import dev.a4i.bsc.etl.common.extract.UnarchivingService

class WorldClimHistoricalExtractionService(
    downloadService: DownloadService,
    unarchivingService: UnarchivingService
) extends ExtractionService:

  def extract(url: URL): RIO[Workspace, Path] =
    for
      workspace: Workspace <- ZIO.service[Workspace]
      archive: Path        <- downloadService.download(url, workspace.path)
      directory: Path      <- unarchivingService.unarchive(archive, workspace.path)
    yield directory

object WorldClimHistoricalExtractionService:

  type Dependencies = DownloadService & UnarchivingService

  val layer: ZLayer[Dependencies, Nothing, WorldClimHistoricalExtractionService] =
    ZLayer.derive[WorldClimHistoricalExtractionService]
