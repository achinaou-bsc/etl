package dev.a4i.bsc.etl.globalai.historical.extract

import os.*
import zio.*
import zio.http.URL

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.DownloadService
import dev.a4i.bsc.etl.common.extract.ExtractionService
import dev.a4i.bsc.etl.common.extract.UnarchivingService

class GlobalAiHistoricalExtractionService(
    downloadService: DownloadService,
    unarchivingService: UnarchivingService
) extends ExtractionService:

  def extract(url: URL): RIO[Workspace, Path] =
    for
      workspace: Workspace <- ZIO.service[Workspace]
      archive: Path        <- downloadService.download(url, workspace.path)
      directory: Path      <- unarchivingService.unarchive(archive, workspace.path, Seq("Global_AI__monthly_v3_1\\.zip".r))
      childArchive: Path    = directory / "Global_AI__monthly_v3_1.zip"
      childDirectory: Path <- unarchivingService.unarchive(childArchive, workspace.path)
    yield childDirectory / "Global_AI__monthly_v3_1"

object GlobalAiHistoricalExtractionService:

  type Dependencies = DownloadService & UnarchivingService

  val layer: ZLayer[Dependencies, Nothing, GlobalAiHistoricalExtractionService] =
    ZLayer.derive[GlobalAiHistoricalExtractionService]
