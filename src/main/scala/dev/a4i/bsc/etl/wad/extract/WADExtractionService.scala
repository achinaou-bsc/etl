package dev.a4i.bsc.etl.wad.extract

import os.*
import zio.*
import zio.http.URL

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.extract.DownloadService
import dev.a4i.bsc.etl.common.extract.ExtractionService
import dev.a4i.bsc.etl.common.extract.UnarchivingService

class WADExtractionService(
    downloadService: DownloadService,
    unarchivingService: UnarchivingService
) extends ExtractionService:

  def extract(url: URL): RIO[Workspace, Path] =
    for
      _         <- ZIO.log("Extracting: Aridity")
      workspace <- ZIO.service[Workspace]
      archive   <- downloadService.download(url, workspace.path)
      directory <- unarchivingService.unarchive(archive, workspace.path)
      _         <- ZIO.log("Extracted: Aridity")
    yield directory

object WADExtractionService:

  type Dependencies = DownloadService & UnarchivingService

  val layer: URLayer[Dependencies, WADExtractionService] =
    ZLayer.derive[WADExtractionService]
