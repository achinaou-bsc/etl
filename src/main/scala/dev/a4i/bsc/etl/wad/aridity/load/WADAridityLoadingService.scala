package dev.a4i.bsc.etl.wad.aridity.load

import os.*
import zio.*

import dev.a4i.bsc.etl.common.load.LoadingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.VectorReaderService

class WADAridityLoadingService(
    vectorReaderService: VectorReaderService,
    postGISFeatureWriterService: PostGISFeatureWriterService
) extends LoadingService:

  def load(vectorFile: Path): Task[Unit] =
    ZIO.scoped:
      for
        _                 <- ZIO.log(s"Reading & Persisting: $vectorFile")
        featureCollection <- vectorReaderService.read(vectorFile)
        _                 <- postGISFeatureWriterService.write("wad_aridity", featureCollection)
        _                 <- ZIO.log(s"Read & Persisted: $vectorFile")
      yield ()

object WADAridityLoadingService:

  type Dependencies = VectorReaderService & PostGISFeatureWriterService

  val layer: URLayer[Dependencies, WADAridityLoadingService] =
    ZLayer.derive[WADAridityLoadingService]
