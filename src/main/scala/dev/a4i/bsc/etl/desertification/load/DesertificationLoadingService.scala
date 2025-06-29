package dev.a4i.bsc.etl.desertification.load

import org.geotools.data.simple.SimpleFeatureCollection
import os.*
import zio.*

import dev.a4i.bsc.etl.common.load.LoadingService
import dev.a4i.bsc.etl.common.load.PostGISFeatureWriterService
import dev.a4i.bsc.etl.common.transform.VectorReaderService

class DesertificationLoadingService(
    vectorReaderService: VectorReaderService,
    postGISFeatureWriterService: PostGISFeatureWriterService
) extends LoadingService:

  def load(vectorFile: Path): ZIO[Any, Throwable, Unit] =
    ZIO.scoped:
      for
        _                                          <- ZIO.log(s"Reading & Persisting: $vectorFile")
        featureCollection: SimpleFeatureCollection <- vectorReaderService.read(vectorFile)
        _                                          <- postGISFeatureWriterService.write(featureCollection)
        _                                          <- ZIO.log(s"Read & Persisted: $vectorFile")
      yield ()

object DesertificationLoadingService:

  type Dependencies = VectorReaderService & PostGISFeatureWriterService

  val layer: ZLayer[Dependencies, Nothing, DesertificationLoadingService] =
    ZLayer.derive[DesertificationLoadingService]
