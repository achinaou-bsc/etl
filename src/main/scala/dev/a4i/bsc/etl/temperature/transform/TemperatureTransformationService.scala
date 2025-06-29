package dev.a4i.bsc.etl.temperature.transform

import java.io.IOException

import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.process.ProcessException
import os.*
import zio.*

import dev.a4i.bsc.etl.common.transform.GeoJSONWriterService
import dev.a4i.bsc.etl.common.transform.RasterReaderService
import dev.a4i.bsc.etl.common.transform.RasterToVectorTransformationService
import dev.a4i.bsc.etl.common.transform.TransformationService

class TemperatureTransformationService(
    rasterReaderService: RasterReaderService,
    rasterToVectorTransformationService: RasterToVectorTransformationService,
    geoJSONWriterService: GeoJSONWriterService
) extends TransformationService:

  def transform(rasterFile: Path, geoJSONFile: Path): IO[IOException | ProcessException, Path] =
    ZIO.scoped:
      for
        _                                          <- ZIO.log(s"Vectorizing: $rasterFile -> $geoJSONFile")
        coverage: GridCoverage2D                   <- rasterReaderService.read(rasterFile)
        featureCollection: SimpleFeatureCollection <- rasterToVectorTransformationService.transform(coverage)
        _                                          <- geoJSONWriterService.write(geoJSONFile)(featureCollection)
        _                                          <- ZIO.log(s"Vectorized: $rasterFile -> $geoJSONFile")
      yield geoJSONFile

object TemperatureTransformationService:

  type Dependencies = RasterReaderService & RasterToVectorTransformationService & GeoJSONWriterService

  val layer: URLayer[Dependencies, TemperatureTransformationService] =
    ZLayer.derive[TemperatureTransformationService]
