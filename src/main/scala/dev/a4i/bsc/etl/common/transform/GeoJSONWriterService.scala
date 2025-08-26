package dev.a4i.bsc.etl.common.transform

import org.geotools.data.geojson.GeoJSONWriter
import org.geotools.data.simple.SimpleFeatureCollection
import os.*
import zio.*

class GeoJSONWriterService:

  def write(geoJSONFile: Path, featureCollection: SimpleFeatureCollection): URIO[Scope, Path] =
    for
      outputStream <- ZIO.attemptBlocking(os.write.over.outputStream(geoJSONFile)).withFinalizerAuto.orDie
      writer       <- ZIO.attemptBlocking(GeoJSONWriter(outputStream)).withFinalizerAuto.orDie
      _            <- ZIO.attemptBlocking(writer.writeFeatureCollection(featureCollection)).orDie
    yield geoJSONFile

object GeoJSONWriterService:

  val layer: ULayer[GeoJSONWriterService] =
    ZLayer.derive[GeoJSONWriterService]
