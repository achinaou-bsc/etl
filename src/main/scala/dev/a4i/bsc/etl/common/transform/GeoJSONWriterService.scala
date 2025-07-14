package dev.a4i.bsc.etl.common.transform

import java.io.IOException

import org.geotools.data.geojson.GeoJSONWriter
import org.geotools.data.simple.SimpleFeatureCollection
import os.*
import zio.*

class GeoJSONWriterService:

  def write(geoJSONFile: Path, featureCollection: SimpleFeatureCollection): ZIO[Scope, IOException, Path] =
    for
      outputStream <- ZIO.fromAutoCloseable:
                        ZIO.attemptBlockingIO:
                          os.write.over.outputStream(geoJSONFile)
      writer       <- ZIO.fromAutoCloseable:
                        ZIO.attemptBlockingIO:
                          GeoJSONWriter(outputStream)
      _            <- ZIO.attemptBlockingIO:
                        writer.writeFeatureCollection(featureCollection)
    yield geoJSONFile

object GeoJSONWriterService:

  val layer: ULayer[GeoJSONWriterService] =
    ZLayer.derive[GeoJSONWriterService]
