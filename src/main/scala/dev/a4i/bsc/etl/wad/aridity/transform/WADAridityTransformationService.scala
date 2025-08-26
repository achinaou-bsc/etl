package dev.a4i.bsc.etl.wad.aridity.transform

import java.io.IOException
import java.util.Locale

import os.*
import zio.*

import dev.a4i.bsc.etl.common.Workspace
import dev.a4i.bsc.etl.common.transform.GeoJSONWriterService
import dev.a4i.bsc.etl.common.transform.TransformationService
import dev.a4i.bsc.etl.common.transform.VectorReaderService

class WADAridityTransformationService(
    vectorReaderService: VectorReaderService,
    geoJSONWriterService: GeoJSONWriterService
) extends TransformationService:

  def transform(shapefileDirectory: Path): URIO[Workspace, Path] =
    for
      workspace  <- ZIO.service[Workspace]
      shapeFile  <- findShapeFile(shapefileDirectory)
      geoJSONFile = workspace.path / s"${shapeFile.baseName}.geojson"
      _          <- transform(shapeFile, geoJSONFile)
    yield geoJSONFile

  private def findShapeFile(directory: Path): UIO[Path] =
    val extensions: Set[String] = Set("shp")

    ZIO
      .attemptBlocking:
        walk(directory)
          .filter(isFile)
          .find(file => extensions.contains(file.ext.toLowerCase(Locale.ROOT)))
          .getOrElse(throw IOException(s"No shapefile found in directory: $directory"))
      .orDie

  private def transform(shapeFile: Path, geoJSONFile: Path): UIO[Path] =
    ZIO.scoped:
      for
        featureCollection <- vectorReaderService.read(shapeFile)
        _                 <- geoJSONWriterService.write(geoJSONFile, featureCollection)
      yield geoJSONFile

object WADAridityTransformationService:

  type Dependencies = VectorReaderService & GeoJSONWriterService

  val layer: URLayer[Dependencies, WADAridityTransformationService] =
    ZLayer.derive[WADAridityTransformationService]
