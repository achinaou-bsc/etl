package dev.a4i.bsc.etl.wad.extract

import zio.http.URL

object WADDataSources:

  val aridity: URL =
    URL
      .decode(
        "https://geospatial.jrc.ec.europa.eu/geoserver/wad/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=wad%3AAI056_int&outputFormat=SHAPE-ZIP"
      )
      .getOrElse(???)
