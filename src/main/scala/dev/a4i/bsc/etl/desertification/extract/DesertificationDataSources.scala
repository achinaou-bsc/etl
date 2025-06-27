package dev.a4i.bsc.etl.desertification.extract

import zio.http.URL

object DesertificationDataSources:

  val url: URL =
    URL
      .decode(
        "https://geospatial.jrc.ec.europa.eu/geoserver/wad/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=wad%3Alpd_int2&outputFormat=SHAPE-ZIP"
      )
      .getOrElse(???)
