package dev.a4i.bsc.etl.common.extract

import zio.http.URL

object TemperatureDataSources:

  object Average:

    object Historical:

      val tenMinutes: URL =
        URL
          .decode("https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_10m_tavg.zip")
          .getOrElse(???)

      val fiveMinutes: URL =
        URL
          .decode("https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_5m_tavg.zip")
          .getOrElse(???)

      val twoAndHalfMinutes: URL =
        URL
          .decode("https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_2.5m_tavg.zip")
          .getOrElse(???)

      val thirtySeconds: URL =
        URL
          .decode("https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_30s_tavg.zip")
          .getOrElse(???)
