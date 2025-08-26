package dev.a4i.bsc.etl.globalai.historical.extract

import zio.http.URL

import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata.Indicator
import dev.a4i.bsc.etl.globalai.historical.common.GlobalAiHistoricalMetadata.Period.*

type GlobalAiHistoricalDataSource = (
    url: URL,
    metadata: GlobalAiHistoricalMetadata[Annual]
)

object GlobalAiHistoricalDataSource:

  val dataSource: GlobalAiHistoricalDataSource =
    (
      url = URL.decode("http://localhost:9000/7504448.zip").getOrElse(???),
      // url = URL.decode("https://figshare.com/ndownloader/articles/7504448/versions/7").getOrElse(???),
      metadata = GlobalAiHistoricalMetadata(
        Indicator.Average,
        30,
        Annual()
      )
    )
