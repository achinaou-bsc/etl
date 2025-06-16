package dev.a4i.bsc.etl.configuration

import zio.*
import zio.http.*
import zio.http.netty.NettyConfig

type Client = zio.http.Client

object Client:

  val layer: ULayer[Client] =
    ZLayer
      .make[Client](
        ZLayer.succeed(ZClient.Config.default.idleTimeout(5.minutes).disabledConnectionPool),
        ZLayer.succeed(NettyConfig.default),
        DnsResolver.default,
        ZClient.live
      )
      .orDie
