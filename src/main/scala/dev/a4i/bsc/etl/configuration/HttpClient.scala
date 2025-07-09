package dev.a4i.bsc.etl.configuration

import zio.*
import zio.http.*
import zio.http.netty.NettyConfig

type HttpClient = zio.http.Client

object HttpClient:

  val layer: ULayer[HttpClient] =
    ZLayer
      .make[HttpClient](
        ZLayer.succeed(ZClient.Config.default.idleTimeout(5.minutes).disabledConnectionPool),
        ZLayer.succeed(NettyConfig.default),
        DnsResolver.default,
        ZClient.live
      )
      .orDie
