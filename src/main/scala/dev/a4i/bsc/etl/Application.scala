package dev.a4i.bsc.etl

import zio.Console
import zio.ZIO
import zio.ZIOAppDefault

object Application extends ZIOAppDefault:

  def run: ZIO[Any, Any, Any] =
    Console.printLine("Hello, World!")
