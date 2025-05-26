package dev.a4i.bsc.etl

import os.Path
import zio.Config
import zio.config.magnolia.*

case class Configuration(workspaces: Path) derives Config

object Configuration:

  given Config[Path] = Config.string.map(Path(_))
