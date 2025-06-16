package dev.a4i.bsc.etl.common

import java.io.IOException
import java.util.UUID

import os.*
import zio.*
import zio.Config
import zio.config.magnolia.*

case class Workspace private (id: UUID, path: Path)

object Workspace:

  def layer: Layer[Config.Error | IOException, Workspace] =
    ZLayer.scoped(ZIO.acquireRelease(create)(delete))

  private def create: IO[Config.Error | IOException, Workspace] =
    for
      configuration: Configuration <- ZIO.config(Configuration.config)
      id: UUID                      = UUID.randomUUID
      path: Path                    = configuration.path / id.toString
      _                            <- ZIO.attemptBlockingIO(makeDir.all(path))
    yield Workspace(id, path)

  private def delete(workspace: Workspace): UIO[Unit] =
    for
      configuration: Configuration <- ZIO.config(Configuration.config).orDie
      _                            <- ZIO.whenDiscard(configuration.autoClean):
                                        ZIO.attemptBlockingIO(remove.all(workspace.path)).orDie
    yield ()

  case class Configuration(path: Path, autoClean: Boolean)

  object Configuration:

    private given DeriveConfig[Path] = DeriveConfig[String].map(Path(_))

    val config: Config[Configuration] = deriveConfig[Configuration].nested("workspaces")
