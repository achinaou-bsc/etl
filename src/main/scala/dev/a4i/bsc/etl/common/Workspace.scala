package dev.a4i.bsc.etl.common

import java.io.IOException
import java.util.UUID

import os.Path
import zio.*

import dev.a4i.bsc.etl.Configuration

case class Workspace private (id: UUID, path: Path)

object Workspace:

  def layer: Layer[Config.Error | IOException, Workspace] =
    ZLayer.scoped(ZIO.acquireRelease(create)(delete))

  private def create: IO[Config.Error | IOException, Workspace] =
    for
      configuration: Configuration <- ZIO.config[Configuration]
      id: UUID                      = UUID.randomUUID
      path: Path                    = configuration.workspaces / id.toString
      _                            <- ZIO.attemptBlockingIO(os.makeDir.all(path))
    yield Workspace(id, path)

  private def delete(workspace: Workspace): UIO[Unit] =
    ZIO.attemptBlockingIO(os.remove.all(workspace.path)).orDie
