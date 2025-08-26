package dev.a4i.bsc.etl.common

import java.util.UUID

import os.*
import zio.*
import zio.Config
import zio.config.magnolia.*

case class Workspace private (id: UUID, path: Path)

object Workspace:

  def layer: ULayer[Workspace] =
    ZLayer.scoped(ZIO.acquireRelease(create)(delete))

  private def create: UIO[Workspace] =
    for
      configuration <- ZIO.config(Configuration.config).orDie
      id             = UUID.randomUUID
      path           = configuration.path / id.toString
      _             <- ZIO.attemptBlocking(makeDir.all(path)).orDie
    yield Workspace(id, path)

  private def delete(workspace: Workspace): UIO[Unit] =
    for
      configuration <- ZIO.config(Configuration.config).orDie
      _             <- ZIO.whenDiscard(configuration.autoClean):
                         ZIO.attemptBlocking(remove.all(workspace.path)).orDie
    yield ()

  case class Configuration(path: Path, autoClean: Boolean)

  object Configuration:

    private given DeriveConfig[Path] = DeriveConfig[String].map(Path(_))

    val config: Config[Configuration] = deriveConfig[Configuration].nested("workspaces")
