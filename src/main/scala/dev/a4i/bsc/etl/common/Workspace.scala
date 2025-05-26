package dev.a4i.bsc.etl.common

import java.util.UUID

import os.Path
import zio.UIO
import zio.ULayer
import zio.ZIO
import zio.ZLayer

import dev.a4i.bsc.etl.Configuration

case class Workspace private (id: UUID, path: Path)

object Workspace:

  def layer: ULayer[Workspace] =
    ZLayer.scoped(ZIO.acquireRelease(create)(delete))

  private def create: UIO[Workspace] =
    for
      configuration <- ZIO.config[Configuration].orDie
      id             = UUID.randomUUID
      path           = configuration.workspaces / id.toString
      _             <- ZIO.attemptBlockingIO(os.makeDir.all(path)).orDie
    yield Workspace(id, path)

  private def delete(workspace: Workspace): UIO[Unit] =
    ZIO.attemptBlockingIO(os.remove.all(workspace.path)).orDie
