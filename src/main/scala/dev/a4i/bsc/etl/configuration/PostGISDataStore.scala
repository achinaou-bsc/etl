package dev.a4i.bsc.etl.configuration

import scala.jdk.CollectionConverters.*

import org.geotools.api.data.DataStore
import org.geotools.api.data.DataStoreFinder
import zio.*
import zio.config.magnolia.deriveConfig

type PostGISDataStore = DataStore

object PostGISDataStore:

  val layer: ULayer[PostGISDataStore] =
    ZLayer.scoped:
      for
        configuration <- ZIO.config[Configuration].orDie
        parameters     = Map(
                           "dbtype"              -> "postgis",
                           "host"                -> configuration.host,
                           "port"                -> configuration.port,
                           "schema"              -> "public",
                           "database"            -> configuration.name,
                           "user"                -> configuration.username,
                           "passwd"              -> configuration.password,
                           "preparedStatements"  -> true,
                           "encode functions"    -> true,
                           "Expose primary keys" -> true
                         )
        dataStore     <-
          val acquire: UIO[DataStore] =
            ZIO
              .succeed(Option(DataStoreFinder.getDataStore(parameters.asJava)))
              .flatMap:
                case Some(dataSource) => ZIO.succeed(dataSource)
                case None             => ZIO.die(RuntimeException("Could not instantiate a DataStore for PostGIS"))

          val release: DataStore => UIO[Unit] = dataStore => ZIO.succeed(dataStore.dispose())

          ZIO.acquireRelease(acquire)(release)
      yield dataStore

  case class Configuration(
      driver: String,
      host: String,
      port: Int,
      name: String,
      username: String,
      password: String
  )

  object Configuration:

    given Config[Configuration] = deriveConfig.nested("database")
