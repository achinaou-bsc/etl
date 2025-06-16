package dev.a4i.bsc.etl.configuration

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import zio.*
import zio.config.magnolia.deriveConfig

type DataSource = javax.sql.DataSource

object DataSource:

  val layer: ULayer[DataSource] =
    ZLayer.scoped:
      ZIO.fromAutoCloseable:
        for
          configuration: Configuration <- ZIO.config[Configuration].orDie
          dataSource: HikariDataSource  = HikariDataSource:
                                            new HikariConfig:
                                              setDriverClassName(configuration.driver)
                                              setJdbcUrl(configuration.jdbcUrl)
                                              setUsername(configuration.username)
                                              setPassword(configuration.password)
        yield dataSource

  extension (configuration: Configuration)
    def jdbcUrl: String =
      s"jdbc:postgresql://${configuration.host}:${configuration.port}/${configuration.name}?ApplicationName=achinaou-bsc-etl"

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
