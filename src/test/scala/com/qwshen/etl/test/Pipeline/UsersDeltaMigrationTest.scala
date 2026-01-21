package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.test.TestApp
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

import scala.util.Properties

class UsersDeltaMigrationTest extends TestApp {
  private val deltaOutputPath = "/tmp/delta/users_migration"

  test("Delta Lake migration - pipeline execution") {
    // Clean up any previous test data
    deleteRecursively(new java.io.File(deltaOutputPath))
    
    // Run the pipeline - validates the pipeline executes successfully with Delta Lake 3.2.0
    this.run(s"${resourceRoot}pipelines/pipeline_users-deltaWrite.yaml")
  }

  private def deleteRecursively(file: java.io.File): Unit = {
    if (file.exists()) {
      if (file.isDirectory) {
        Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
      }
      file.delete()
    }
  }

  override protected def loadConfig(): Config = {
    val cfgString = loadContent(this.resourceRoot + "application-onprem.conf")
    val config = ConfigFactory.parseString(cfgString)

    val cfgOverride = Seq(
      String.format("events.users_input = \"%s\"", s"${resourceRoot}data/users"),
      String.format("events.delta_output = \"%s\"", deltaOutputPath),
      String.format("events.staging_uri = \"file:///tmp/staging/events\""),
      String.format("application.scripts_uri = \"%s\"", s"${resourceRoot}scripts")
    ).mkString(Properties.lineSeparator)
    ConfigFactory.parseString(cfgOverride).withFallback(config)
  }

  override def createSparkSession(): SparkSession = SparkSession.builder()
    .appName("users-delta-migration-test")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
}
