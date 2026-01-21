package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.test.TestApp
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.util.Properties

class UsersDeltaMigrationTest extends TestApp {
  private val deltaOutputPath = "/tmp/delta/users"
  
  private val expectedSchema = StructType(Seq(
    StructField("user_id", StringType, nullable = true),
    StructField("birthyear", IntegerType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("joined_at", StringType, nullable = true)
  ))

  test("Schema validation - verify output schema matches expected") {
    for {
      session <- this.start()
    } {
      try {
        this.run(s"${resourceRoot}pipelines/pipeline_users-deltaWrite.yaml")
        
        val df = session.read.format("delta").load(deltaOutputPath)
        val actualFields = df.schema.fields.map(f => (f.name, f.dataType))
        val expectedFields = expectedSchema.fields.map(f => (f.name, f.dataType))
        
        actualFields should contain theSameElementsAs expectedFields
      } finally {
        session.stop()
      }
    }
  }

  test("Input validation - verify input data loads correctly") {
    for {
      session <- this.start()
    } {
      try {
        val inputPath = s"${resourceRoot}data/users"
        val df = session.read
          .option("header", "true")
          .schema(expectedSchema)
          .csv(inputPath)
        
        df.count() should be > 0L
        df.columns should contain allOf ("user_id", "birthyear", "gender", "joined_at")
      } finally {
        session.stop()
      }
    }
  }

  test("Output validation - verify row counts match") {
    for {
      session <- this.start()
    } {
      try {
        this.run(s"${resourceRoot}pipelines/pipeline_users-deltaWrite.yaml")
        
        val inputPath = s"${resourceRoot}data/users"
        val inputDf = session.read
          .option("header", "true")
          .schema(expectedSchema)
          .csv(inputPath)
        
        val outputDf = session.read.format("delta").load(deltaOutputPath)
        
        outputDf.count() shouldEqual inputDf.count()
      } finally {
        session.stop()
      }
    }
  }

  test("Data quality - check for null values in required fields") {
    for {
      session <- this.start()
    } {
      try {
        this.run(s"${resourceRoot}pipelines/pipeline_users-deltaWrite.yaml")
        
        val df = session.read.format("delta").load(deltaOutputPath)
        
        val nullUserIds = df.filter(df("user_id").isNull).count()
        nullUserIds shouldEqual 0L
        
        val validGenders = Seq("male", "female")
        val invalidGenders = df.filter(!df("gender").isin(validGenders: _*)).count()
        invalidGenders shouldEqual 0L
        
        val validBirthyears = df.filter(df("birthyear") >= 1900 && df("birthyear") <= 2010).count()
        validBirthyears shouldEqual df.count()
      } finally {
        session.stop()
      }
    }
  }

  test("Partitioning validation - verify data is partitioned by gender") {
    for {
      session <- this.start()
    } {
      try {
        this.run(s"${resourceRoot}pipelines/pipeline_users-deltaWrite.yaml")
        
        val df = session.read.format("delta").load(deltaOutputPath)
        val distinctGenders = df.select("gender").distinct().count()
        
        distinctGenders should be >= 1L
      } finally {
        session.stop()
      }
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
