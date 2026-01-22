package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.test.TestApp
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import scala.util.Properties
import com.qwshen.common.io.FileChannel

class DeltaUsersMigrationTest extends TestApp {
  
  private val deltaOutputPath = "/tmp/events/delta/users"
  
  override protected def loadConfig(): Config = {
    val cfgString = FileChannel.loadAsString(this.resourceRoot + "application-test.conf")
    val config = ConfigFactory.parseString(cfgString)

    val cfgOverride = Seq(
      String.format("events.users_input = \"%s\"", s"${resourceRoot}data/users"),
      String.format("events.events_input = \"%s\"", s"${resourceRoot}data/events"),
      String.format("events.train_input = \"%s\"", s"${resourceRoot}data/train"),
      String.format("events.delta_output = \"%s\"", deltaOutputPath),
      String.format("application.scripts_uri = \"%s\"", s"${resourceRoot}scripts")
    ).mkString(Properties.lineSeparator)
    ConfigFactory.parseString(cfgOverride).withFallback(config)
  }
  
  private val expectedSchema = StructType(Seq(
    StructField("user_id", StringType, nullable = true),
    StructField("birthyear", IntegerType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("joined-at", StringType, nullable = true)
  ))
  
  private val expectedRowCount = 6
  
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
        
        assert(df.count() == expectedRowCount, s"Expected $expectedRowCount rows, got ${df.count()}")
        
        val requiredColumns = Seq("user_id", "birthyear", "gender", "joined-at")
        requiredColumns.foreach { col =>
          assert(df.columns.contains(col), s"Required column '$col' is missing")
        }
      } finally {
        this.done(session)
      }
    }
  }
  
  test("Pipeline test - file read / delta write users with validation (HBase migration)") {
    // First run the pipeline to create Delta output
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-deltaWrite-users.yaml")
    
    // Then validate the output
    for {
      session <- this.start()
    } {
      try {
        val df = session.read.format("delta").load(deltaOutputPath)
        
        // Schema validation
        val actualFields = df.schema.fields.filterNot(_.name == "gender")
        val expectedFields = expectedSchema.fields.filterNot(_.name == "gender")
        
        actualFields.zip(expectedFields).foreach { case (actual, expected) =>
          assert(actual.name == expected.name, s"Column name mismatch: ${actual.name} != ${expected.name}")
          assert(actual.dataType == expected.dataType, s"Column ${actual.name} type mismatch: ${actual.dataType} != ${expected.dataType}")
        }
        assert(df.schema.fieldNames.contains("gender"), "Partition column 'gender' should exist")
        
        // Row count validation
        assert(df.count() == expectedRowCount, s"Expected $expectedRowCount rows in Delta output, got ${df.count()}")
        
        // Partition validation
        val partitionCounts = df.groupBy("gender").count().collect()
        assert(partitionCounts.length == 2, "Expected 2 gender partitions (male, female)")
        
        val maleCount = partitionCounts.find(_.getString(0) == "male").map(_.getLong(1)).getOrElse(0L)
        val femaleCount = partitionCounts.find(_.getString(0) == "female").map(_.getLong(1)).getOrElse(0L)
        assert(maleCount == 3, s"Expected 3 male users, got $maleCount")
        assert(femaleCount == 3, s"Expected 3 female users, got $femaleCount")
        
        // Data quality validation
        val nullUserIds = df.filter(df("user_id").isNull).count()
        assert(nullUserIds == 0, s"Found $nullUserIds null user_id values")
        
        val nullGenders = df.filter(df("gender").isNull).count()
        assert(nullGenders == 0, s"Found $nullGenders null gender values")
        
        val validGenders = df.filter(df("gender").isin("male", "female")).count()
        assert(validGenders == expectedRowCount, s"Expected all rows to have valid gender, got $validGenders")
        
        val distinctUserIds = df.select("user_id").distinct().count()
        assert(distinctUserIds == expectedRowCount, s"Expected $expectedRowCount distinct user_ids, got $distinctUserIds")
        
        println(s"All validations passed! Delta output at $deltaOutputPath contains $expectedRowCount rows")
      } finally {
        this.done(session)
      }
    }
  }

  override def createSparkSession(): SparkSession = SparkSession.builder()
    .appName("delta-users-migration-test")
    .master("local[*]")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
}
