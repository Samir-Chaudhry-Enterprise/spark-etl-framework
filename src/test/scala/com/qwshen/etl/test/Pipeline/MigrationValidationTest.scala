package com.qwshen.etl.test.Pipeline

import com.qwshen.etl.test.TestApp
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class MigrationValidationTest extends TestApp {

  private val expectedOutputSchema = StructType(Seq(
    StructField("user_id", LongType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("birthyear", IntegerType, nullable = true),
    StructField("timestamp", StringType, nullable = true),
    StructField("interested", IntegerType, nullable = true),
    StructField("process_date", StringType, nullable = true),
    StructField("event_id", LongType, nullable = true)
  ))

  private val expectedUsersRowCount = 6
  private val expectedTrainRowCount = 28

  test("Schema validation - verify output schema matches expected structure") {
    for {
      session <- this.start()
    } {
      try {
        val outputPath = s"${resourceRoot}../../../target/test-output/features"
        val outputDf = session.read.option("header", "true").csv(outputPath)

        val actualColumns = outputDf.columns.toSet
        val expectedColumns = expectedOutputSchema.fieldNames.toSet

        assert(actualColumns == expectedColumns,
          s"Schema mismatch. Expected: ${expectedColumns.mkString(", ")}, Actual: ${actualColumns.mkString(", ")}")

        println(s"Schema validation PASSED: ${actualColumns.size} columns match expected schema")
      } finally {
        this.done(session)
      }
    }
  }

  test("Input validation - verify users data loads correctly") {
    for {
      session <- this.start()
    } {
      try {
        val usersPath = s"${resourceRoot}data/users"
        val usersDf = session.read.option("header", "true").csv(usersPath)

        val actualRowCount = usersDf.count()
        assert(actualRowCount == expectedUsersRowCount,
          s"Users row count mismatch. Expected: $expectedUsersRowCount, Actual: $actualRowCount")

        val requiredColumns = Seq("user_id", "birthyear", "gender", "joined-at")
        val actualColumns = usersDf.columns.toSet
        requiredColumns.foreach { col =>
          assert(actualColumns.contains(col), s"Missing required column: $col")
        }

        println(s"Input validation PASSED: Users has $actualRowCount rows with all required columns")
      } finally {
        this.done(session)
      }
    }
  }

  test("Input validation - verify train data loads correctly") {
    for {
      session <- this.start()
    } {
      try {
        val trainPath = s"${resourceRoot}data/train/train.csv"
        val trainDf = session.read.option("header", "false").csv(trainPath)

        val actualRowCount = trainDf.count() - 2
        assert(actualRowCount == expectedTrainRowCount,
          s"Train row count mismatch. Expected: $expectedTrainRowCount, Actual: $actualRowCount")

        println(s"Input validation PASSED: Train has $actualRowCount data rows")
      } finally {
        this.done(session)
      }
    }
  }

  test("Data quality - check for null values in required fields") {
    for {
      session <- this.start()
    } {
      try {
        val usersPath = s"${resourceRoot}data/users"
        val usersDf = session.read.option("header", "true").csv(usersPath)

        val nullUserIdCount = usersDf.filter(usersDf("user_id").isNull).count()
        val nullGenderCount = usersDf.filter(usersDf("gender").isNull).count()

        assert(nullUserIdCount == 0, s"Found $nullUserIdCount null user_id values")
        assert(nullGenderCount == 0, s"Found $nullGenderCount null gender values")

        println(s"Data quality PASSED: No null values in required fields")
      } finally {
        this.done(session)
      }
    }
  }

  test("Pipeline execution - run fileRead-fileWrite pipeline") {
    this.run(s"${resourceRoot}pipelines/pipeline_fileRead-fileWrite.xml")
  }

  def compareOutputs(onPremPath: String, cloudPath: String)(implicit session: SparkSession): Map[String, (Long, Long, String)] = {
    val onPremDf = session.read.option("header", "true").csv(onPremPath)
    val cloudDf = session.read.option("header", "true").csv(cloudPath)

    val onPremPartitions = onPremDf.select("gender", "interested").distinct().collect()
    val results = scala.collection.mutable.Map[String, (Long, Long, String)]()

    onPremPartitions.foreach { row =>
      val gender = row.getString(0)
      val interested = row.getInt(1)
      val partitionKey = s"gender=$gender/interested=$interested"

      val onPremCount = onPremDf.filter(s"gender = '$gender' AND interested = $interested").count()
      val cloudCount = cloudDf.filter(s"gender = '$gender' AND interested = $interested").count()

      val status = if (onPremCount == cloudCount) "MATCH" else "MISMATCH"
      results(partitionKey) = (onPremCount, cloudCount, status)
    }

    results.toMap
  }

  def printComparisonTable(results: Map[String, (Long, Long, String)]): Unit = {
    println("\n| Partition | On-Prem Rows | Cloud Rows | Status |")
    println("|-----------|--------------|------------|--------|")

    var totalOnPrem = 0L
    var totalCloud = 0L
    var allMatch = true

    results.toSeq.sortBy(_._1).foreach { case (partition, (onPrem, cloud, status)) =>
      println(f"| $partition%-30s | $onPrem%12d | $cloud%10d | $status%-6s |")
      totalOnPrem += onPrem
      totalCloud += cloud
      if (status != "MATCH") allMatch = false
    }

    val finalStatus = if (allMatch && totalOnPrem == totalCloud) "PASS" else "FAIL"
    println(f"| **TOTAL** | **$totalOnPrem%d** | **$totalCloud%d** | **$finalStatus** |")
  }
}
