package com.qwshen.etl.validation

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * Validation test suite for pipeline_fileRead-fileWrite.xml migration from on-prem to AWS.
 * 
 * This test validates:
 * 1. Schema validation - output schema matches expected columns and types
 * 2. Input validation - each input source loads correctly with expected row counts
 * 3. Output validation - output row counts and partitioning are correct
 * 4. Data quality - null values, valid ranges, distinct counts
 * 5. Business rules - join results match expectations
 */
class MigrationValidationTest extends FunSuite with BeforeAndAfterAll {
  
  var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("MigrationValidationTest")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .getOrCreate()
  }
  
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
  
  val expectedOutputSchema: StructType = StructType(Seq(
    StructField("user_id", LongType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("birthyear", IntegerType, nullable = true),
    StructField("timestamp", StringType, nullable = true),
    StructField("interested", IntegerType, nullable = true),
    StructField("process_date", StringType, nullable = true),
    StructField("event_id", LongType, nullable = true)
  ))
  
  def validateSchema(outputDf: DataFrame): Boolean = {
    val actualColumns = outputDf.schema.fields.map(f => (f.name, f.dataType.typeName)).toSet
    val expectedColumns = expectedOutputSchema.fields.map(f => (f.name, f.dataType.typeName)).toSet
    
    val missingColumns = expectedColumns.diff(actualColumns)
    val extraColumns = actualColumns.diff(expectedColumns)
    
    if (missingColumns.nonEmpty) {
      println(s"Missing columns: ${missingColumns.mkString(", ")}")
    }
    if (extraColumns.nonEmpty) {
      println(s"Extra columns: ${extraColumns.mkString(", ")}")
    }
    
    missingColumns.isEmpty
  }
  
  def validateInputs(usersPath: String, trainPath: String): Map[String, (Long, Boolean)] = {
    val results = scala.collection.mutable.Map[String, (Long, Boolean)]()
    
    try {
      val usersDf = spark.read
        .option("header", "true")
        .option("delimiter", ",")
        .csv(usersPath)
      
      val usersCount = usersDf.count()
      val hasRequiredColumns = Seq("user_id", "birthyear", "gender", "joined-at")
        .forall(c => usersDf.columns.contains(c))
      
      results("users") = (usersCount, hasRequiredColumns && usersCount == 6)
    } catch {
      case e: Exception =>
        println(s"Error loading users: ${e.getMessage}")
        results("users") = (0L, false)
    }
    
    try {
      val trainCsvDf = spark.read
        .option("header", "false")
        .option("delimiter", ",")
        .csv(trainPath + "/train.csv")
        .filter(!col("_c0").startsWith("HDR") && !col("_c0").startsWith("TRL"))
      
      val trainCount = trainCsvDf.count()
      results("train_csv") = (trainCount, trainCount == 28)
    } catch {
      case e: Exception =>
        println(s"Error loading train_csv: ${e.getMessage}")
        results("train_csv") = (0L, false)
    }
    
    results.toMap
  }
  
  def validateOutput(outputPath: String): Map[String, (Long, Boolean)] = {
    val results = scala.collection.mutable.Map[String, (Long, Boolean)]()
    
    try {
      val outputDf = spark.read
        .option("header", "true")
        .csv(outputPath)
      
      val totalCount = outputDf.count()
      
      val partitionCounts = outputDf.groupBy("gender", "interested").count().collect()
      
      partitionCounts.foreach { row =>
        val gender = Option(row.getAs[String]("gender")).getOrElse("__HIVE_DEFAULT_PARTITION__")
        val interested = row.getAs[Any]("interested").toString
        val count = row.getAs[Long]("count")
        val partitionKey = s"$gender/$interested"
        results(partitionKey) = (count, count > 0)
      }
      
      results("TOTAL") = (totalCount, totalCount > 0)
    } catch {
      case e: Exception =>
        println(s"Error validating output: ${e.getMessage}")
        results("TOTAL") = (0L, false)
    }
    
    results.toMap
  }
  
  def validateDataQuality(outputDf: DataFrame): Map[String, Boolean] = {
    val results = scala.collection.mutable.Map[String, Boolean]()
    
    val nullUserIds = outputDf.filter(col("user_id").isNull).count()
    results("no_null_user_ids") = nullUserIds == 0
    
    val validGenders = Set("male", "female")
    val invalidGenders = outputDf.filter(!col("gender").isin(validGenders.toSeq: _*) && col("gender").isNotNull).count()
    results("valid_genders") = invalidGenders == 0
    
    val invalidInterested = outputDf.filter(!col("interested").isin(0, 1)).count()
    results("valid_interested") = invalidInterested == 0
    
    val invalidBirthyear = outputDf.filter(
      col("birthyear").isNotNull && (col("birthyear") < 1900 || col("birthyear") > 2010)
    ).count()
    results("valid_birthyear") = invalidBirthyear == 0
    
    val distinctUsers = outputDf.select("user_id").distinct().count()
    results("has_multiple_users") = distinctUsers > 1
    
    results.toMap
  }
  
  def validateBusinessRules(outputDf: DataFrame, usersCount: Long, trainCount: Long): Map[String, Boolean] = {
    val results = scala.collection.mutable.Map[String, Boolean]()
    
    val outputCount = outputDf.count()
    results("output_count_reasonable") = outputCount <= trainCount
    
    val distinctOutputUsers = outputDf.select("user_id").distinct().count()
    results("users_from_source") = distinctOutputUsers <= usersCount
    
    val matchedUsers = outputDf.filter(col("gender").isNotNull).count()
    results("join_produced_matches") = matchedUsers > 0
    
    results.toMap
  }
  
  def compareOutputs(onpremPath: String, cloudPath: String): Map[String, (Long, Long, String)] = {
    val results = scala.collection.mutable.Map[String, (Long, Long, String)]()
    
    try {
      val onpremDf = spark.read.option("header", "true").csv(onpremPath)
      val cloudDf = spark.read.option("header", "true").csv(cloudPath)
      
      val onpremTotal = onpremDf.count()
      val cloudTotal = cloudDf.count()
      val totalStatus = if (onpremTotal == cloudTotal) "MATCH" else "MISMATCH"
      results("TOTAL") = (onpremTotal, cloudTotal, totalStatus)
      
      val onpremPartitions = onpremDf.groupBy("gender", "interested").count()
        .collect().map(r => (s"${r.getAs[String]("gender")}/${r.getAs[Any]("interested")}", r.getAs[Long]("count"))).toMap
      
      val cloudPartitions = cloudDf.groupBy("gender", "interested").count()
        .collect().map(r => (s"${r.getAs[String]("gender")}/${r.getAs[Any]("interested")}", r.getAs[Long]("count"))).toMap
      
      val allPartitions = onpremPartitions.keySet ++ cloudPartitions.keySet
      
      allPartitions.foreach { partition =>
        val onpremCount = onpremPartitions.getOrElse(partition, 0L)
        val cloudCount = cloudPartitions.getOrElse(partition, 0L)
        val status = if (onpremCount == cloudCount) "MATCH" else "MISMATCH"
        results(partition) = (onpremCount, cloudCount, status)
      }
      
      val onpremData = onpremDf.drop("process_date").orderBy("user_id", "event_id", "timestamp")
      val cloudData = cloudDf.drop("process_date").orderBy("user_id", "event_id", "timestamp")
      
      val onpremRows = onpremData.collect().map(_.mkString(",")).sorted
      val cloudRows = cloudData.collect().map(_.mkString(",")).sorted
      
      val dataMatch = onpremRows.sameElements(cloudRows)
      results("DATA_VALUES") = (if (dataMatch) 1L else 0L, if (dataMatch) 1L else 0L, if (dataMatch) "MATCH" else "MISMATCH")
      
    } catch {
      case e: Exception =>
        println(s"Error comparing outputs: ${e.getMessage}")
        results("ERROR") = (0L, 0L, e.getMessage)
    }
    
    results.toMap
  }
}

object MigrationValidationTest {
  def main(args: Array[String]): Unit = {
    val test = new MigrationValidationTest()
    test.beforeAll()
    
    try {
      if (args.length < 2) {
        println("Usage: MigrationValidationTest <onprem_output_path> <cloud_output_path>")
        println("       MigrationValidationTest --validate-inputs <users_path> <train_path>")
        println("       MigrationValidationTest --validate-output <output_path>")
        System.exit(1)
      }
      
      args(0) match {
        case "--validate-inputs" =>
          val results = test.validateInputs(args(1), args(2))
          println("\n=== Input Validation Results ===")
          results.foreach { case (name, (count, valid)) =>
            println(s"$name: count=$count, valid=$valid")
          }
          
        case "--validate-output" =>
          val results = test.validateOutput(args(1))
          println("\n=== Output Validation Results ===")
          results.foreach { case (partition, (count, valid)) =>
            println(s"$partition: count=$count, valid=$valid")
          }
          
        case "--compare" =>
          val results = test.compareOutputs(args(1), args(2))
          println("\n=== Comparison Results ===")
          println("| Partition | On-Prem Rows | Cloud Rows | Status |")
          println("|-----------|--------------|------------|--------|")
          results.toSeq.sortBy(_._1).foreach { case (partition, (onprem, cloud, status)) =>
            println(f"| $partition%-15s | $onprem%12d | $cloud%10d | $status%-6s |")
          }
          
        case _ =>
          val results = test.compareOutputs(args(0), args(1))
          println("\n=== Comparison Results ===")
          results.foreach { case (partition, (onprem, cloud, status)) =>
            println(s"$partition: onprem=$onprem, cloud=$cloud, status=$status")
          }
      }
    } finally {
      test.afterAll()
    }
  }
}
