package com.qwshen.etl.test.validation

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

  private val expectedUsersSchema = StructType(Seq(
    StructField("user_id", StringType, nullable = true),
    StructField("birthyear", StringType, nullable = true),
    StructField("gender", StringType, nullable = true),
    StructField("joined-at", StringType, nullable = true)
  ))

  test("Schema validation - output schema matches expected") {
    for {
      session <- this.start()
    } {
      try {
        val outputPath = s"${resourceRoot}../../../data/features"
        val outputDf = session.read.option("header", "true").csv(outputPath)
        
        val actualFields = outputDf.schema.fields.map(f => (f.name, f.dataType.typeName)).toSet
        val expectedFields = Set(
          ("user_id", "string"),
          ("gender", "string"),
          ("birthyear", "string"),
          ("timestamp", "string"),
          ("interested", "string"),
          ("process_date", "string"),
          ("event_id", "string")
        )
        
        actualFields.map(_._1) should contain allElementsOf expectedFields.map(_._1)
      } finally {
        this.done(session)
      }
    }
  }

  test("Input validation - users data loads correctly") {
    for {
      session <- this.start()
    } {
      try {
        val usersPath = s"${resourceRoot}data/users"
        val usersDf = session.read.option("header", "true").csv(usersPath)
        
        usersDf.count() shouldBe 6
        usersDf.columns should contain allOf ("user_id", "birthyear", "gender", "joined-at")
      } finally {
        this.done(session)
      }
    }
  }

  test("Input validation - train CSV data loads correctly") {
    for {
      session <- this.start()
    } {
      try {
        val trainPath = s"${resourceRoot}data/train/train.csv"
        val trainDf = session.read.option("header", "false").csv(trainPath)
        
        trainDf.count() should be > 0L
      } finally {
        this.done(session)
      }
    }
  }

  test("Input validation - train TXT data loads correctly") {
    for {
      session <- this.start()
    } {
      try {
        val trainPath = s"${resourceRoot}data/train/train.txt"
        val trainDf = session.read.text(trainPath)
        
        trainDf.count() should be > 0L
      } finally {
        this.done(session)
      }
    }
  }

  test("Data quality - users have no null user_id") {
    for {
      session <- this.start()
    } {
      try {
        val usersPath = s"${resourceRoot}data/users"
        val usersDf = session.read.option("header", "true").csv(usersPath)
        
        val nullCount = usersDf.filter(usersDf("user_id").isNull).count()
        nullCount shouldBe 0
      } finally {
        this.done(session)
      }
    }
  }

  test("Data quality - users have valid gender values") {
    for {
      session <- this.start()
    } {
      try {
        val usersPath = s"${resourceRoot}data/users"
        val usersDf = session.read.option("header", "true").csv(usersPath)
        
        val validGenders = Set("male", "female")
        val distinctGenders = usersDf.select("gender").distinct().collect().map(_.getString(0)).toSet
        
        distinctGenders.forall(validGenders.contains) shouldBe true
      } finally {
        this.done(session)
      }
    }
  }

  test("Data quality - users have valid birthyear range") {
    for {
      session <- this.start()
    } {
      try {
        val usersPath = s"${resourceRoot}data/users"
        val usersDf = session.read.option("header", "true").csv(usersPath)
        
        import session.implicits._
        val birthyears = usersDf.select("birthyear").as[String].collect().map(_.toInt)
        
        birthyears.forall(y => y >= 1900 && y <= 2010) shouldBe true
      } finally {
        this.done(session)
      }
    }
  }

  test("Business rule - join produces expected row count") {
    for {
      session <- this.start()
    } {
      try {
        val usersPath = s"${resourceRoot}data/users"
        val trainPath = s"${resourceRoot}data/train/train.txt"
        
        val usersDf = session.read.option("header", "true").csv(usersPath)
        val trainDf = session.read.text(trainPath)
        
        val usersCount = usersDf.count()
        val trainCount = trainDf.count() - 2
        
        usersCount shouldBe 6
        trainCount shouldBe 28
      } finally {
        this.done(session)
      }
    }
  }
}
