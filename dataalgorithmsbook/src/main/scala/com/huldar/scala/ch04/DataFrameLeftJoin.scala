package com.huldar.scala.ch04

import org.apache.spark.sql.{Row, SparkSession}

/**
  *
  * 左连接模式:
  * 使用Spark DataFrame解决
  *
  * @author huldar
  *
  */
object DataFrameLeftJoin {
  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Usage: DataFrameLeftOuterJoin <users-data-path> <transactions-data-path> <output-path>")

    val Array(userInputFile, transactionInputFile, output) = args

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(DataFrameLeftJoin.getClass.getSimpleName)
      //.config("spark.default.parallelism", 5)
      .config("spark.sql.shuffle.partitions", 3)
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.types._
    val userScheme = StructType(Seq(
      StructField("userId", StringType, nullable = false),
      StructField("location", StringType, nullable = false)
    ))

    val transactionScheme = StructType(Seq(
      StructField("transactionId", StringType, nullable = false),
      StructField("productId", StringType, nullable = false),
      StructField("userId", StringType, nullable = false),
      StructField("quantity", IntegerType, nullable = false),
      StructField("price", DoubleType, nullable = false))
    )

    def userRows(line: String): Row = {
      val tokens = line.split(",")
      Row(tokens(0), tokens(1))
    }

    def transactionRows(line: String): Row = {
      val tokens = line.split(",")
      Row(tokens(0), tokens(1), tokens(2), tokens(3).toInt, tokens(4).toDouble)
    }

    val userRaw = sc.textFile(userInputFile, 2)
    val userRDDRows = userRaw.filter(_.split(",").length == 2)
      .map(userRows)
    val users = spark.createDataFrame(userRDDRows, userScheme)

    users.show()

    val transactionRaw = sc.textFile(transactionInputFile, 2)
    val transactionRDDRows = transactionRaw.filter(_.split(",").length == 5)
      .map(transactionRows)
    val transactions = spark.createDataFrame(transactionRDDRows, transactionScheme)

    transactions.show()

    val joined = transactions.join(users, transactions("userId") === users("userId"))

    joined.show()
    joined.printSchema()

    val productAndLocation = joined.select(joined.col("productId"), joined.col("location"))
    val productAndLocationDistinct = productAndLocation.distinct()

    val products = productAndLocationDistinct.groupBy("productId").count()

    products.show()
    products.write.save(output + "/approach1")
    products.rdd.saveAsTextFile(output + "/approach1_textFormat")

    // done
    spark.stop()

  }
}
