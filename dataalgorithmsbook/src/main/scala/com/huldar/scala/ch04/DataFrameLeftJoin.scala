package com.huldar.scala.ch04

import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.reflect.internal.util.TableDef.Column


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
    //参数
    require(args.length == 3, "Usage: DataFrameLeftOuterJoin <users-data-path> <transactions-data-path> <output-path>")

    val Array(userInputFile, transactionInputFile, output) = args

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(DataFrameLeftJoin.getClass.getSimpleName)
      //.config("spark.default.parallelism", 5)
      .config("spark.sql.shuffle.partitions", 3)
      .config("spark.extraListeners", "com.huldar.scala.ch04.MySparkLisener")
      .getOrCreate()
    val sc = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.types._
    val userScheme = StructType(Seq(
      StructField("user_id", StringType, nullable = false),
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
    users.createOrReplaceTempView("users")
    val transactionRaw = sc.textFile(transactionInputFile, 2)
    val transactionRDDRows = transactionRaw.filter(_.split(",").length == 5)
      .map(transactionRows)
    val transactions = spark.createDataFrame(transactionRDDRows, transactionScheme)
    transactions
      .createOrReplaceTempView("transactions")


    val value =
      """
        |(
        |CASE
        |WHEN user_id IS NOT NULL THEN user_id
        |WHEN user_id IS NULL THEN userId
        |ELSE userId END
        |)AS user
      """.stripMargin
      //"(CASE WHEN user_id IS NOT NULL THEN user_id ELSE userId END)AS user"
    users.join(transactions, users("user_id") === transactions("userId"), "full")
      .selectExpr("*", value).show()
    //      .selectExpr("*","if (user_id IS NOT NULL,user_id,userId )AS user").show()
    //.selectExpr(s"if ${col("user_id")} != null,${col("user_id")},${col("user_id")}").show()
    //.selectExpr("if user_id != null,user_id,user_id").show()
    //.select(col("userId"), col("location"), col("transactionId"))

    //.select(users("userId"),users("location"),transactions("transactionId")).show()

    spark.sql("select * from users u full join transactions t on u.user_id=t.userId").groupBy("user_id")
      .agg("price" -> "min").withColumnRenamed("min(price)", "price").selectExpr(
      "*", "price"
    ).show()
    // transactions.show()
    //    val value = users.joinWith(transactions, transactions("userId") === users("userId"), "full_outer")
    //val value = users.join(transactions, users("userId") === transactions("userId"), "full_outer")


    // users.rdd.union(transactions.rdd).foreach(println)


    //    val joined = transactions.join(users, transactions("userId") === users("userId"))
    //
    //joined.show()
    //    joined.printSchema()

    //    val productAndLocation = joined.select(joined.col("productId"), joined.col("location"))
    //    val productAndLocationDistinct = productAndLocation.distinct()
    //
    //    val products = productAndLocationDistinct.groupBy("productId").count()
    //
    //    products.show()
    //    products.write.save(output + "/approach1")
    //    products.rdd.saveAsTextFile(output + "/approach1_textFormat")

    // done
    spark.stop()

  }
}
