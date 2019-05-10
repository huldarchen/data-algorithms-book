package com.huldar.scala.ch04

import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * spark的RDD的左外连接
  *
  * @author huldar
  *
  */
object SparkLeftOuterJoin {
  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      println("Usage: SparkLeftOuterJoin <users> <transactions> <output>")
      sys.exit(1)
    }
    val Array(userInput, transactionInput, output) = args

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val userRaw = sc.textFile(userInput)
    val transactionRaw = sc.textFile(transactionInput)

    val userAndLocation = userRaw.map(line => {
      val tokens = line.split(",")
      (tokens(0), tokens(1))
    })

    val userAndProduct = transactionRaw.map(line => {
      val tokens = line.split(",")

      (tokens(2), tokens(1))
    })

    val joined = userAndProduct join userAndLocation



    val productLocations = joined.values.map(f => (f._1, Option(f._2).getOrElse("unknown")))
    val productByLocation = productLocations.groupByKey()
    val result = productByLocation.map(t => (t._1, t._2.toSet)).map(tuple => (tuple._1, tuple._2.size))

    result.saveAsTextFile(output)

    sc.stop()


  }

}
