package com.huldar.ch01.spark

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  *
  * TODO
  *
  * @author huldar
  *
  */
object SecondarySort {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage <number-of-partitions> <input-path> <output-path>")
      sys.exit(1)
    }
    val Array(partitions, inputPath, outputPath) = args

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val line = spark.sparkContext.textFile(inputPath)

    val valueToKey = line.map(line => {
      val lineArray = line.split(",", -1)
      ((lineArray(0) + "-" + lineArray(1), lineArray(2).toInt), lineArray(2).toInt)
      // ((2019-03,35.5),35.5)
    })

    implicit def tupleOrderingDesc: Ordering[(String, Int)] {
    } = new Ordering[(String, Int)] {
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        if (y._1.compare(x._1) == 0) y._2.compare(x._2)
        else y._1.compare(x._1)
      }
    }
    Random.nextInt()

    val sorted = valueToKey.repartitionAndSortWithinPartitions(new CustomPartitioner(partitions.toInt))
    val result = sorted.map {
      case (k, v) => (k._1, v)
    }

    result.saveAsTextFile(outputPath)

    spark.stop
  }
}
