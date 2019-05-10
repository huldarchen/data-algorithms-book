package com.huldar.scala.ch04

import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationStart, SparkListenerEvent}
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionStart, SparkPlanGraph}

class MySparkLisener extends SparkListener {


  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {

    val appid = applicationStart.appId

    val user = applicationStart.sparkUser

    println("****** appid :"+appid)
    println("****** user :"+user)

  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {

    event match {

      case SparkListenerSQLExecutionStart(executionId, description, details,
      physicalPlanDescription, sparkPlanInfo, time) =>

        val physicalPlanGraph = SparkPlanGraph(sparkPlanInfo)

        val sqlPlanMetrics = physicalPlanGraph.allNodes.flatMap { node =>
          node.metrics.map(metric => metric.accumulatorId -> metric)
        }


        //TEST 打印
        println("~~~~~~~~~~~~~*************~~~~~~~~~~~~~")
        println(sparkPlanInfo.metrics)
       // println(physicalPlanGraph.toString)
       // println(sqlPlanMetrics.toString())


      case _ => // Ignore

    }
  }

}
