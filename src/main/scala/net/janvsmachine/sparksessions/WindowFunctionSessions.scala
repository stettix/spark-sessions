package net.janvsmachine.sparksessions

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Implements sessionization using Spark SQL window functions.
  */
object WindowFunctionSessions extends Sessions with Spark {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = {

    import spark.implicits._

    val clicksWithSessionIds = clicks
      .select('userId, 'timestamp,
        lag('timestamp, 1)
          .over(Window.partitionBy('userId).orderBy('timestamp))
          .as('prevTimestamp))
      .select('userId, 'timestamp,
        when('timestamp.minus('prevTimestamp) < lit(maxSessionDuration), lit(0)).otherwise(lit(1))
          .as('isNewSession))
      .select('userId, 'timestamp,
        sum('isNewSession)
          .over(Window.partitionBy('userId).orderBy('userId, 'timestamp))
          .as('sessionId))

    clicksWithSessionIds
      .groupBy("userId", "sessionId")
      .agg(min("timestamp").as("startTime"), max("timestamp").as("endTime"), count("*").as("count"))
      .as[Session]
  }

}
