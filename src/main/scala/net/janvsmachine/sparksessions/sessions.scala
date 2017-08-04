package net.janvsmachine.sparksessions

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.IntegerType


case class Click(userId: String, targetId: String, timestamp: Long)

case class Session(userId: String, startTime: Long, endTime: Long, count: Long)

/**
  * Common code for sessions implementations
  */
trait Sessions extends LazyLogging {

  self: Spark =>

  def main(args: Array[String]): Unit = {
    if (args.length < 3 || args.length > 3) {
      println(s"Usage: ${getClass.getSimpleName.dropRight(1)} <input path> <output path> [local]")
      System.exit(-1)
    }

    val inputPath = args(0)
    val outputPath = args(1)
    val localSpark = args.length == 3 && args(2) == "local"

    implicit val spark: SparkSession = createSession(localSpark)

    val clicks = loadClicks(inputPath)

    val sessionized = sessionize(clicks, maxSessionDuration = 30000).sort("userId", "startTime")

    sessionized.write.mode(SaveMode.Overwrite).csv(outputPath)
  }

  def loadClicks(path: String)(implicit spark: SparkSession): Dataset[Click] = {
    import spark.implicits._
    spark.read.option("header", "true").csv(path)
      .map { case Row(userId: String, documentId: String, timestamp: String, _, _, _) => Click(userId, documentId, timestamp.toLong) }
  }

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session]

}

object GroupBy extends Sessions with Spark {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = {

    import spark.implicits._

    def sessionizeClicks(clicks: Iterable[Click]): Seq[Session] = {
      def mergeClickWithSessions(sessions: Seq[Session], click: Click): Seq[Session] =
        if (sessions.nonEmpty && click.timestamp <= sessions.head.endTime + maxSessionDuration) {
          val lastSession = sessions.head
          val updatedSession = lastSession.copy(endTime = click.timestamp, count = lastSession.count + 1)
          updatedSession +: sessions.tail
        }
        else
          Session(click.userId, click.timestamp, click.timestamp, count = 1) +: sessions

      clicks.toSeq.sortBy(_.timestamp).foldLeft(Seq[Session]())(mergeClickWithSessions)
    }

    val sessions: RDD[Session] =
      clicks.rdd
        .groupBy(_.userId)
        .flatMapValues(sessionizeClicks)
        .values

    spark.createDataset(sessions)
  }
}

object Partitions extends Sessions with Spark {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = ???

}

object WindowsFunctions extends Sessions with Spark {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = {

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // TODO: Try writing this using the Dataset + Dataframe DSLs?
    val clicksWithSessionIds = clicks
      .selectExpr("*", "lag (timestamp, 1) over (partition by userId order by timestamp) as lastTimestamp")
      .selectExpr("*", "timestamp - lastTimestamp as timeDiff", s"case when timestamp - lastTimestamp < $maxSessionDuration then 0 else 1 end as isNewSession")
      .selectExpr("*", "sum(isNewSession) over (order by userId, timestamp) as sessionId")

    clicksWithSessionIds.explain(true)

    clicksWithSessionIds
      .groupBy("userId", "sessionId")
      .agg(min("timestamp").as("startTime"), max("timestamp").as("endTime"), count("*").as("count"))
      .as[Session]
  }

}
