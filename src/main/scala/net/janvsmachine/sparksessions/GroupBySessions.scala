package net.janvsmachine.sparksessions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Implements sessionization using `groupBy` on an RDD.
  */
object GroupBySessions extends Sessions with Spark {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = {

    import spark.implicits._

    // Convert a sequence of clicks into sessions.
    // Assumes given clicks all belong to same group that we want to create sessions for, e.g. user.
    def sessionizeClicks(clicks: Iterable[Click]): Seq[Session] = {

      def mergeClickWithSessions(sessions: Seq[Session], click: Click): Seq[Session] =
        if (sessions.nonEmpty && click.timestamp <= sessions.head.endTime + maxSessionDuration) {
          val previousSession = sessions.head
          val updatedSession = previousSession.copy(endTime = click.timestamp, count = previousSession.count + 1)
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
