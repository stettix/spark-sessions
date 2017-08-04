package net.janvsmachine.sparksessions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object GroupBySessions extends Sessions with Spark {

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
