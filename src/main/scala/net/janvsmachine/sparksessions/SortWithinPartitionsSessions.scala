package net.janvsmachine.sparksessions

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Partition datasets by keys and sort within partitions, then use [[Dataset.mapPartitions()]] to produce sessions.
  *
  * See "High Performance Spark" by Karau & Warren, and "Advanced Analytics with Spark" by Ryz, Laserson, Owen & Wills
  * for details.
  */
object SortWithinPartitionsSessions extends Sessions with Spark {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = {

    import spark.implicits._

    val partitionedAndSortedClicks: Dataset[Click] = clicks
      .repartition('userId)
      .sortWithinPartitions('userId, 'timestamp)

    partitionedAndSortedClicks.mapPartitions(aggregateClicks(maxSessionDuration))
  }

  // Aggregate clicks into sessions.
  // Assumes the given clicks are sorted by grouping columns (e.g. user) and timestamp.
  // Takes care not to use in-memory structures for any part.
  private[sparksessions] def aggregateClicks(maxSessionDuration: Long)(clicks: Iterator[Click]): Iterator[Session] = {

    def mergeClickWithSessions(sessions: (Iterator[Session], Option[Session]), click: Click): (Iterator[Session], Option[Session]) = sessions match {
      case (prev, None) =>
        val newSession = Session(click.userId, click.timestamp, click.timestamp, count = 1)
        prev -> Some(newSession)
      case (prev, Some(session)) if click.userId == session.userId && click.timestamp - session.endTime < maxSessionDuration =>
        val updatedSession = session.copy(endTime = click.timestamp, count = session.count + 1)
        prev -> Some(updatedSession)
      case (prev, Some(session)) =>
        val newSession = Session(click.userId, click.timestamp, click.timestamp, count = 1)
        (Iterator[Session](session) ++ prev) -> Some(newSession)
    }

    val (sessions, lastSession) = clicks.foldLeft(Iterator[Session]() -> Option.empty[Session])(mergeClickWithSessions)
    val allSessions = lastSession.map(s => Iterator[Session](s) ++ sessions).getOrElse(sessions)

    allSessions
  }

}
