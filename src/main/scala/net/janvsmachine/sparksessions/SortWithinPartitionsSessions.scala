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

    def mergeClickWithSessions(sessions: (Stream[Session], Option[Session]), click: Click): (Stream[Session], Option[Session]) = sessions match {
      case (prev, None) =>
        val newSession = Session(click.userId, click.timestamp, click.timestamp, count = 1)
        prev -> Some(newSession)
      case (prev, Some(currentSession)) if click.userId == currentSession.userId && click.timestamp - currentSession.endTime < maxSessionDuration =>
        val updatedSession = currentSession.copy(endTime = click.timestamp, count = currentSession.count + 1)
        prev -> Some(updatedSession)
      case (prev, Some(currentSession)) =>
        val newSession = Session(click.userId, click.timestamp, click.timestamp, count = 1)
        (currentSession #:: prev) -> Some(newSession)
    }

    val (sessions, lastSession) = clicks.foldLeft(Stream[Session]() -> Option.empty[Session])(mergeClickWithSessions)
    val allSessions = lastSession.map(s => Stream[Session](s) ++ sessions).getOrElse(sessions)

    allSessions.iterator
  }

}
