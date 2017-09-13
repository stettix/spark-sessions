package net.janvsmachine.sparksessions

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.{AbstractIterator, Iterator}

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

  /**
    * Aggregate clicks into sessions.
    * Assumes the given clicks are sorted by grouping columns (e.g. user) and timestamp.
    * Takes care not to use in-memory structures for any part, i.e. it's a strict
    * iterator-to-iterator transformation.
    **/
  private[sparksessions] def aggregateClicks(maxSessionDuration: Long)(clicks: Iterator[Click]): Iterator[Session] =
    new SessionIterator(maxSessionDuration, clicks)

  class SessionIterator(maxSessionDuration: Long, rawClicks: Iterator[Click]) extends AbstractIterator[Session] with Iterator[Session] {

    // Get a buffered iterator that lets us peek at the next value without consuming it.
    val clicks: BufferedIterator[Click] = rawClicks.buffered

    var nextSession: Session = _

    override def hasNext: Boolean = {
      if (nextSession == null)
        nextSession = updateAndGetNextSession()

      nextSession != null
    }

    override def next(): Session = {
      val result = {
        if (nextSession == null) updateAndGetNextSession()
        else try nextSession finally nextSession = null
      }
      if (result == null) Iterator.empty.next()
      else result
    }

    /** @return next Session if one exists, or null if we're at the end of the iteration. */
    private def updateAndGetNextSession(): Session =
      if (!clicks.hasNext) {
        null
      } else {
        val first = clicks.next()
        var last = first
        var count = 1

        while (clicks.hasNext && inSameSession(maxSessionDuration)(last, clicks.head)) {
          last = clicks.next()
          count = count + 1
        }

        Session(first.userId, first.timestamp, last.timestamp, count)
      }

    private def inSameSession(maxSessionDuration: Long)(c1: Click, c2: Click): Boolean =
      c1.userId == c2.userId && Math.abs(c1.timestamp - c2.timestamp) < maxSessionDuration

  }

}
