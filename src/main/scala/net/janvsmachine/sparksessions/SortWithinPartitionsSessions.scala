package net.janvsmachine.sparksessions

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Partition datasets by keys and sort within partitions, then use `mapPartitions` to produce sessions.
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

    def aggregateClicks(clicks: Iterator[Click]): Iterator[Session] = {
      /* TODO! Make subSequences function use iterators instead. */
      val clicksSeq = clicks.toSeq

      val sessionClicks: Seq[Seq[Click]] = Sequences.subSequences(clicksSeq, (c1: Click, c2: Click) => c1.userId == c2.userId && Math.abs(c2.timestamp - c1.timestamp) < maxSessionDuration)
      val sessions: Seq[Session] = sessionClicks.map { clicksForSession =>
        assert(clicksForSession.nonEmpty)
        val sorted = clicksForSession.sortBy(_.timestamp)
        val startTime = sorted.head.timestamp
        val endTime = sorted.last.timestamp
        Session(clicksForSession.head.userId, startTime, endTime, clicksForSession.size)
      }

      sessions.iterator
    }

    partitionedAndSortedClicks.mapPartitions(aggregateClicks)
  }

}
