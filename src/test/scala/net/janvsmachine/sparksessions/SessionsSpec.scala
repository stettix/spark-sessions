package net.janvsmachine.sparksessions

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

abstract class SessionsSpec extends FlatSpec with Matchers with Spark {

  implicit val spark: SparkSession = createSession(true)

  import spark.implicits._

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session]

  val clicks = Seq(
    Click("user1", "pageA", 1),
    Click("user2", "pageX", 2),
    Click("user1", "pageB", 9),
    Click("user1", "pageB", 10),
    Click("user2", "pageX", 50),
    Click("user2", "pageY", 100),
    Click("user1", "pageC", 55)
  )

  val expectedSessions = Seq(
    Session("user1", 1, 10, count = 3),
    Session("user1", 55, 55, count = 1),
    Session("user2", 2, 2, count = 1),
    Session("user2", 50, 50, count = 1),
    Session("user2", 100, 100, count = 1)
  )

  "Sessionizing user clicks" must "return sessions for single user" in {
    val user1Clicks = clicks.filter(_.userId == "user1")
    val expected = Seq(
      Session("user1", 1, 10, count = 3),
      Session("user1", 55, 55, count = 1)
    )
    sessionize(user1Clicks.toDS(), 10).collect() should contain theSameElementsAs expected
  }

  it must "return the expected sessions for multiple users" in {
    sessionize(clicks.toDS(), 10).collect() should contain theSameElementsAs expectedSessions
  }

  it must "return nothing for empty input" in {
    sessionize(Seq[Click]().toDS(), 10).collect() shouldBe empty
  }

  it must "return a single session for a single click" in {
    sessionize(Seq(Click("user1", "pageA", 100)).toDS(), 10).collect() should contain theSameElementsAs Seq(Session("user1", 100, 100, 1))
  }

}

class GroupBySessionsSpec() extends SessionsSpec {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] =
    GroupBySessions.sessionize(clicks, maxSessionDuration)

}

class SortWithinPartitionsSessionsSpec() extends SessionsSpec {

  import SortWithinPartitionsSessions._

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = SortWithinPartitionsSessions.sessionize(clicks, maxSessionDuration)

  "Grouping sorted clicks" should "return sessions" in {
    val sortedClicks = clicks.sortBy(c => c.userId -> c.timestamp)
    aggregateClicks(10)(sortedClicks.iterator).toSeq should contain theSameElementsAs expectedSessions
  }

  it must "iterate efficiently over very large sequences of clicks that map to a single session without blowing up" in {
    // Create a stream of clicks that will all map to a single session.
    // Make it so big that it would run very slow, or blow up, if the implementation pulled it all into memory.

    val maxTime = 10000000
    val clicks: Iterator[Click] = (1 to maxTime).view.map(t => Click("user1", "page", t)).iterator
    aggregateClicks(maxSessionDuration = 10)(clicks).toSeq should contain theSameElementsAs Seq(Session("user1", 1, maxTime, maxTime))
  }

  it must "iterate efficiently over sequences of clicks that map to a very large number of sessions without blowing up" in {
    // Create a stream of clicks that will all map to its own session.
    // Make it so big that it would run very slow, or blow up, if the implementation pulled it all into memory.

    val numClicks = 100000000

    // Note: Be careful to create the test input iterator in a way that doesn't hold on to memory!
    val clicks: Iterator[Click] = (1 to numClicks).view.map(t => Click("user1", t.toString, 10 * t)).iterator
    aggregateClicks(maxSessionDuration = 1)(clicks).size shouldBe numClicks
  }
}

class WindowFunctionSessionsSpec() extends SessionsSpec {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] =
    WindowFunctionSessions.sessionize(clicks, maxSessionDuration)

}
