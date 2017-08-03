package net.janvsmachine.sparksessions

import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

abstract class SessionsSpec extends FlatSpec with Matchers with Spark {

  implicit val spark = createSession(true)

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

  "Sessionizing user clicks" should "return sessions for single user" in {
    val user1Clicks = clicks.filter(_.userId == "user1")
    val expected = Seq(
      Session("user1", 1, 10, count = 3),
      Session("user1", 55, 55, count = 1)
    )
    sessionize(user1Clicks.toDS(), 10).collect() should contain theSameElementsAs expected
  }

  it should "return the expected sessions for multiple users" in {
    val expected = Seq(
      Session("user1", 1, 10, count = 3),
      Session("user1", 55, 55, count = 1),
      Session("user2", 2, 2, count = 1),
      Session("user2", 50, 50, count = 1),
      Session("user2", 100, 100, count = 1)
    )
    sessionize(clicks.toDS(), 10).collect() should contain theSameElementsAs expected
    assert(2 + 2 == 4)
  }

  it should "return nothing for empty input" in {
    sessionize(Seq[Click]().toDS(), 10).collect() shouldBe empty
  }

}

class GroupBySpec() extends SessionsSpec {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = GroupBy.sessionize(clicks, maxSessionDuration)

}

class PartitionSpec() extends SessionsSpec {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = Partitions.sessionize(clicks, maxSessionDuration)

}

class WindowFunctionsSpec() extends SessionsSpec {

  def sessionize(clicks: Dataset[Click], maxSessionDuration: Long)(implicit spark: SparkSession): Dataset[Session] = WindowsFunctions.sessionize(clicks, maxSessionDuration)

}
