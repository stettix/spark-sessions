package net.janvsmachine.sparksessions

import net.janvsmachine.sparksessions.Sequences.subSequences
import org.scalatest.{FlatSpec, Matchers}

class SequencesSpec extends FlatSpec with Matchers {

  def always[T](value: Boolean)(x: T, y: T): Boolean = value

  "Getting subSequences" should "return nothing for an empty sequence" in {
    subSequences(Seq(), always(true)) shouldBe empty
    subSequences(Seq(), always(false)) shouldBe empty
  }

  it should "return a single subsequence for a predicate that always returns true" in {
    subSequences(1 to 100, always(true)) should contain theSameElementsInOrderAs Seq(1 to 100)
  }

  it should "split subsequences on the given predicate" in {
    val xs = Seq(1, 3, 5, 6, 7, 13, 15, 20, 60, 61)

    def isInSameDecade(x: Int, y: Int) = x / 10 == y / 10

    subSequences(xs, isInSameDecade) shouldBe Seq(Seq(1, 3, 5, 6, 7), Seq(13, 15), Seq(20), Seq(60, 61))
  }

}
