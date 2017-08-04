package net.janvsmachine.sparksessions

object Sequences {

  /**
    * Given a sequences of values, return a sequence of sub-sequences, where
    * the given predicate decides whether two values that follow each other in the original
    * sequence should belong to the same subsequences, or should be split into separate subsequences.
    */
  // TODO: Make sure this doesn't build an in-memory result!
  def subSequences[T](xs: Seq[T], inSameSubsequence: (T, T) => Boolean): Seq[Seq[T]] = {
    def combine(prev: Seq[Seq[T]], next: T): Seq[Seq[T]] =
      if (prev.isEmpty) Seq(Seq(next))
      else if (inSameSubsequence(prev.head.head, next)) (next +: prev.head) +: prev.tail
      else Seq(next) +: prev

    xs.foldRight(Seq[Seq[T]]())((t: T, b: Seq[Seq[T]]) => combine(b, t))
  }

}
