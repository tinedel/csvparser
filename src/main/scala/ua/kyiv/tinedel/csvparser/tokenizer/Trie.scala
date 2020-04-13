package ua.kyiv.tinedel.csvparser.tokenizer

case class Trie[T](children: Map[Char, Trie[T]] = Map[Char, Trie[T]]().withDefault(_ => Trie[T]()), token: Option[T] = None) {
  def add(s: Seq[Char], token: T): Trie[T] = s match {
    case Nil => Trie(token = Some(token))
    case head +: tail =>
      val childTrie = children(head)
      if (childTrie.token.isDefined) throw new TrieException("Ambiguous separators list")
      if (tail.isEmpty && childTrie.children.nonEmpty) throw new TrieException("Ambiguous separators list")
      Trie(children = children + (head -> childTrie.add(tail, token)))
  }

  def `with`(p: (String, T)): Trie[T] = add(p._1, p._2)

  def isEmpty: Boolean = children.isEmpty && token.isEmpty

  def nonEmpty: Boolean = !isEmpty

  def matchChar(c: Char): Either[Trie[T], T] = {
    val trie = children(c)
    trie.token.toRight(trie)
  }
}

object Trie {
  def apply[T](toAdd: (String, T)*): Trie[T] = toAdd.foldLeft(new Trie[T]())((t, c) => c match {
    case (str, token) => t.add(str, token)
  })
}

class TrieException(message: String) extends RuntimeException(message)