package ua.kyiv.tinedel.csvparser.tokenizer

case class Trie(children: Map[Char, Trie] = Map.empty.withDefault(_ => Trie()), token: Option[Token[Nothing]] = None) {
  def add(s: Seq[Char], token: Token[Nothing]): Trie = s match {
    case Nil => Trie(token = Some(token))
    case head +: tail => Trie(children = children + (head -> children(head).add(tail, token)))
  }

  def `with`(p: (String, Token[Nothing])): Trie = add(p._1, p._2)

  def isEmpty: Boolean = children.isEmpty && token.isEmpty

  def nonEmpty: Boolean = !isEmpty

  def matchChar(c: Char): Either[Trie, Token[Nothing]] = {
    val trie = children(c)
    trie.token.toRight(trie)
  }
}

object Trie {
  def apply(toAdd: (String, Token[Nothing])*): Trie = toAdd.foldLeft(new Trie())((t, c) => c match {
    case (str, token) => t.add(str, token)
  })
}
