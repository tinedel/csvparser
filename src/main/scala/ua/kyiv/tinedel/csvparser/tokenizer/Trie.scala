package ua.kyiv.tinedel.csvparser.tokenizer

/**
 * Trie data structure to keep track of possible tokens during parsing of input data.
 *
 * Contract of this particular implementation is that the trie node will contain either children or
 * [[ua.kyiv.tinedel.csvparser.tokenizer.Token]], but not both. As otherwise it will create ambiguity during parse.
 * e.g. having ":" as field separator and "::" as record separator makes impossible to make sense of input data
 * {{{
 *   field1:field2::field3
 * }}}
 * Is it empty field or line break between fields 2 and 3
 *
 * [[ua.kyiv.tinedel.csvparser.tokenizer.TrieException]] will be thrown if attempted to violate the contract
 *
 * @see [[https://en.wikipedia.org/wiki/Trie]]
 * @param children map of children char to next Trie containing children further down or Token
 * @param token    leaf node in Trie with token
 * @tparam T type of data stored in leaf nodes
 */
case class Trie[T](children: Map[Char, Trie[T]] = Map[Char, Trie[T]]().withDefault(_ => Trie[T]()), token: Option[T] = None) {

  /**
   * Creates a copy of this Trie with string `s` being added
   *
   * @param s     string to add implicitly converted to Seq[Char] to descent easier
   * @param token data to store
   * @return Trie with all the data in this Trie plus data given as parameters
   */
  def add(s: Seq[Char], token: T): Trie[T] = s match {
    case Nil => Trie(token = Some(token))
    case head +: tail =>
      val childTrie = children(head)
      if (childTrie.token.isDefined) throw new TrieException("Ambiguous separators list")
      if (tail.isEmpty && childTrie.children.nonEmpty) throw new TrieException("Ambiguous separators list")
      Trie(children = children + (head -> childTrie.add(tail, token)))
  }

  /**
   * Handy shortcut for [[ua.kyiv.tinedel.csvparser.tokenizer.Trie#add(s: Seq[Char], token: T)]]
   * {{{
   *   val trie: Trie[Int] = buildTrie()
   *   val modifiedTrie = trie `with` ("abc" -> 5)
   * }}}
   *
   * @param p pair of key values to add
   * @return Trie containing the data from this Trie plus data given as parameter
   */
  def `with`(p: (String, T)): Trie[T] = add(p._1, p._2)

  /**
   * Handy shortcut for [[ua.kyiv.tinedel.csvparser.tokenizer.Trie#add(s: Seq[Char], token: T)]]
   * {{{
   *   val trie: Trie[Int] = buildTrie()
   *   val modifiedTrie = trie + ("abc" -> 5)
   * }}}
   *
   * @param p pair of key values to add
   * @return Trie containing the data from this Trie plus data given as parameter
   */
  def +(p: (String, T)): Trie[T] = add(p._1, p._2)

  /**
   * Returns true if Trie is empty - no children and no token
   */
  def isEmpty: Boolean = children.isEmpty && token.isEmpty

  /**
   * Returns true if Trie have some data in it
   */
  def nonEmpty: Boolean = !isEmpty

  /**
   * Descents the Trie one char at a time
   *
   * @param c char to test
   * @return Either Trie or stored data if leaf is reached
   */
  def matchChar(c: Char): Either[Trie[T], T] = {
    val trie = children(c)
    trie.token.toRight(trie)
  }
}

/**
 * Factory object for [[ua.kyiv.tinedel.csvparser.tokenizer.Trie]] class
 */
object Trie {
  /**
   * Return [[ua.kyiv.tinedel.csvparser.tokenizer.Trie]] with provided data
   */
  def apply[T](toAdd: (String, T)*): Trie[T] = toAdd.foldLeft(new Trie[T]())((t, c) => c match {
    case (str, token) => t.add(str, token)
  })
}

/**
 * Exception thrown if ambiguity is introduced in Trie
 *
 * @see [[ua.kyiv.tinedel.csvparser.tokenizer.Trie]]
 */
class TrieException(message: String) extends RuntimeException(message)