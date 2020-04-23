package ua.kyiv.tinedel.csvparser.immutable

import ua.kyiv.tinedel.csvparser.tokenizer._

import scala.io.{Codec, Source}

class ImmutableTokenizer(val rootTrie: Trie[Token[Nothing]]) {
  def tokenize(source: Source): Stream[Token[String]] = {
    tokenize(source.toStream)
  }

  def appendChar(c: Char, maybeTokens: List[(Int, Trie[Token[Nothing]])]): List[(Int, Trie[Token[Nothing]])] = {
    val newMaybeToken = (1, rootTrie.children(c))
    (newMaybeToken ::
      maybeTokens.map {
        case (pos, trie) => (pos + 1, trie.children(c))
      }).filterNot(_._2.isEmpty)
  }

  def tokenize(stream: Stream[Char]): Stream[Token[String]] = {

    def tokenizeInt(s: Stream[Char],
                    maybeTokens: List[(Int, Trie[Token[Nothing]])],
                    sb: List[Char]): Stream[Token[String]] = s match {
      // true end of stream, all buffers are empty
      case Stream.Empty if sb.isEmpty && maybeTokens.isEmpty => Stream.empty
      // some leftovers in stringbuilder
      case Stream.Empty if sb.nonEmpty => Block(sb.reverse.mkString) #:: Stream.empty

      case c #:: tail =>
        val nextTries = appendChar(c, maybeTokens)
        val nextSb = c :: sb
        val tokenFound = nextTries.find(_._2.token.nonEmpty)
        tokenFound match {
          case Some((pos, Trie(_, Some(token)))) if nextSb.length == pos => token #:: tokenizeInt(tail, List(), List())
          case Some((pos, Trie(_, Some(token)))) if nextSb.length > pos =>
            Block(nextSb.drop(pos).reverse.mkString) #:: token #:: tokenizeInt(tail, List(), List())
          case None => tokenizeInt(tail, nextTries, nextSb)
        }
    }

    tokenizeInt(stream, List(), List())
  }
}

object ImmutableTokenizer {
  /**
   * Creates [[ua.kyiv.tinedel.csvparser.immutable.ImmutableTokenizer]] for given byte channel with provided strings
   * to be used for splitting input data into tokens
   *
   * @param quotingString   string to quote fields. If encountered in input data will produce
   *                        [[ua.kyiv.tinedel.csvparser.tokenizer.QuotationMark]] token
   * @param recordSeparator string to separate records. If encountered in input data will produce
   *                        [[ua.kyiv.tinedel.csvparser.tokenizer.RecordSeparator]] token
   * @param fieldSeparator  string to separate fields. If encountered in input data will produce
   *                        [[ua.kyiv.tinedel.csvparser.tokenizer.FieldSeparator]] token
   * @param escapeString    string to escape other tokens. By default is not recognized, but if provided will produce
   *                        [[ua.kyiv.tinedel.csvparser.tokenizer.Escape]] token
   * @param codec           codec to be used to making sense of data.
   * @return [[ua.kyiv.tinedel.csvparser.immutable.ImmutableTokenizer]] configured with provided parameters
   */
  def apply(quotingString: String = "\"",
            recordSeparator: String = "\n",
            fieldSeparator: String = ",",
            escapeString: Option[String] = Some("\\"))(implicit codec: Codec): ImmutableTokenizer = {

    val tokensTrie: Trie[Token[Nothing]] = Trie(
      quotingString -> QuotationMark,
      recordSeparator -> RecordSeparator,
      fieldSeparator -> FieldSeparator,
    )

    val withEscape = escapeString.map(es => tokensTrie `with` (es -> Escape)).getOrElse(tokensTrie)
    new ImmutableTokenizer(withEscape)
  }

}
