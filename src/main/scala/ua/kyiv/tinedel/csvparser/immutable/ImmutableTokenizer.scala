package ua.kyiv.tinedel.csvparser.immutable

import ua.kyiv.tinedel.csvparser.tokenizer._

import scala.io.{Codec, Source}

/**
 * Consumes data from character stream and composes it into tokens suitable for lexing CSV
 *
 * @param rootTrie data about token strings in form convenient for finding tokens when characters coming one by one
 */
class ImmutableTokenizer(val rootTrie: Trie[Token[Nothing]]) {
  /**
   * Gets character stream from [[scala.io.Source]] and produces Tokens stream
   *
   * @param source source of data
   * @return stream of Tokens
   */
  final def tokenize(source: Source): Stream[Token[String]] = {
    tokenize(source.toStream)
  }

  private def appendChar(c: Char, maybeTokens: List[(Int, Trie[Token[Nothing]])]): List[(Int, Trie[Token[Nothing]])] = {
    val newMaybeToken = (1, rootTrie.children(c))
    (newMaybeToken ::
      maybeTokens.map {
        case (pos, trie) => (pos + 1, trie.children(c))
        // if some tries are exhausted - no need to keep them
      }).filterNot(_._2.isEmpty)
  }

  /**
   * Produces stream of tokens from stream of characters
   *
   * @param stream of characters to tokenize
   * @return stream of tokens
   */
  final def tokenize(stream: Stream[Char]): Stream[Token[String]] = {

    // non tailrec, but it's fine because it is a stream
    def tokenizeInt(s: Stream[Char],
                    maybeTokens: List[(Int, Trie[Token[Nothing]])],
                    sb: List[Char]): Stream[Token[String]] = s match {
      // no tokens, no buffers, no new characters, we are done
      case Stream.Empty if sb.isEmpty && maybeTokens.isEmpty => Stream.empty
      // no tokens, but some leftovers in buffer - need to build final block
      case Stream.Empty if sb.nonEmpty => Block(sb.reverse.mkString) #:: Stream.empty
      // next character
      case c #:: tail =>
        // maybe start of the token
        val nextTries = appendChar(c, maybeTokens)
        // or just part of the block
        val nextSb = c :: sb
        // any tokens?
        val tokenFound = nextTries.find(_._2.token.nonEmpty)
        // it started pos characters ago, so it can be
        tokenFound match {
          // just token, than don't create extra empty blocks, just produce token
          case Some((pos, Trie(_, Some(token)))) if nextSb.length == pos => token #:: tokenizeInt(tail, List(), List())
          // some useful things before, keep them in block, and produce token
          case Some((pos, Trie(_, Some(token)))) if nextSb.length > pos =>
            Block(nextSb.drop(pos).reverse.mkString) #:: token #:: tokenizeInt(tail, List(), List())
          // no tokens - continue collecting characters
          case None => tokenizeInt(tail, nextTries, nextSb)
        }
    }

    tokenizeInt(stream, List(), List())
  }
}

/**
 * Factory object producing ImmutableTokenizers
 */
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
