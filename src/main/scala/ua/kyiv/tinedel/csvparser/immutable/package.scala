package ua.kyiv.tinedel.csvparser


/**
 * Immutable CSV parser and which relies on immutable tokenizer and lexer to perform the job
 *
 * [[ImmutableTokenizer]] - Takes input [[scala.io.Source]] and splits it into tokens
 * [[ImmutableLexer]] - Processes CSV grammar to get stream of lexems according to csv standard
 * [[ImmutableParser]] - Finalizes the processing producing [[Map]] of field number (1-based) to field content
 *
 * Instances of Lexer and Parser are capable of taking non-string Streams to implement parsers with smaller memory
 * consumption for the case of huge values in the cells.
 *
 * @see [[ua.kyiv.tinedel.csvparser.lexer.GenericCSVLexer]], [[MinimalMemoryCSVParser]]
 */
package object immutable {

  /**
   * Provides operation matchComposite to allow composing several shareable partial functions and traditional
   * match case syntax
   *
   * @param s object
   * @tparam K of any type
   */
  implicit class PostfixMatcher[K](s: K) {
    def matchComposite[T](f: K => T): T = f(s)
  }

}
