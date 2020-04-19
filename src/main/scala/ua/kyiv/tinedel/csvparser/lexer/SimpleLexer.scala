package ua.kyiv.tinedel.csvparser.lexer

import ua.kyiv.tinedel.csvparser.tokenizer._

/**
 * Simple lexer extending [[ua.kyiv.tinedel.csvparser.lexer.GenericCSVLexer]] and operating on string tokenizers
 * providing string Lexemes
 *
 * @param tokenizer iterator providing Tokens
 * @param tokens    map of tokens to field content type used when lexer finds out it needs to add literal value of token in field
 */
class SimpleLexer private[csvparser](tokenizer: Tokenizer[String], tokens: Map[Token[_], String])
  extends GenericCSVLexer[String](tokenizer, tokens, "", (a, b) => a + b)

/**
 * Factory object producing SimpleLexers
 */
object SimpleLexer {
  /**
   * Creates [[ua.kyiv.tinedel.csvparser.lexer.SimpleLexer]] from provided parameters
   *
   * @param tokenizer       String based tokenizer
   * @param quotingString   string for quoting fields
   * @param recordSeparator string separating records
   * @param fieldSeparator  string separating fields
   * @param escapeString    optional escape string to enable escaping support which is not standard
   * @return configured [[ua.kyiv.tinedel.csvparser.lexer.SimpleLexer]]
   */
  def apply(tokenizer: Tokenizer[String], quotingString: String = "\"",
            recordSeparator: String = "\n",
            fieldSeparator: String = ",",
            escapeString: Option[String] = Some("\\")): SimpleLexer = {
    new SimpleLexer(tokenizer, Map(QuotationMark -> quotingString,
      FieldSeparator -> fieldSeparator, RecordSeparator -> recordSeparator) ++ escapeString.map(es => Escape -> es).toMap)
  }
}
