package ua.kyiv.tinedel.csvparser.lexer

import ua.kyiv.tinedel.csvparser.tokenizer._

class SimpleLexer private(tokenizer: Tokenizer[String], tokens: Map[Token[_], String])
  extends GenericCSVLexer[String](tokenizer, tokens, "", (a, b) => a + b)

object SimpleLexer {
  def apply(tokenizer: Tokenizer[String], quotingString: String = "\"",
            lineSeparator: String = "\n",
            fieldDelimiter: String = ",",
            escapeString: Option[String] = Some("\\")): SimpleLexer = {
    new SimpleLexer(tokenizer, Map(QuotationMark -> quotingString,
      FieldSeparator -> fieldDelimiter, RecordSeparator -> lineSeparator) ++ escapeString.map(es => Escape -> es).toMap)
  }
}
