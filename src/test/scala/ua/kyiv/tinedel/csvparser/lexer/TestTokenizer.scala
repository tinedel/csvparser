package ua.kyiv.tinedel.csvparser.lexer

import ua.kyiv.tinedel.csvparser.tokenizer.{Token, Tokenizer}

class TestTokenizer(val iterator: Iterator[Token[String]]) extends Tokenizer[String] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): Token[String] = iterator.next()
}

object TestTokenizer {
  def apply(tokens: Token[String]*): TestTokenizer = new TestTokenizer(Iterator(tokens: _*))
}
