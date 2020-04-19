package ua.kyiv.tinedel.csvparser.util

import ua.kyiv.tinedel.csvparser.lexer.{Lexeme, Lexer}
import ua.kyiv.tinedel.csvparser.tokenizer.Token

class TestLexer(val iterator: Iterator[Lexeme[String]]) extends Lexer[String] with Iterator[Lexeme[String]] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): Lexeme[String] = iterator.next()

  override def lexemesStream(tokens: Stream[Token[String]]): Stream[Lexeme[String]] = ???
}

object TestLexer {
  def apply(lexemes: Lexeme[String]*): TestLexer = new TestLexer(Iterator(lexemes: _*))
}


