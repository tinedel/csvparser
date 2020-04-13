package ua.kyiv.tinedel.csvparser

import ua.kyiv.tinedel.csvparser.lexer.{Lexeme, Lexer}

class TestLexer(val iterator: Iterator[Lexeme[String]]) extends Lexer[String] {
  override def hasNext: Boolean = iterator.hasNext

  override def next(): Lexeme[String] = iterator.next()
}

object TestLexer {
  def apply(lexemes: Lexeme[String]*): TestLexer = new TestLexer(Iterator(lexemes: _*))
}


