package ua.kyiv.tinedel.csvparser.lexer

sealed trait Lexeme[+T]

case class Field[T](value: T) extends Lexeme[T]

case object FieldBreak extends Lexeme[Nothing]

case object RecordBreak extends Lexeme[Nothing]