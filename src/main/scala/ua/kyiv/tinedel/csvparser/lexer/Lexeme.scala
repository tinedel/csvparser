package ua.kyiv.tinedel.csvparser.lexer

/**
 * Represents Lexeme in CSV file. As quoting and escaping is syntactic sugar they are not represented as Lexeme
 *
 * Lexer is supposed to take them into account when building fields and breaks. For examples of how tokens are
 * translated into lexemes please look at SimpleLexerTest
 *
 * @tparam T data stored in fields
 */
sealed trait Lexeme[+T]

case class Field[T](value: T) extends Lexeme[T]

case object FieldBreak extends Lexeme[Nothing]

case object RecordBreak extends Lexeme[Nothing]