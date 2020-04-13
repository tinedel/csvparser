package ua.kyiv.tinedel.csvparser.tokenizer

sealed trait Token[+T]

case class Block[T](value: T) extends Token[T]

case object QuotationMark extends Token[Nothing]

case object FieldSeparator extends Token[Nothing]

case object RecordSeparator extends Token[Nothing]

case object Escape extends Token[Nothing]