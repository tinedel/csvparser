package ua.kyiv.tinedel.csvparser.tokenizer

/**
 * Represents token in CSV
 * e.g. following text
 * {{{
 *   "quoted text",unquoted text
 *   and next line,
 * }}}
 * could be represented as series of Tokens
 * {{{
 *   QuotationMark, Block[String]("quoted text"), QuotationMark, FieldSeparator,Block("unquoted text"),
 *   RecordBreak, Block("and next line"), FieldSeparator
 * }}}
 *
 * Assuming tokenizer is configured with default settings for CSV
 *
 * @tparam T type of block to which tokenizer should capture block content to give flexibility in representation.
 * @see [[ua.kyiv.tinedel.csvparser.tokenizer.MinimalMemoryTokenizer]], [[ua.kyiv.tinedel.csvparser.tokenizer.GenericTokenizer]]
 */
sealed trait Token[+T]

/**
 * captures actual text of the field
 *
 * @param value captured content
 * @tparam T type of block to which tokenizer should capture block content e.g. String
 */
case class Block[T](value: T) extends Token[T]

/**
 * @inheritdoc
 */
case object QuotationMark extends Token[Nothing]

/**
 * @inheritdoc
 */
case object FieldSeparator extends Token[Nothing]

/**
 * @inheritdoc
 */
case object RecordSeparator extends Token[Nothing]

/**
 * @inheritdoc
 */
case object Escape extends Token[Nothing]