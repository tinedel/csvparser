package ua.kyiv.tinedel.csvparser

import java.io.{File, FileInputStream, InputStream}
import java.nio.channels.{Channels, ReadableByteChannel}

import ua.kyiv.tinedel.csvparser.lexer._
import ua.kyiv.tinedel.csvparser.tokenizer._

import scala.io.Codec

/**
 * Relaxed csv parser trying to make as much sense of lexemes provided by lexer as possible.
 *
 * Returns records as Maps with 1-based indexes to string with default value of empty string
 * Field skips, unbalanced quotation, non-standard CSV is allowed. Parser does not expect fixed amount of fields
 * per record, meaning
 * {{{
 *   header, field name,
 *   val1, val2, val3, val4
 *   nr1,nr2
 * }}}
 *
 * is perfectly consumable CSV which will produce header row (if header == true) as
 * Map(1->"header", 2->"field name", 3->"")
 *
 * And content as Map(1->"val1", 2->"val2", 3->"val3", 4->"val4") and Map(1->"nr1", 2->"nr2")
 *
 * This parser do not distinguish between no value in a field and empty string.
 *
 * not thread-safe
 *
 * @param lexer  provides lexemes
 * @param header indicates the header must be parsed and stored as headerRow
 */
class RelaxedCSVParser(val lexer: Lexer[String] with Iterator[Lexeme[String]],
                       val header: Boolean = false) extends Iterator[Map[Int, String]] {

  val headerRecord: Map[Int, String] = if (header && lexer.hasNext) next() else Map[Int, String]()

  override def hasNext: Boolean = {
    lexer.hasNext
  }

  /**
   * At any given time previous token in Lexemes iterator was RecordBreak or the Lexeme to be read is the very first one
   * So to build the next record all Lexemes need to be read until RecordBreak or end of lexer is encountered
   *
   * As lexer is never should produce to fields one by one not separated by some kind of break.
   * Anyway even if this will happen they are problably should not be glued together and will be put in separate keys of
   * the resulting map. Which is probably fine.
   *
   * @return Map for the whole record of 1-based index to actual Field value
   */
  override def next(): Map[Int, String] = {
    var res = Map[Int, String]()
    var idx = 1
    var hadField = false
    while (lexer.hasNext) {
      lexer.next() match {
        case Field(value) =>
          hadField = true
          res = res + (idx -> value)
          idx += 1
        case FieldBreak =>
          if (!hadField) {
            res = res + (idx -> "")
            idx += 1
          }
          hadField = false
        case RecordBreak =>
          if (!hadField)
            res = res + (idx -> "")
          return res
      }
    }
    if (!hadField)
      res = res + (idx -> "")
    res
  }
}

object RelaxedCSVParser {

  /**
   * Creates Relaxed csv parser to read from file
   *
   * @param file            to read from
   * @param header          does it have a header
   * @param quotingString   to quote fields
   * @param recordSeparator to separate records
   * @param fieldSeparator  to separate fields
   * @param escapeString    optional escape string, supplying Some() here enables escaping
   * @param codec           to convert bytes to characters and back
   */
  def apply(file: File, header: Boolean = false,
            quotingString: String = "\"",
            recordSeparator: String = "\n",
            fieldSeparator: String = ",",
            escapeString: Option[String] = None)(implicit codec: Codec): RelaxedCSVParser = {

    val tokenizer: SimpleTokenizer = SimpleTokenizer(new FileInputStream(file).getChannel, quotingString, recordSeparator, fieldSeparator, escapeString)
    val lexer: SimpleLexer = SimpleLexer(tokenizer, quotingString, recordSeparator, fieldSeparator, escapeString)

    new RelaxedCSVParser(lexer, header)
  }

  /**
   * Creates relaxed csv parser to read from stream
   *
   * @param stream          to read from
   * @param header          does it have a header
   * @param quotingString   to quote fields
   * @param recordSeparator to separate records
   * @param fieldSeparator  to separate fields
   * @param escapeString    optional escape string, supplying Some() here enables escaping
   * @param codec           to convert bytes to characters and back
   */
  def fromStream(stream: InputStream,
                 header: Boolean = false,
                 quotingString: String = "\"",
                 recordSeparator: String = "\n",
                 fieldSeparator: String = ",",
                 escapeString: Option[String] = None)(implicit codec: Codec): RelaxedCSVParser = {

    val tokenizer: SimpleTokenizer = SimpleTokenizer(Channels.newChannel(stream), quotingString, recordSeparator, fieldSeparator, escapeString)
    val lexer: SimpleLexer = SimpleLexer(tokenizer, quotingString, recordSeparator, fieldSeparator, escapeString)

    new RelaxedCSVParser(lexer, header)
  }

  /**
   * Creates relaxed csv parser to read from channel
   *
   * @param channel         to read from
   * @param header          does it have a header
   * @param quotingString   to quote fields
   * @param recordSeparator to separate records
   * @param fieldSeparator  to separate fields
   * @param escapeString    optional escape string, supplying Some() here enables escaping
   * @param codec           to convert bytes to characters and back
   * @return
   */
  def fromChannel(channel: ReadableByteChannel,
                  header: Boolean = false,
                  quotingString: String = "\"",
                  recordSeparator: String = "\n",
                  fieldSeparator: String = ",",
                  escapeString: Option[String] = None)(implicit codec: Codec): RelaxedCSVParser = {

    val tokenizer: SimpleTokenizer = SimpleTokenizer(channel, quotingString, recordSeparator, fieldSeparator, escapeString)
    val lexer: SimpleLexer = SimpleLexer(tokenizer, quotingString, recordSeparator, fieldSeparator, escapeString)

    new RelaxedCSVParser(lexer, header)
  }
}
