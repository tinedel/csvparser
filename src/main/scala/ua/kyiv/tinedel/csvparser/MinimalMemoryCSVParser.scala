package ua.kyiv.tinedel.csvparser

import java.io.{File, FileInputStream, InputStream}
import java.nio.channels.{Channels, ReadableByteChannel}

import ua.kyiv.tinedel.csvparser.lexer._
import ua.kyiv.tinedel.csvparser.tokenizer.MinimalMemoryTokenizer

import scala.io.Codec

/**
 * Experimental csv parser using byte ranges to operate. Theoretically allows to process files of arbitrary sized fields and records,
 *
 * As a downside - extremely not handy to use. Result of parsing is the Iterator of Maps representing one record each.
 * {{{
 *   Map(1 -> ByteSeq(ByteRang(0, 10), ByteRange(12, 15)))
 * }}}
 * Is being the parse result of 0123456789""234
 *
 * So to read actual values client must open the file from which the data were parsed, extract given bytes and convert them
 * to strings if the length permits using correct encodings. Please note that actual Byte not characters being used,
 * pay attention to multi/variable byte encodings.
 *
 * @param lexer  ByteSeq operating lexer
 * @param header true if file has a header
 */
class MinimalMemoryCSVParser(val lexer: Lexer[ByteSeq] with Iterator[Lexeme[ByteSeq]],
                             val header: Boolean = false) extends Iterator[Map[Int, ByteSeq]] {

  val headerRecord: Map[Int, ByteSeq] = if (header && lexer.hasNext) next() else Map[Int, ByteSeq]()

  override def hasNext: Boolean = {
    lexer.hasNext
  }

  override def next(): Map[Int, ByteSeq] = {
    var res = Map[Int, ByteSeq]()
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
            res = res + (idx -> ByteSeq(List()))
            idx += 1
          }
          hadField = false
        case RecordBreak =>
          if (!hadField)
            res = res + (idx -> ByteSeq(List()))
          return res
      }
    }
    if (!hadField)
      res = res + (idx -> ByteSeq(List()))
    res
  }
}


object MinimalMemoryCSVParser {
  /**
   * Creates Minimal memory csv parser to read from file
   *
   * @param file            to read from
   * @param header          does it have a header
   * @param quotingString   to quote fields
   * @param recordSeparator to separate records
   * @param fieldSeparator  to separate fields
   * @param escapeString    optional escape string, supplying Some() here enables escaping
   * @param codec           to convert bytes to characters and back
   * @return
   */
  def apply(file: File, header: Boolean = false,
            quotingString: String = "\"",
            recordSeparator: String = "\n",
            fieldSeparator: String = ",",
            escapeString: Option[String] = None)(implicit codec: Codec): MinimalMemoryCSVParser = {

    val tokenizer: MinimalMemoryTokenizer = MinimalMemoryTokenizer(new FileInputStream(file).getChannel, quotingString, recordSeparator, fieldSeparator, escapeString)
    val lexer: MinimalMemoryLexer = MinimalMemoryLexer(tokenizer, quotingString, recordSeparator, fieldSeparator, escapeString)

    new MinimalMemoryCSVParser(lexer, header)
  }

  /**
   * Creates Minimal memory csv parser to read from stream
   *
   * @param stream          to read from
   * @param header          does it have a header
   * @param quotingString   to quote fields
   * @param recordSeparator to separate records
   * @param fieldSeparator  to separate fields
   * @param escapeString    optional escape string, supplying Some() here enables escaping
   * @param codec           to convert bytes to characters and back
   * @return
   */
  def fromStream(stream: InputStream,
                 header: Boolean = false,
                 quotingString: String = "\"",
                 recordSeparator: String = "\n",
                 fieldSeparator: String = ",",
                 escapeString: Option[String] = None)(implicit codec: Codec): MinimalMemoryCSVParser = {

    val tokenizer: MinimalMemoryTokenizer = MinimalMemoryTokenizer(Channels.newChannel(stream), quotingString, recordSeparator, fieldSeparator, escapeString)
    val lexer: MinimalMemoryLexer = MinimalMemoryLexer(tokenizer, quotingString, recordSeparator, fieldSeparator, escapeString)

    new MinimalMemoryCSVParser(lexer, header)
  }

  /**
   * Creates Minimal memory csv parser to read from channel
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
                  escapeString: Option[String] = None)(implicit codec: Codec): MinimalMemoryCSVParser = {

    val tokenizer: MinimalMemoryTokenizer = MinimalMemoryTokenizer(channel, quotingString, recordSeparator, fieldSeparator, escapeString)
    val lexer: MinimalMemoryLexer = MinimalMemoryLexer(tokenizer, quotingString, recordSeparator, fieldSeparator, escapeString)

    new MinimalMemoryCSVParser(lexer, header)
  }
}
