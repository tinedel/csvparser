package ua.kyiv.tinedel.csvparser

import java.io.{File, FileInputStream, InputStream}
import java.nio.channels.{Channels, ReadableByteChannel}

import ua.kyiv.tinedel.csvparser.lexer._
import ua.kyiv.tinedel.csvparser.tokenizer._

import scala.io.Codec

class RelaxedCSVParser(val lexer: Lexer[String],
                       val header: Boolean = false) extends Iterator[Map[Int, String]] {

  val headerRecord: Map[Int, String] = if (header && lexer.hasNext) next() else Map[Int, String]()

  override def hasNext: Boolean = {
    lexer.hasNext
  }

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
  def apply(file: File, header: Boolean = false,
            quotingString: String = "\"",
            lineSeparator: String = "\n",
            fieldDelimiter: String = ",",
            escapeString: Option[String] = None)(implicit codec: Codec): RelaxedCSVParser = {

    val tokenizer: SimpleTokenizer = SimpleTokenizer(new FileInputStream(file).getChannel, quotingString, lineSeparator, fieldDelimiter, escapeString)
    val lexer: SimpleLexer = SimpleLexer(tokenizer, quotingString, lineSeparator, fieldDelimiter, escapeString)

    new RelaxedCSVParser(lexer, header)
  }

  def fromStream(stream: InputStream,
                 header: Boolean = false,
                 quotingString: String = "\"",
                 lineSeparator: String = "\n",
                 fieldDelimiter: String = ",",
                 escapeString: Option[String] = None)(implicit codec: Codec): RelaxedCSVParser = {

    val tokenizer: SimpleTokenizer = SimpleTokenizer(Channels.newChannel(stream), quotingString, lineSeparator, fieldDelimiter, escapeString)
    val lexer: SimpleLexer = SimpleLexer(tokenizer, quotingString, lineSeparator, fieldDelimiter, escapeString)

    new RelaxedCSVParser(lexer, header)
  }

  def fromChannel(channel: ReadableByteChannel,
                  header: Boolean = false,
                  quotingString: String = "\"",
                  lineSeparator: String = "\n",
                  fieldDelimiter: String = ",",
                  escapeString: Option[String] = None)(implicit codec: Codec): RelaxedCSVParser = {

    val tokenizer: SimpleTokenizer = SimpleTokenizer(channel, quotingString, lineSeparator, fieldDelimiter, escapeString)
    val lexer: SimpleLexer = SimpleLexer(tokenizer, quotingString, lineSeparator, fieldDelimiter, escapeString)

    new RelaxedCSVParser(lexer, header)
  }
}
