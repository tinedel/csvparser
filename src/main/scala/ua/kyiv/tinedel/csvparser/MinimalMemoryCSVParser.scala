package ua.kyiv.tinedel.csvparser

import java.io.{File, FileInputStream, InputStream}
import java.nio.channels.{Channels, ReadableByteChannel}

import ua.kyiv.tinedel.csvparser.lexer._
import ua.kyiv.tinedel.csvparser.tokenizer.MinimalMemoryTokenizer

import scala.io.Codec

class MinimalMemoryCSVParser(val lexer: Lexer[ByteSeq],
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
  def apply(file: File, header: Boolean = false,
            quotingString: String = "\"",
            lineSeparator: String = "\n",
            fieldDelimiter: String = ",",
            escapeString: Option[String] = None)(implicit codec: Codec): MinimalMemoryCSVParser = {

    val tokenizer: MinimalMemoryTokenizer = MinimalMemoryTokenizer(new FileInputStream(file).getChannel, quotingString, lineSeparator, fieldDelimiter, escapeString)
    val lexer: MinimalMemoryLexer = MinimalMemoryLexer(tokenizer, quotingString, lineSeparator, fieldDelimiter, escapeString)

    new MinimalMemoryCSVParser(lexer, header)
  }

  def fromStream(stream: InputStream,
                 header: Boolean = false,
                 quotingString: String = "\"",
                 lineSeparator: String = "\n",
                 fieldDelimiter: String = ",",
                 escapeString: Option[String] = None)(implicit codec: Codec): MinimalMemoryCSVParser = {

    val tokenizer: MinimalMemoryTokenizer = MinimalMemoryTokenizer(Channels.newChannel(stream), quotingString, lineSeparator, fieldDelimiter, escapeString)
    val lexer: MinimalMemoryLexer = MinimalMemoryLexer(tokenizer, quotingString, lineSeparator, fieldDelimiter, escapeString)

    new MinimalMemoryCSVParser(lexer, header)
  }

  def fromChannel(channel: ReadableByteChannel,
                  header: Boolean = false,
                  quotingString: String = "\"",
                  lineSeparator: String = "\n",
                  fieldDelimiter: String = ",",
                  escapeString: Option[String] = None)(implicit codec: Codec): MinimalMemoryCSVParser = {

    val tokenizer: MinimalMemoryTokenizer = MinimalMemoryTokenizer(channel, quotingString, lineSeparator, fieldDelimiter, escapeString)
    val lexer: MinimalMemoryLexer = MinimalMemoryLexer(tokenizer, quotingString, lineSeparator, fieldDelimiter, escapeString)

    new MinimalMemoryCSVParser(lexer, header)
  }
}
