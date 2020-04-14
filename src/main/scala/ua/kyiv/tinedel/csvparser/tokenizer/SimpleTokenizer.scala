package ua.kyiv.tinedel.csvparser.tokenizer

import java.nio.CharBuffer
import java.nio.channels.ReadableByteChannel

import scala.io.Codec

/**
 * Simple concrete implementation of the [[ua.kyiv.tinedel.csvparser.tokenizer.Tokenizer]] storing block content in stings
 *
 * @see [[ua.kyiv.tinedel.csvparser.tokenizer.GenericTokenizer]]
 * @inheritdoc
 */
class SimpleTokenizer(readableByteChannel: ReadableByteChannel,
                      tokensTrie: Trie[Token[Nothing]],
                      blockSize: Int = 512 * 1024)(implicit codec: Codec)
  extends GenericTokenizer[String](readableByteChannel, tokensTrie, blockSize, buildBlock = SimpleTokenizer.buildStringBlock)
    with Iterator[Token[String]] {
}

/**
 * Object factory for [[ua.kyiv.tinedel.csvparser.tokenizer.SimpleTokenizer]]
 */
object SimpleTokenizer {
  private val buildStringBlock = (charBuffer: CharBuffer, from: Int, to: Int, _: Codec) => {
    val current = charBuffer.position()
    try {
      charBuffer.position(from)
      Block(charBuffer.subSequence(0, to - from).toString)
    } finally {
      charBuffer.position(current)
    }
  }

  /**
   * Creates [[ua.kyiv.tinedel.csvparser.tokenizer.SimpleTokenizer]] for given byte channel with provided strings
   * to be used for splitting input data into tokens
   *
   * @param readableByteChannel opened readable byte channel. Not being closed by the tokenizer
   * @param quotingString       string to quote fields. If encountered in input data will produce
   *                            [[ua.kyiv.tinedel.csvparser.tokenizer.QuotationMark]] token
   * @param recordSeparator     string to separate records. If encountered in input data will produce
   *                            [[ua.kyiv.tinedel.csvparser.tokenizer.RecordSeparator]] token
   * @param fieldSeparator      string to separate fields. If encountered in input data will produce
   *                            [[ua.kyiv.tinedel.csvparser.tokenizer.FieldSeparator]] token
   * @param escapeString        string to escape other tokens. By default is not recognized, but if provided will produce
   *                            [[ua.kyiv.tinedel.csvparser.tokenizer.Escape]] token
   * @param blockSize           tokenizer is reading data in blocks of the given size
   * @param codec               codec to be used to making sense of data.
   * @return [[ua.kyiv.tinedel.csvparser.tokenizer.SimpleTokenizer]] configured with provided parameters
   */
  def apply(readableByteChannel: ReadableByteChannel,
            quotingString: String = "\"",
            recordSeparator: String = "\n",
            fieldSeparator: String = ",",
            escapeString: Option[String] = Some("\\"),
            blockSize: Int = 512 * 1024)(implicit codec: Codec): SimpleTokenizer = {

    val tokensTrie: Trie[Token[Nothing]] = Trie(
      quotingString -> QuotationMark,
      recordSeparator -> RecordSeparator,
      fieldSeparator -> FieldSeparator,
    )

    val withEscape = escapeString.map(es => tokensTrie `with` (es -> Escape)).getOrElse(tokensTrie)
    new SimpleTokenizer(readableByteChannel, withEscape, blockSize)
  }
}
