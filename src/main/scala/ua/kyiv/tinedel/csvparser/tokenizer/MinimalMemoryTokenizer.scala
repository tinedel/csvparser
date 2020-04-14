package ua.kyiv.tinedel.csvparser.tokenizer

import java.nio.CharBuffer
import java.nio.channels.ReadableByteChannel

import ua.kyiv.tinedel.csvparser.{ByteRange, ByteSeq}

import scala.io.Codec

/**
 * Experimental minimal memory consumption tokenizer. It keeps track of only length of Block tokens discarding any strings
 * Later on client can use this data to read actual data from file.
 *
 * Only to be used for files with extremely long records
 *
 * @see [[ua.kyiv.tinedel.csvparser.tokenizer.GenericTokenizer]]
 * @param readableByteChannel supplies data. Theoretically any kind of readable data channel could be used.
 *                            Only FileChannel and ReadableByteChannel from ByteArrayInputStream were tested.
 * @param tokensTrie          [[ua.kyiv.tinedel.csvparser.tokenizer.Trie]] with Tokens
 * @param blockSize           block to try to read from channel at once. Tokenizer will create character and byte buffer
 *                            which are able to store data of this size.
 * @param codec               codec to make byte data into string. Can be configured to replace, report or ignore errors during decoding.
 *                            In case if errors are reported parsing will throw exception if malformed or unmappable byte sequence is found
 */
class MinimalMemoryTokenizer(readableByteChannel: ReadableByteChannel,
                             tokensTrie: Trie[Token[Nothing]],
                             blockSize: Int = 512 * 1024)(implicit codec: Codec)
  extends GenericTokenizer[ByteSeq](readableByteChannel, tokensTrie, blockSize, buildBlock = MinimalMemoryTokenizer.buildStringBlock)
    with Iterator[Token[ByteSeq]] {
}

/**
 * Factory for [[ua.kyiv.tinedel.csvparser.tokenizer.MinimalMemoryTokenizer]] objects
 */
object MinimalMemoryTokenizer {

  /**
   * Builds block converting copy of CharBuffer back to bytes and toking the pos and limit as byte range
   */
  private val buildStringBlock = (charBuffer: CharBuffer, from: Int, to: Int, codec: Codec) => {
    val current = charBuffer.position()
    try {
      charBuffer.position(from)
      val bb = codec.encoder.encode(charBuffer.subSequence(0, to - from))
      Block(ByteSeq(List(ByteRange(bb.position(), bb.limit()))))
    } finally {
      charBuffer.position(current)
    }
  }

  /**
   * Creates [[ua.kyiv.tinedel.csvparser.tokenizer.MinimalMemoryTokenizer]] for given byte channel with provided strings
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
   * @return [[ua.kyiv.tinedel.csvparser.tokenizer.MinimalMemoryTokenizer]] configured with provided parameters
   */
  def apply(readableByteChannel: ReadableByteChannel,
            quotingString: String = "\"",
            recordSeparator: String = "\n",
            fieldSeparator: String = ",",
            escapeString: Option[String] = Some("\\"),
            blockSize: Int = 512 * 1024)(implicit codec: Codec): MinimalMemoryTokenizer = {

    val tokensTrie: Trie[Token[Nothing]] = Trie(
      quotingString -> QuotationMark,
      recordSeparator -> RecordSeparator,
      fieldSeparator -> FieldSeparator,
    )

    val withEscape = escapeString.map(es => tokensTrie `with` (es -> Escape)).getOrElse(tokensTrie)
    new MinimalMemoryTokenizer(readableByteChannel, withEscape, blockSize)
  }
}

