package ua.kyiv.tinedel.csvparser.tokenizer

import java.nio.CharBuffer
import java.nio.channels.ReadableByteChannel

import ua.kyiv.tinedel.csvparser.{ByteRange, ByteSeq}

import scala.io.Codec


class MinimalMemoryTokenizer(file: ReadableByteChannel,
                             tokensTrie: Trie[Token[Nothing]],
                             blockSize: Int = 512 * 1024)(implicit codec: Codec)
  extends GenericTokenizer[ByteSeq](file, tokensTrie, blockSize, buildBlock = MinimalMemoryTokenizer.buildStringBlock)
    with Iterator[Token[ByteSeq]] {
}

object MinimalMemoryTokenizer {
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

  def apply(file: ReadableByteChannel,
            quotingString: String = "\"",
            lineSeparator: String = "\n",
            fieldDelimiter: String = ",",
            escapeString: Option[String] = Some("\\"),
            blockSize: Int = 512 * 1024)(implicit codec: Codec): MinimalMemoryTokenizer = {

    val tokensTrie: Trie[Token[Nothing]] = Trie(
      quotingString -> QuotationMark,
      lineSeparator -> RecordSeparator,
      fieldDelimiter -> FieldSeparator,
    )

    val withEscape = escapeString.map(es => tokensTrie `with` (es -> Escape)).getOrElse(tokensTrie)
    new MinimalMemoryTokenizer(file, withEscape, blockSize)
  }
}

