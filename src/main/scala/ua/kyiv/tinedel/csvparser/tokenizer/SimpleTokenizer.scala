package ua.kyiv.tinedel.csvparser.tokenizer

import java.nio.CharBuffer
import java.nio.channels.ReadableByteChannel

import scala.io.Codec

class SimpleTokenizer(file: ReadableByteChannel,
                      tokensTrie: Trie[Token[Nothing]],
                      blockSize: Int = 512 * 1024)(implicit codec: Codec)
  extends GenericTokenizer[String](file, tokensTrie, blockSize, buildBlock = SimpleTokenizer.buildStringBlock)
    with Iterator[Token[String]] {
}

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

  def apply(file: ReadableByteChannel,
            quotingString: String = "\"",
            lineSeparator: String = "\n",
            fieldDelimiter: String = ",",
            escapeString: Option[String] = Some("\\"),
            blockSize: Int = 512 * 1024)(implicit codec: Codec): SimpleTokenizer = {

    val tokensTrie: Trie[Token[Nothing]] = Trie(
      quotingString -> QuotationMark,
      lineSeparator -> RecordSeparator,
      fieldDelimiter -> FieldSeparator,
    )

    val withEscape = escapeString.map(es => tokensTrie `with` (es -> Escape)).getOrElse(tokensTrie)
    new SimpleTokenizer(file, withEscape, blockSize)
  }
}
