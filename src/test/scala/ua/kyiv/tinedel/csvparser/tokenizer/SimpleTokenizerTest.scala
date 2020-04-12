package ua.kyiv.tinedel.csvparser.tokenizer

import java.io.ByteArrayInputStream
import java.nio.channels.{Channels, ReadableByteChannel}
import java.nio.charset.{Charset, StandardCharsets}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.io.Codec

class SimpleTokenizerTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  behavior of "A simple tokenizer"

  private val defaultBlockSize = 512 * 1024
  private val evenBlockSize = 16
  private val oddBlockSize = 15

  private val charsets = Table(
    ("charset", "blockSize"),
    (StandardCharsets.ISO_8859_1, defaultBlockSize),
    (StandardCharsets.US_ASCII, defaultBlockSize),
    (StandardCharsets.UTF_8, defaultBlockSize),
    (StandardCharsets.UTF_16, defaultBlockSize),
    (StandardCharsets.UTF_16BE, defaultBlockSize),
    (StandardCharsets.UTF_16LE, defaultBlockSize),
    (StandardCharsets.US_ASCII, evenBlockSize),
    (StandardCharsets.UTF_8, evenBlockSize),
    (StandardCharsets.UTF_16LE, evenBlockSize),
    (StandardCharsets.US_ASCII, oddBlockSize),
    (StandardCharsets.UTF_8, oddBlockSize),
    (StandardCharsets.UTF_16LE, oddBlockSize)
  )

  def concatBlocks(tokens: List[Token[String]]): List[Token[String]] = {
    tokens.reverse.foldLeft(List[Token[String]]()) {
      case (Nil, token) => token :: Nil
      case (Block(found) :: tail, Block(prevToken)) => Block(prevToken + found) :: tail
      case (list, token) => token :: list
    }
  }

  forAll(charsets) { (c: Charset, blockSize: Int) =>

    implicit val codec: Codec = Codec(c)

    it must s"tokenize string without special characters to one block " +
      s"with charset ${c.name()} and $blockSize blockSize" in {

      val text = "some block of text without special characters"
      val channel: ReadableByteChannel = getChannelFromString(text, c)
      val tokens = new SimpleTokenizer(channel, default, blockSize).toList
      concatBlocks(tokens) must contain only Block(text)
    }

    it must s"tokenize string with special characters to separate blocks " +
      s"with charset ${c.name()} and $blockSize blockSize" in {

      val text = "some block of text with field separator,record separator\nquotation mark\""
      val channel: ReadableByteChannel = getChannelFromString(text, c)
      val tokens = new SimpleTokenizer(channel, default, blockSize).toList
      concatBlocks(tokens) must contain theSameElementsInOrderAs List(Block("some block of text with field separator"),
        FieldSeparator, Block("record separator"), RecordSeparator, Block("quotation mark"), QuotationMark)
    }

    it must s"tokenize string with special characters to separate blocks and don't forget last bits " +
      s"with charset ${c.name()} and $blockSize blockSize" in {

      val text = "some block of text with field separator,record separator\nquotation mark\"last bits"
      val channel: ReadableByteChannel = getChannelFromString(text, c)
      val tokens = new SimpleTokenizer(channel, default, blockSize).toList
      concatBlocks(tokens) must contain theSameElementsInOrderAs List(Block("some block of text with field separator"),
        FieldSeparator, Block("record separator"), RecordSeparator, Block("quotation mark"), QuotationMark,
        Block("last bits"))
    }

    it must s"tokenize alternative separators with ${c.name()} and $blockSize blockSize" in {

      val text = "break one way\nand then another\rand then first one\nagain"
      val channel: ReadableByteChannel = getChannelFromString(text, c)
      val tokens = new SimpleTokenizer(channel, default `with` "\r" -> RecordSeparator, blockSize).toList
      concatBlocks(tokens) must contain theSameElementsInOrderAs List(Block("break one way"), RecordSeparator,
        Block("and then another"), RecordSeparator, Block("and then first one"), RecordSeparator, Block("again"))
    }

    it must s"be able to deal with dos linebreaks with ${c.name()} and $blockSize blockSize" in {

      val text = "dos\r\nbreak"
      val channel: ReadableByteChannel = getChannelFromString(text, c)
      val tokens = new SimpleTokenizer(channel, dosLineBreaks, blockSize).toList
      concatBlocks(tokens) must contain theSameElementsInOrderAs List(Block("dos"), RecordSeparator, Block("break"))
    }
  }

  val default: Trie = Trie(
    "\n" -> RecordSeparator,
    "," -> FieldSeparator,
    "\"" -> QuotationMark
  )

  val dosLineBreaks: Trie = Trie(
    "\r\n" -> RecordSeparator,
    "," -> FieldSeparator,
    "\"" -> QuotationMark
  )

  private def getChannelFromString(text: String, charset: Charset = StandardCharsets.UTF_8) = {
    val bytes = text.getBytes(charset)
    val bais = new ByteArrayInputStream(bytes)
    Channels.newChannel(bais)
  }
}
