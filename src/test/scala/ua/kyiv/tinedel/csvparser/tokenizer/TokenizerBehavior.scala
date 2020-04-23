package ua.kyiv.tinedel.csvparser.tokenizer

import java.io.{ByteArrayInputStream, File, InputStream}
import java.nio.charset.{Charset, StandardCharsets}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import ua.kyiv.tinedel.csvparser.util.{HugeFile, HugeFileTest}

import scala.io.Codec

trait TokenizerBehavior {
  this: AnyFlatSpec with Matchers with HugeFile with TableDrivenPropertyChecks =>

  private def getInputStreamFromString(text: String, charset: Charset = StandardCharsets.UTF_8): InputStream = {
    val bytes = text.getBytes(charset)
    new ByteArrayInputStream(bytes)
  }

  private val defaultBlockSize = 512 * 1024
  private val evenBlockSize = 16
  private val oddBlockSize = 15

  private val default = Trie[Token[Nothing]](
    "\n" -> RecordSeparator,
    "," -> FieldSeparator,
    "\"" -> QuotationMark
  )

  private val dosLineBreaks = Trie[Token[Nothing]](
    "\r\n" -> RecordSeparator,
    "," -> FieldSeparator,
    "\"" -> QuotationMark
  )

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

  def correctTokenizer(tokenize: (InputStream, Trie[Token[Nothing]], Int, Codec) => Stream[Token[String]]): Unit = {

    def concatBlocks(tokens: Stream[Token[String]]): List[Token[String]] = {
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
        val channel = getInputStreamFromString(text, c)
        val tokens = tokenize(channel, default, blockSize, codec)
        concatBlocks(tokens) must contain only Block(text)
      }

      it must s"tokenize string with special characters to separate blocks " +
        s"with charset ${c.name()} and $blockSize blockSize" in {

        val text = "some block of text with field separator,record separator\nquotation mark\""
        val channel = getInputStreamFromString(text, c)
        val tokens = tokenize(channel, default, blockSize, codec)
        concatBlocks(tokens) must contain theSameElementsInOrderAs List(Block("some block of text with field separator"),
          FieldSeparator, Block("record separator"), RecordSeparator, Block("quotation mark"), QuotationMark)
      }

      it must s"tokenize string with special characters to separate blocks and don't forget last bits " +
        s"with charset ${c.name()} and $blockSize blockSize" in {

        val text = "some block of text with field separator,record separator\nquotation mark\"last bits"
        val channel = getInputStreamFromString(text, c)
        val tokens = tokenize(channel, default, blockSize, codec)
        concatBlocks(tokens) must contain theSameElementsInOrderAs List(Block("some block of text with field separator"),
          FieldSeparator, Block("record separator"), RecordSeparator, Block("quotation mark"), QuotationMark,
          Block("last bits"))
      }

      it must s"tokenize alternative separators with ${c.name()} and $blockSize blockSize" in {

        val text = "break one way\nand then another\rand then first one\nagain"
        val channel = getInputStreamFromString(text, c)
        val tokens = tokenize(channel, default `with` "\r" -> RecordSeparator, blockSize, codec)
        concatBlocks(tokens) must contain theSameElementsInOrderAs List(Block("break one way"), RecordSeparator,
          Block("and then another"), RecordSeparator, Block("and then first one"), RecordSeparator, Block("again"))
      }

      it must s"be able to deal with dos linebreaks with ${c.name()} and $blockSize blockSize" in {

        val text = "dos\r\nbreak"
        val channel = getInputStreamFromString(text, c)
        val tokens = tokenize(channel, dosLineBreaks, blockSize, codec)
        concatBlocks(tokens) must contain theSameElementsInOrderAs List(Block("dos"), RecordSeparator, Block("break"))
      }

      it must s"be able to deal with repeated tokens with ${c.name()} and $blockSize blockSize" in {
        val text = s""""Venture ""Extended Edition\"\"\"""" // "Venture ""Extended Edition""" - because parser test was failing for this
        val channel = getInputStreamFromString(text, c)
        val tokens = tokenize(channel, default, blockSize, codec)
        concatBlocks(tokens) must contain theSameElementsInOrderAs List(
          QuotationMark, Block("Venture "), QuotationMark, QuotationMark, Block("Extended Edition"),
          QuotationMark, QuotationMark, QuotationMark
        )
      }
    }
  }

  def correctTokenizerWithHugeFile(tokenizer: File => Stream[Token[String]], cleanup: => Unit): Unit = {
    /*
      Test create 1GiB file in /tmp directory and will run succesfully only on linux
      and may be other Unix like systems as it uses
      bash, /dev/urandom and dd to initially create file
      use sbt hugeFile:test to run
      to exclude in IntelliJ idea add "-l HugeFileTest" to test options
      -l is -L but lowercase
   */
    it must "be able to deal with huge files, e.g. /dev/urandom" taggedAs HugeFileTest in withFile(1024, 1024 * 1024) { file =>
      try {
        info("Read started")
        var count = 0L
        val time = System.currentTimeMillis()
        var lastReport = time
        var charCount = 0L
        val tokenCounts = tokenizer(file).foldLeft(Map[Token[String], Int]().withDefault(_ => 0)) { (map, token) =>
          if ((System.currentTimeMillis() - lastReport) > 10 * 1000) {
            lastReport = System.currentTimeMillis()
            info(s"${count * 1000 / (lastReport - time)} tkn/s, ${charCount * 1000 / (lastReport - time)} c/s")
          }
          count += 1
          token match {
            case Block(s) =>
              charCount += s.length
              map
            case other => map + (other -> (map(other) + 1))
          }
        }

        tokenCounts must contain key RecordSeparator
        tokenCounts must contain key FieldSeparator
        tokenCounts must contain key QuotationMark
        info(s"Token read: ${tokenCounts.mkString("\n")}")
      } finally {
        cleanup
      }
    }
  }
}
