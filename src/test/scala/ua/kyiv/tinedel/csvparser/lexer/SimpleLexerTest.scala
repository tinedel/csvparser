package ua.kyiv.tinedel.csvparser.lexer

import java.io.FileInputStream

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.tokenizer._
import ua.kyiv.tinedel.csvparser.{HugeFile, HugeFileTest}

class SimpleLexerTest extends AnyFlatSpec with Matchers with HugeFile {

  behavior of "A simple lexer"

  it must "be empty for empty tokenizer" in {
    val lexer = SimpleLexer(TestTokenizer())
    lexer mustBe empty
  }

  it must "produce one field from Block" in {
    val lexer = SimpleLexer(TestTokenizer(Block("some")))
    lexer.toStream must contain(Field("some"))
  }

  it must "produce one field from series of blocks" in {
    val lexer = SimpleLexer(TestTokenizer(Block("some "), Block("series "), Block("of blocks")))
    lexer.toStream must contain(Field("some series of blocks"))
  }

  it must "produce one field from quoted block" in {
    val lexer = SimpleLexer(TestTokenizer(
      QuotationMark, Block("in quotes"), QuotationMark
    ))
    lexer.toStream must contain(Field("in quotes"))
  }

  it must "produce one field from quoted series of blocks and tokens" in {
    val lexer = SimpleLexer(TestTokenizer(
      QuotationMark, Block("in quotes"), FieldSeparator, Block(" means in quotes"), RecordSeparator, Block("even new lines"), QuotationMark
    ))
    lexer.toStream must contain(Field("in quotes, means in quotes\neven new lines"))
  }

  it must "produce fields when mixing everything" in {
    val lexer = SimpleLexer(TestTokenizer(
      Block("first field"), FieldSeparator, Block("start nonquoted "), QuotationMark, Block("then remember"), FieldSeparator, Block(" that we need to quote"),
      RecordSeparator, Block("couple of things"), QuotationMark, Block(" and continue without quoting"), FieldSeparator, Block("next field"), RecordSeparator,
      Block("first"), FieldSeparator, Block("second"), FieldSeparator, Block("third")
    ))

    lexer.toList must contain theSameElementsInOrderAs List(
      Field("first field"), FieldBreak, Field("start nonquoted then remember, that we need to quote\ncouple of things and continue without quoting"), FieldBreak, Field("next field"), RecordBreak,
      Field("first"), FieldBreak, Field("second"), FieldBreak, Field("third")
    )
  }

  it must "not freak out if quoting is forgotten" in {
    val lexer = SimpleLexer(TestTokenizer(
      QuotationMark, Block("something"), RecordSeparator
    ))
    lexer.toStream must contain(Field("something\n"))
  }

  it must "escape quote" in {
    val lexer = SimpleLexer(TestTokenizer(
      Escape, QuotationMark, Block("something")
    ))

    lexer.toStream must contain(Field("\"something"))
  }

  it must "escape block" in {
    val lexer = SimpleLexer(TestTokenizer(
      Escape, Block("something")
    ))

    lexer.toStream must contain(Field("something"))
  }

  it must "escape block in middle" in {
    val lexer = SimpleLexer(TestTokenizer(
      Block("some"), Escape, Block("thing")
    ))

    lexer.toStream must contain(Field("something"))
  }

  it must "escape quote in quotation" in {
    val lexer = SimpleLexer(TestTokenizer(
      QuotationMark, Block("some"), Escape, QuotationMark, Block("thing"), FieldSeparator, QuotationMark, Block("anything")
    ))

    lexer.toStream must contain(Field("some\"thing,anything"))
  }

  it must "escape quote and others in quotation" in {
    val lexer = SimpleLexer(TestTokenizer(
      QuotationMark, Block("some"), Escape, QuotationMark, Block("thing"), Escape, FieldSeparator, QuotationMark, Block("anything"), FieldSeparator, Block("unquoted")
    ))

    lexer.toStream must contain theSameElementsInOrderAs List(Field("some\"thing,anything"), FieldBreak, Field("unquoted"))
  }

  it must "escape fieldseparator, recordseparator and escape" in {
    val lexer = SimpleLexer(TestTokenizer(Escape, FieldSeparator, Escape, RecordSeparator, Escape, Escape), recordSeparator = ",", fieldSeparator = "\n")
    lexer.toStream must contain(Field("\n,\\"))
  }

  it must "support double quotes" in {
    val lexer = SimpleLexer(TestTokenizer(QuotationMark, QuotationMark))
    lexer.toStream must contain(Field(""))
  }

  it must "support double quotes before block" in {
    val lexer = SimpleLexer(TestTokenizer(QuotationMark, QuotationMark, Block("something")))
    lexer.toStream must contain(Field("something"))
  }

  it must "support double quotes after block" in {
    val lexer = SimpleLexer(TestTokenizer(Block("something"), QuotationMark, QuotationMark))
    lexer.toStream must contain(Field("something"))
  }

  it must "support double quotes in the middle" in {
    val lexer = SimpleLexer(TestTokenizer(Block("some"), QuotationMark, QuotationMark, Block("thing")))
    lexer.toStream must contain(Field("something"))
  }

  it must "support double quotes before block in quotes" in {
    val lexer = SimpleLexer(TestTokenizer(QuotationMark, QuotationMark, QuotationMark, Block("something"), QuotationMark))
    lexer.toStream must contain(Field("\"something"))
  }

  it must "support double quotes after block in quotes" in {
    val lexer = SimpleLexer(TestTokenizer(QuotationMark, Block("something"), QuotationMark, QuotationMark, QuotationMark))
    lexer.toStream must contain(Field("something\""))
  }

  it must "support double quotes in the middle in quotes" in {
    val lexer = SimpleLexer(TestTokenizer(QuotationMark, Block("some"), QuotationMark, QuotationMark, Block("thing"), QuotationMark))
    lexer.toStream must contain(Field("some\"thing"))
  }

  /* Test create 1GiB file in /tmp directory and will run succesfully only on linux
  and may be other Unix like systems as it uses
  bash, /dev/urandom and dd to initially create file
  use sbt hugeFile:test to run
  to exclude in IntelliJ idea add "-l HugeFileTest" to test options
  -l is -L but lowercase
  */
  it must "be able to deal with huge files, e.g. /dev/urandom" taggedAs HugeFileTest in withFile(1024, 1024 * 1024) { file =>
    val channel = new FileInputStream(file).getChannel
    val default = Trie[Token[Nothing]](
      "\n" -> RecordSeparator,
      "," -> FieldSeparator,
      "\"" -> QuotationMark
    )

    try {
      info("Read started")
      var count = 0L
      var fieldCount = 0L
      val time = System.currentTimeMillis()
      var lastReport = time
      var charCount = 0L
      val simpleTokenizer = new SimpleTokenizer(channel, default)
      val lexemeCounts = SimpleLexer(simpleTokenizer).foldLeft(Map[Lexeme[String], Int]().withDefault(_ => 0)) { (map, lexeme) =>
        if ((System.currentTimeMillis() - lastReport) > 10 * 1000) {
          lastReport = System.currentTimeMillis()
          info(s"${count * 1000 / (lastReport - time)} lx/s, ${fieldCount * 1000 / (lastReport - time)} f/s, " +
            s"${charCount * 1000 / (lastReport - time)} c/s")
        }
        count += 1
        lexeme match {
          case Field(s) =>
            fieldCount += 1
            charCount += s.length
            map
          case other => map + (other -> (map(other) + 1))
        }
      }

      lexemeCounts must contain key RecordBreak
      lexemeCounts must contain key FieldBreak
      info(s"Lexemes read: ${lexemeCounts.mkString("\n")}")
      info(s"Field count: $fieldCount, field average: ${charCount * 1.0 / fieldCount}")
    } finally {
      channel.close()
    }
  }
}
