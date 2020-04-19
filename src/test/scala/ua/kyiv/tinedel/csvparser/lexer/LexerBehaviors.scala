package ua.kyiv.tinedel.csvparser.lexer

import java.io.File

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.tokenizer._
import ua.kyiv.tinedel.csvparser.util.{HugeFile, HugeFileTest, TestTokenizer}

trait LexerBehaviors {
  this: AnyFlatSpec with Matchers with HugeFile =>

  def correctLexer(lexer: Tokenizer[String] => Stream[Lexeme[String]]): Unit = {
    it must "be empty for empty tokenizer" in {
      val res = lexer(TestTokenizer())
      res mustBe empty
    }

    it must "produce one field from Block" in {
      val res = lexer(TestTokenizer(Block("some")))
      res must contain(Field("some"))
    }

    it must "produce one field from series of blocks" in {
      val res = lexer(TestTokenizer(Block("some "), Block("series "), Block("of blocks")))
      res must contain(Field("some series of blocks"))
    }

    it must "produce one field from quoted block" in {
      val res = lexer(TestTokenizer(
        QuotationMark, Block("in quotes"), QuotationMark
      ))
      res must contain(Field("in quotes"))
    }

    it must "produce one field from quoted series of blocks and tokens" in {
      val res = lexer(TestTokenizer(
        QuotationMark, Block("in quotes"), FieldSeparator, Block(" means in quotes"), RecordSeparator, Block("even new lines"), QuotationMark
      ))
      res must contain(Field("in quotes, means in quotes\neven new lines"))
    }

    it must "produce fields when mixing everything" in {
      val res = lexer(TestTokenizer(
        Block("first field"), FieldSeparator, Block("start nonquoted "), QuotationMark, Block("then remember"), FieldSeparator, Block(" that we need to quote"),
        RecordSeparator, Block("couple of things"), QuotationMark, Block(" and continue without quoting"), FieldSeparator, Block("next field"), RecordSeparator,
        Block("first"), FieldSeparator, Block("second"), FieldSeparator, Block("third")
      ))

      res must contain theSameElementsInOrderAs List(
        Field("first field"), FieldBreak, Field("start nonquoted then remember, that we need to quote\ncouple of things and continue without quoting"), FieldBreak, Field("next field"), RecordBreak,
        Field("first"), FieldBreak, Field("second"), FieldBreak, Field("third")
      )
    }

    it must "not freak out if quoting is forgotten" in {
      val res = lexer(TestTokenizer(
        QuotationMark, Block("something"), RecordSeparator
      ))
      res must contain(Field("something\n"))
    }

    it must "escape quote" in {
      val res = lexer(TestTokenizer(
        Escape, QuotationMark, Block("something")
      ))

      res must contain(Field("\"something"))
    }

    it must "escape block" in {
      val res = lexer(TestTokenizer(
        Escape, Block("something")
      ))

      res must contain(Field("something"))
    }

    it must "escape block in middle" in {
      val res = lexer(TestTokenizer(
        Block("some"), Escape, Block("thing")
      ))

      res must contain(Field("something"))
    }

    it must "escape quote in quotation" in {
      val res = lexer(TestTokenizer(
        QuotationMark, Block("some"), Escape, QuotationMark, Block("thing"), FieldSeparator, QuotationMark, Block("anything")
      ))

      res must contain(Field("some\"thing,anything"))
    }

    it must "escape quote and others in quotation" in {
      val res = lexer(TestTokenizer(
        QuotationMark, Block("some"), Escape, QuotationMark, Block("thing"), Escape, FieldSeparator, QuotationMark, Block("anything"), FieldSeparator, Block("unquoted")
      ))

      res must contain theSameElementsInOrderAs List(Field("some\"thing,anything"), FieldBreak, Field("unquoted"))
    }

    it must "support double quotes" in {
      val res = lexer(TestTokenizer(QuotationMark, QuotationMark))
      res must contain(Field(""))
    }

    it must "support double quotes before block" in {
      val res = lexer(TestTokenizer(QuotationMark, QuotationMark, Block("something")))
      res must contain(Field("something"))
    }

    it must "support double quotes after block" in {
      val res = lexer(TestTokenizer(Block("something"), QuotationMark, QuotationMark))
      res must contain(Field("something"))
    }

    it must "support double quotes in the middle" in {
      val res = lexer(TestTokenizer(Block("some"), QuotationMark, QuotationMark, Block("thing")))
      res must contain(Field("something"))
    }

    it must "support double quotes before block in quotes" in {
      val res = lexer(TestTokenizer(QuotationMark, QuotationMark, QuotationMark, Block("something"), QuotationMark))
      res must contain(Field("\"something"))
    }

    it must "support double quotes after block in quotes" in {
      val res = lexer(TestTokenizer(QuotationMark, Block("something"), QuotationMark, QuotationMark, QuotationMark))
      res must contain(Field("something\""))
    }

    it must "support double quotes in the middle in quotes" in {
      val res = lexer(TestTokenizer(QuotationMark, Block("some"), QuotationMark, QuotationMark, Block("thing"), QuotationMark))
      res must contain(Field("some\"thing"))
    }
  }

  def correctLexerWithCustomTokens(lexer: Map[Token[_], String] => Tokenizer[String] => Stream[Lexeme[String]]): Unit = {
    it must "escape fieldseparator, recordseparator and escape" in {
      val res = lexer(Map(RecordSeparator -> ",", FieldSeparator -> "\n", Escape -> "\\"))(TestTokenizer(Escape, FieldSeparator, Escape, RecordSeparator, Escape, Escape))
      res must contain(Field("\n,\\"))
    }
  }

  def correctLexerWithHugeFiles[T](tokenizerFromFile: File => Tokenizer[T],
                                   lexerFromTokenizer: Tokenizer[T] => Lexer[T],
                                   cleanup: => Unit): Unit = {
    /* Test create 1GiB file in /tmp directory and will run succesfully only on linux
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
        var fieldCount = 0L
        val time = System.currentTimeMillis()
        var lastReport = time
        var charCount = 0L
        val simpleTokenizer = tokenizerFromFile(file)
        val lexemeCounts = lexerFromTokenizer(simpleTokenizer).lexemesStream(simpleTokenizer.toStream)
          .foldLeft(Map[Lexeme[T], Int]().withDefault(_ => 0)) { (map, lexeme) =>
            if ((System.currentTimeMillis() - lastReport) > 10 * 1000) {
              lastReport = System.currentTimeMillis()
              info(s"${count * 1000 / (lastReport - time)} lx/s, ${fieldCount * 1000 / (lastReport - time)} f/s, " +
                s"${charCount * 1000 / (lastReport - time)} c/s")
            }
            count += 1
            lexeme match {
              case Field(s) =>
                fieldCount += 1
                charCount += s.toString.length
                map
              case other => map + (other -> (map(other) + 1))
            }
          }

        lexemeCounts must contain key RecordBreak
        lexemeCounts must contain key FieldBreak
        info(s"Lexemes read: ${lexemeCounts.mkString("\n")}")
        info(s"Field count: $fieldCount, field average: ${charCount * 1.0 / fieldCount}")
      } finally {
        cleanup
      }
    }
  }
}
