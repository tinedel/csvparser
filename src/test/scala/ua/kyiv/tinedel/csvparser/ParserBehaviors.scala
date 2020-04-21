package ua.kyiv.tinedel.csvparser

import java.io.{File, InputStream}
import java.nio.charset.CodingErrorAction

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.lexer.{Field, FieldBreak, Lexeme, RecordBreak}
import ua.kyiv.tinedel.csvparser.tokenizer.TokenizerException
import ua.kyiv.tinedel.csvparser.util.{HugeFile, HugeFileTest, TestLexer}

import scala.io.Codec

trait ParserBehaviors {
  this: AnyFlatSpec with Matchers with HugeFile =>

  def correctParser(parser: Stream[Lexeme[String]] => Stream[Map[Int, String]]): Unit = {
    it must "be empty if lexer is empty" in {
      val res = parser(TestLexer().toStream)
      res mustBe empty
    }

    it must "have 2 rows if header set to false" in {
      val res = parser(TestLexer(Field("id"), FieldBreak, Field("name"), FieldBreak, Field("value"), RecordBreak,
        Field("1"), FieldBreak, FieldBreak).toStream)
      res must contain allOf(
        Map(1 -> "id", 2 -> "name", 3 -> "value"),
        Map(1 -> "1", 2 -> "", 3 -> "")
      )
    }
  }

  def correctParserWithHeader(parser: Stream[Lexeme[String]] => (Option[Map[Int, String]], Stream[Map[Int, String]])): Unit = {
    it must "have header row and be empty if only header in lexer" in {
      val (header, res) = parser(TestLexer(Field("id"), FieldBreak, Field("name"), FieldBreak, Field("value")).toStream)
      res mustBe empty
      header.get must contain allOf(1 -> "id", 2 -> "name", 3 -> "value")
    }

    it must "have header row and row" in {
      val (header, res) = parser(TestLexer(Field("id"), FieldBreak, Field("name"), FieldBreak, Field("value"), RecordBreak,
        Field("1"), FieldBreak, FieldBreak).toStream)
      res must contain(
        Map(1 -> "1", 2 -> "", 3 -> "")
      )
      header.get must contain allOf(1 -> "id", 2 -> "name", 3 -> "value")
    }
  }

  def correctParserFromIOStream(parser: InputStream => (Option[Map[Int, String]], Stream[Map[Int, String]])): Unit = {
    it must "handle correctness test" in {
      val (header, records) = parser(getClass.getResourceAsStream("/correctness.csv"))
      val data = records.toList.map(
        m => m.toList.sortBy(_._1).map(_._2)
      )

      data must contain theSameElementsInOrderAs List(
        List("1997", "Ford", "E350", "ac, abs, moon", "3000.00"),
        List("1999", "Chevy", "Venture \"Extended Edition\"", "", "4900.00"),
        List("1996", "Jeep", "Grand Cherokee", "MUST SELL!\nair, moon roof, loaded", "4799.00"),
        List("1999", "Chevy", "Venture \"Extended Edition, Very Large\"", "", "5000.00"),
        List("", "", "Venture \"Extended Edition\"", "", "4900.00")
      )

      header.get must contain allOf(
        1 -> "Year", 2 -> "Make", 3 -> "Model", 4 -> "Description", 5 -> "Price"
      )

      header.get.size must be(5)
    }

    it must "handle incorrect encoding throwing by default" in {
      val parser = RelaxedCSVParser.fromStream(getClass.getResourceAsStream("/UTF-8-test.txt"))

      a[TokenizerException] must be thrownBy parser.toList
    }
  }

  def correctParserWithRelaxedCodec(parser: (InputStream, Codec) => (Option[Map[Int, String]], Stream[Map[Int, String]])): Unit = {

    implicit val codec: Codec = Codec.UTF8
      .onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE)
      .decodingReplaceWith("?")


    it must "handle incorrect encoding with replacement if configured" in {

      val (header, res) = parser(getClass.getResourceAsStream("/UTF-8-test.txt"), codec)

      res.toList must not be empty
    }
  }

  def correctParserWithHugeFile(parseFile: File => Stream[Map[Int, String]], cleanUp: => Unit): Unit = {
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
        parseFile(file).foreach { record =>
          if ((System.currentTimeMillis() - lastReport) > 10 * 1000) {
            lastReport = System.currentTimeMillis()
            info(s"${count * 1000 / (lastReport - time)} r/s, ${fieldCount * 1000 / (lastReport - time)} f/s")
          }
          count += 1
          fieldCount += record.size
        }

      } finally {
        cleanUp
      }
    }
  }
}