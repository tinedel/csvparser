package ua.kyiv.tinedel.csvparser

import java.io.FileInputStream
import java.nio.charset.CodingErrorAction

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.lexer.{Field, FieldBreak, RecordBreak}
import ua.kyiv.tinedel.csvparser.tokenizer.TokenizerException

import scala.io.Codec

class RelaxedCSVParserTest extends AnyFlatSpec with Matchers with HugeFile {

  behavior of "A relaxed csv parser"

  it must "be empty if lexer is empty" in {
    val parser = new RelaxedCSVParser(TestLexer())
    parser mustBe empty
    parser.headerRecord mustBe empty
  }

  it must "have header row and be empty if only header in lexer" in {
    val parser = new RelaxedCSVParser(TestLexer(Field("id"), FieldBreak, Field("name"), FieldBreak, Field("value")), true)
    parser mustBe empty
    parser.headerRecord must contain allOf(1 -> "id", 2 -> "name", 3 -> "value")
  }

  it must "have header row and row" in {
    val parser = new RelaxedCSVParser(TestLexer(Field("id"), FieldBreak, Field("name"), FieldBreak, Field("value"), RecordBreak,
      Field("1"), FieldBreak, FieldBreak), true)
    parser.toStream must contain(
      Map(1 -> "1", 2 -> "", 3 -> "")
    )
    parser.headerRecord must contain allOf(1 -> "id", 2 -> "name", 3 -> "value")
  }

  it must "have 2 rows if header set to false" in {
    val parser = new RelaxedCSVParser(TestLexer(Field("id"), FieldBreak, Field("name"), FieldBreak, Field("value"), RecordBreak,
      Field("1"), FieldBreak, FieldBreak), false)
    parser.toStream must contain allOf(
      Map(1 -> "id", 2 -> "name", 3 -> "value"),
      Map(1 -> "1", 2 -> "", 3 -> "")
    )
    parser.headerRecord mustBe empty
  }

  it must "handle correctness test" in {
    val parser = RelaxedCSVParser.fromStream(getClass.getResourceAsStream("/correctness.csv"), header = true)
    val data = parser.toList.map(
      m => m.toList.sortBy(_._1).map(_._2)
    )

    data must contain theSameElementsInOrderAs List(
      List("1997", "Ford", "E350", "ac, abs, moon", "3000.00"),
      List("1999", "Chevy", "Venture \"Extended Edition\"", "", "4900.00"),
      List("1996", "Jeep", "Grand Cherokee", "MUST SELL!\nair, moon roof, loaded", "4799.00"),
      List("1999", "Chevy", "Venture \"Extended Edition, Very Large\"", "", "5000.00"),
      List("", "", "Venture \"Extended Edition\"", "", "4900.00")
    )

    parser.headerRecord must contain allOf(
      1 -> "Year", 2 -> "Make", 3 -> "Model", 4 -> "Description", 5 -> "Price"
    )

    parser.headerRecord.size must be(5)
  }

  it must "handle incorrect encoding throwing by default" in {
    val parser = RelaxedCSVParser.fromStream(getClass.getResourceAsStream("/UTF-8-test.txt"))

    a[TokenizerException] must be thrownBy parser.toList
  }

  it must "handle incorrect encoding with replacement if configured" in {

    implicit val codec: Codec = Codec.UTF8
      .onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.REPLACE)
      .decodingReplaceWith("?")

    val parser = RelaxedCSVParser.fromStream(getClass.getResourceAsStream("/UTF-8-test.txt"))

    parser.toList must not be empty
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

    try {
      info("Read started")
      var count = 0L
      var fieldCount = 0L

      val time = System.currentTimeMillis()
      var lastReport = time
      RelaxedCSVParser.fromChannel(channel).foreach { record =>
        if ((System.currentTimeMillis() - lastReport) > 10 * 1000) {
          lastReport = System.currentTimeMillis()
          info(s"${count * 1000 / (lastReport - time)} r/s, ${fieldCount * 1000 / (lastReport - time)} f/s")
        }
        count += 1
        fieldCount += record.size
      }

    } finally {
      channel.close()
    }
  }
}
