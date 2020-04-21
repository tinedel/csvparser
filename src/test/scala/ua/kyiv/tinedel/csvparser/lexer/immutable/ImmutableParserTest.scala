package ua.kyiv.tinedel.csvparser.lexer.immutable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.ParserBehaviors
import ua.kyiv.tinedel.csvparser.util.HugeFile

class ImmutableParserTest extends AnyFlatSpec with Matchers with HugeFile with ParserBehaviors {

  behavior of "An immutable parser"

  it must behave like correctParser(lexemes => new ImmutableParser[String]("").records(lexemes))
  it must behave like correctParserWithHeader(lexemes => new ImmutableParser[String]("").withHeader(lexemes))

  /* it must behave like correctParserFromIOStream(iostream => {
    val parser = RelaxedCSVParser.fromStream(iostream, header = true)
    (Some(parser.headerRecord), parser.toStream)
  })

  it must behave like correctParserWithRelaxedCodec((iostream, codec) => {
    val parser = RelaxedCSVParser.fromStream(iostream, header = true)(codec)
    (Some(parser.headerRecord), parser.toStream)
  }) */

}
