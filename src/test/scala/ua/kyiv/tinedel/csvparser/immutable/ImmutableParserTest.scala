package ua.kyiv.tinedel.csvparser.immutable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.ParserBehaviors
import ua.kyiv.tinedel.csvparser.util.HugeFile

import scala.io.Source

class ImmutableParserTest extends AnyFlatSpec with Matchers with HugeFile with ParserBehaviors {

  behavior of "An immutable parser"

  it must behave like correctParser(lexemes => new ImmutableParser[String]("").records(lexemes))
  it must behave like correctParserWithHeader(lexemes => new ImmutableParser[String]("").withHeader(lexemes))

  it must behave like correctParserFromIOStream(iostream => {
    val tokenizer = ImmutableTokenizer().tokenize(Source.fromInputStream(iostream))
    val lexer = ImmutableLexer()

    new ImmutableParser[String]("").withHeader(lexer.lexemesStream(tokenizer))
  })

  it must behave like correctParserWithRelaxedCodec((iostream, codec) => {
    val tokenizer = ImmutableTokenizer().tokenize(Source.fromInputStream(iostream)(codec))
    val lexer = ImmutableLexer()

    new ImmutableParser[String]("").withHeader(lexer.lexemesStream(tokenizer))
  })

  var opennedChannel: Option[Source] = None
  it must behave like correctParserWithHugeFile(
    file => {
      opennedChannel = Some(Source.fromFile(file))
      new ImmutableParser[String]("").records(ImmutableLexer().lexemesStream(ImmutableTokenizer().tokenize(opennedChannel.get)))
    },
    opennedChannel.foreach(_.close)
  )
}
