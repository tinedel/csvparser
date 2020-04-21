package ua.kyiv.tinedel.csvparser.lexer.immutable

import java.io.FileInputStream
import java.nio.channels.{Channels, FileChannel}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.ParserBehaviors
import ua.kyiv.tinedel.csvparser.tokenizer.SimpleTokenizer
import ua.kyiv.tinedel.csvparser.util.HugeFile

class ImmutableParserTest extends AnyFlatSpec with Matchers with HugeFile with ParserBehaviors {

  behavior of "An immutable parser"

  it must behave like correctParser(lexemes => new ImmutableParser[String]("").records(lexemes))
  it must behave like correctParserWithHeader(lexemes => new ImmutableParser[String]("").withHeader(lexemes))

  it must behave like correctParserFromIOStream(iostream => {
    val tokenizer = SimpleTokenizer(Channels.newChannel(iostream))
    val lexer = ImmutableLexer()

    new ImmutableParser[String]("").withHeader(lexer.lexemesStream(tokenizer.toStream))
  })

  it must behave like correctParserWithRelaxedCodec((iostream, codec) => {
    val tokenizer = SimpleTokenizer(Channels.newChannel(iostream))(codec)
    val lexer = ImmutableLexer()

    new ImmutableParser[String]("").withHeader(lexer.lexemesStream(tokenizer.toStream))
  })

  var opennedChannel: Option[FileChannel] = None
  it must behave like correctParserWithHugeFile(
    file => {
      opennedChannel = Some(new FileInputStream(file).getChannel)
      new ImmutableParser[String]("").records(ImmutableLexer().lexemesStream(SimpleTokenizer(opennedChannel.get).toStream))
    },
    opennedChannel.foreach(_.close)
  )
}
