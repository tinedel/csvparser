package ua.kyiv.tinedel.csvparser.lexer

import java.io.FileInputStream
import java.nio.channels.ReadableByteChannel

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.tokenizer._
import ua.kyiv.tinedel.csvparser.util.HugeFile

class SimpleLexerTest extends AnyFlatSpec with Matchers with HugeFile with LexerBehaviors {

  behavior of "A simple lexer"

  it must behave like correctLexer(t => SimpleLexer(t).toStream)
  it must behave like correctLexerWithCustomTokens(m => t => new SimpleLexer(t, m).toStream)

  var openedChannel: Option[ReadableByteChannel] = None
  it must behave like correctLexerWithHugeFiles[String](
    file => {
      openedChannel = Some(new FileInputStream(file).getChannel)
      val tokenizer = SimpleTokenizer(openedChannel.get)
      SimpleLexer(
        tokenizer
      ).lexemesStream(tokenizer.toStream)
    },
    openedChannel.foreach(_.close()))

}
