package ua.kyiv.tinedel.csvparser.immutable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.lexer.{Lexeme, LexerBehaviors}
import ua.kyiv.tinedel.csvparser.tokenizer.{Token, Tokenizer}
import ua.kyiv.tinedel.csvparser.util.HugeFile

import scala.io.Source

class ImmutableLexerTest extends AnyFlatSpec with Matchers with HugeFile with LexerBehaviors {

  behavior of "An immutable lexer"

  /* do not have the state - we can reuse */
  private val lexer: ImmutableLexer[String] = ImmutableLexer()

  private def buildLexing(tokenMap: Map[Token[_], String])(t: Tokenizer[String]): Stream[Lexeme[String]] = {
    new ImmutableLexer[String](tokenMap, "", _ + _).lexemesStream(t.toStream)
  }

  it must behave like correctLexer(t => lexer.lexemesStream(t.toStream))
  it must behave like correctLexerWithCustomTokens(m => buildLexing(m))

  var openedChannel: Option[Source] = None

  it must behave like correctLexerWithHugeFiles[String](
    file => {
      openedChannel = Some(Source.fromFile(file))
      ImmutableLexer().lexemesStream(ImmutableTokenizer().tokenize(openedChannel.get))
    },
    openedChannel.foreach(_.close()))
}
