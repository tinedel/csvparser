package ua.kyiv.tinedel.csvparser.immutable

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import ua.kyiv.tinedel.csvparser.tokenizer.TokenizerBehavior
import ua.kyiv.tinedel.csvparser.util.HugeFile

import scala.io.Source

class ImmutableTokenizerTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks with HugeFile
  with TokenizerBehavior {

  behavior of "An immutable tokenizer"

  it must behave like correctTokenizer(
    (is, trie, _, codec) => new ImmutableTokenizer(trie).tokenize(Source.fromInputStream(is)(codec))
  )

  var opennedFile: Option[Source] = None
  it must behave like correctTokenizerWithHugeFile(
    file => {
      opennedFile = Some(Source.fromFile(file))
      ImmutableTokenizer().tokenize(opennedFile.get)
    },
    opennedFile.foreach(_.close())
  )
}
