package ua.kyiv.tinedel.csvparser.tokenizer

import java.io.FileInputStream
import java.nio.channels.{Channels, ReadableByteChannel}

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import ua.kyiv.tinedel.csvparser.util.HugeFile

class SimpleTokenizerTest extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks with HugeFile
  with TokenizerBehavior {

  behavior of "A simple tokenizer"

  it must behave like correctTokenizer(
    (is, trie, blockSize, codec) => new SimpleTokenizer(Channels.newChannel(is), trie, blockSize)(codec).toStream
  )

  var opennedFile: Option[ReadableByteChannel] = None
  it must behave like correctTokenizerWithHugeFile(
    file => {
      opennedFile = Some(new FileInputStream(file).getChannel)
      SimpleTokenizer(opennedFile.get).toStream
    },
    opennedFile.foreach(_.close())
  )
}
