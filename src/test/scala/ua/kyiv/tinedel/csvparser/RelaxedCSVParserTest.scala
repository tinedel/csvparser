package ua.kyiv.tinedel.csvparser

import java.io.FileInputStream
import java.nio.channels.FileChannel

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import ua.kyiv.tinedel.csvparser.util.HugeFile

class RelaxedCSVParserTest extends AnyFlatSpec with Matchers with HugeFile with ParserBehaviors {

  behavior of "A relaxed csv parser"

  it must behave like correctParser(lexemes => new RelaxedCSVParser(lexemes.iterator).toStream)
  it must behave like correctParserWithHeader(lexemes => {
    val parser = new RelaxedCSVParser(lexemes.iterator, true)
    (Some(parser.headerRecord), parser.toStream)
  })

  it must behave like correctParserFromIOStream(iostream => {
    val parser = RelaxedCSVParser.fromStream(iostream, header = true)
    (Some(parser.headerRecord), parser.toStream)
  })

  it must behave like correctParserWithRelaxedCodec((iostream, codec) => {
    val parser = RelaxedCSVParser.fromStream(iostream, header = true)(codec)
    (Some(parser.headerRecord), parser.toStream)
  })

  var opennedChannel: Option[FileChannel] = None
  it must behave like correctParserWithHugeFile(
    file => {
      opennedChannel = Some(new FileInputStream(file).getChannel)
      RelaxedCSVParser.fromChannel(opennedChannel.get).toStream
    },
    opennedChannel.foreach(_.close)
  )
}
