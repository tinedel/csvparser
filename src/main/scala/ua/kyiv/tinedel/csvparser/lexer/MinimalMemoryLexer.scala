package ua.kyiv.tinedel.csvparser.lexer

import ua.kyiv.tinedel.csvparser.tokenizer._
import ua.kyiv.tinedel.csvparser.{ByteRange, ByteSeq}

import scala.io.Codec

class MinimalMemoryLexer private(tokenizer: Tokenizer[ByteSeq], tokens: Map[Token[_], ByteSeq])
  extends GenericCSVLexer[ByteSeq](tokenizer, tokens, ByteSeq(List()), (a, b) => a + b) {

  var pos = 0L

  override def nextToken: Token[ByteSeq] = {
    val res = super.nextToken
    pos = res match {
      case Block(value) => pos + value.seq.head.to - value.seq.head.from
      case t: Token[_] => pos + tokens(t).seq.head.to - tokens(t).seq.head.from
    }
    res
  }

  override def prependToContext(b: Block[ByteSeq]): List[Token[ByteSeq]] = {
    // block must end at pos

    super.prependToContext(b.copy(value = b.value.moveEndToPos(pos)))
  }
}

object MinimalMemoryLexer {
  private def string2ByteSeq(str: String)(implicit codec: Codec): ByteSeq = {
    val v = codec.charSet.encode(str)
    ByteSeq(List(ByteRange(v.position(), v.limit())))
  }

  def apply(tokenizer: Tokenizer[ByteSeq], quotingString: String = "\"",
            lineSeparator: String = "\n",
            fieldDelimiter: String = ",",
            escapeString: Option[String] = Some("\\"))(implicit codec: Codec): MinimalMemoryLexer = {
    val map = Map(QuotationMark -> quotingString,
      FieldSeparator -> fieldDelimiter, RecordSeparator -> lineSeparator) ++ escapeString.map(es => Escape -> es).toMap
    new MinimalMemoryLexer(tokenizer, map.map({ case (t: Token[_], s) => (t, string2ByteSeq(s)) }))
  }
}
