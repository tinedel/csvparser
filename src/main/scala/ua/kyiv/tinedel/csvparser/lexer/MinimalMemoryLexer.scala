package ua.kyiv.tinedel.csvparser.lexer

import ua.kyiv.tinedel.csvparser.tokenizer._
import ua.kyiv.tinedel.csvparser.{ByteRange, ByteSeq}

import scala.io.Codec

/**
 * Experimental lexer operating on [[ua.kyiv.tinedel.csvparser.ByteSeq]]tokenizer allowing to avoid keeping strings and
 * storing only ByteRange to be able to read needed bytes later on
 *
 * @param tokenizer iterator providing Tokens
 * @param tokens    map of tokens to field content type used when lexer finds out it needs to add literal value of token in field
 */
class MinimalMemoryLexer private(tokenizer: Tokenizer[ByteSeq], tokens: Map[Token[_], ByteSeq])
  extends GenericCSVLexer[ByteSeq](tokenizer, tokens, ByteSeq(List()), (a, b) => a + b) {

  private var pos = 0L

  /**
   * Overridden to track end position of the last read token
   *
   * @return next token
   */
  override def nextToken: Token[ByteSeq] = {
    val res = super.nextToken
    pos = res match {
      case Block(value) => pos + value.seq.head.to - value.seq.head.from
      case t: Token[_] => pos + tokens(t).seq.head.to - tokens(t).seq.head.from
    }
    res
  }

  /**
   * As it is not guaranteed for blocks to have correct start and end positions, only correct length,
   * so this methods tracks block creation and the only possibility when block is added to buffer is when
   * actual end position of token read last is at the end of block.
   *
   * @param b block to add
   * @return buffer with the block added in front
   */
  override def prependToContext(b: Block[ByteSeq]): List[Token[ByteSeq]] = {
    // block must end at pos

    super.prependToContext(b.copy(value = b.value.moveEndToPos(pos)))
  }
}

/**
 * @see [[ua.kyiv.tinedel.csvparser.lexer.SimpleLexer]]
 */
object MinimalMemoryLexer {
  private def string2ByteSeq(str: String)(implicit codec: Codec): ByteSeq = {
    val v = codec.charSet.encode(str)
    ByteSeq(List(ByteRange(v.position(), v.limit())))
  }

  /**
   * @see [[ua.kyiv.tinedel.csvparser.lexer.SimpleLexer]]
   */
  def apply(tokenizer: Tokenizer[ByteSeq], quotingString: String = "\"",
            recordSeparator: String = "\n",
            fieldSeparator: String = ",",
            escapeString: Option[String] = Some("\\"))(implicit codec: Codec): MinimalMemoryLexer = {
    val map = Map(QuotationMark -> quotingString,
      FieldSeparator -> fieldSeparator, RecordSeparator -> recordSeparator) ++ escapeString.map(es => Escape -> es).toMap
    new MinimalMemoryLexer(tokenizer, map.map({ case (t: Token[_], s) => (t, string2ByteSeq(s)) }))
  }
}
