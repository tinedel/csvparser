package ua.kyiv.tinedel.csvparser

import ua.kyiv.tinedel.csvparser.tokenizer.TokenizerException

case class ByteRange(from: Long, to: Long) {
  def moveEndToPos(pos: Long): ByteRange = {
    val len = to - from
    this.copy(from = pos - len, to = pos)
  }
}

case class ByteSeq(seq: List[ByteRange]) {
  def moveEndToPos(pos: Long): ByteSeq = {
    if (seq.size != 1) throw new TokenizerException("Minimal memory tokenizer created composite block")
    ByteSeq(seq.map(_.moveEndToPos(pos)))
  }

  def collapse(last: ByteRange, head: ByteRange): List[ByteRange] = {
    if (last.to == head.from)
      List(ByteRange(last.from, head.to))
    else
      List(last, head)
  }

  def +(other: ByteSeq): ByteSeq = {
    if (seq.isEmpty)
      other
    else if (other.seq.isEmpty)
      this
    else {
      ByteSeq(seq.init ++ collapse(seq.last, other.seq.head) ++ other.seq.tail)
    }
  }
}
