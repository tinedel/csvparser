package ua.kyiv.tinedel.csvparser

import ua.kyiv.tinedel.csvparser.tokenizer.TokenizerException

/**
 * Represents range of bytes starting at position from and ending at to
 *
 * @param from start of the range
 * @param to   end of the range
 */
case class ByteRange(from: Long, to: Long) {

  /**
   * Realligns the range the way so it ends at positions pos
   *
   * @param pos new end of the range
   * @return new ByteRange of the same length, but ending at the specified position
   */
  def moveEndToPos(pos: Long): ByteRange = {
    val len = to - from
    this.copy(from = pos - len, to = pos)
  }
}

/**
 * Represent sequence of ByteRanges
 */
case class ByteSeq(seq: List[ByteRange]) {

  /**
   * ALlows to move byte sequence consisting of one range so that the new end for it is at given position
   */
  def moveEndToPos(pos: Long): ByteSeq = {
    if (seq.size != 1) throw new TokenizerException("Minimal memory tokenizer created composite block")
    ByteSeq(seq.map(_.moveEndToPos(pos)))
  }

  /**
   * Given last = ByteRange(start, mid) and head = ByteRange(mid, end) returns List(ByteRange(start, end))
   * List(last, head) is returned otherwise
   */
  def collapse(last: ByteRange, head: ByteRange): List[ByteRange] = {
    if (last.to == head.from)
      List(ByteRange(last.from, head.to))
    else
      List(last, head)
  }

  /**
   * Adds up two byte sequences. If last element of this is adjacent to first element of other - they are merged together
   *
   * @param other ByteSeq to add to the current
   * @return potentially new bytes sequence covering the same bytes as this and other
   */
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
