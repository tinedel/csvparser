package ua.kyiv.tinedel.csvparser.immutable

import ua.kyiv.tinedel.csvparser.lexer.{Field, FieldBreak, Lexeme, RecordBreak}

import scala.annotation.tailrec

class ImmutableParser[T](z: T) {

  def composeRecord(record: Stream[Lexeme[T]]): Map[Int, T] = {
    @tailrec
    def composeRecordInt(r: Stream[Lexeme[T]], num: Int, seenField: Boolean, res: Map[Int, T]): Map[Int, T] = r match {
      case Stream.Empty if seenField => res
      case Stream.Empty => composeRecordInt(r, num + 1, seenField = true, res + (num -> z))
      case Field(v) #:: tail => composeRecordInt(tail, num + 1, seenField = true, res + (num -> v))
      case FieldBreak #:: tail if seenField => composeRecordInt(tail, num, seenField = false, res)
      case FieldBreak #:: tail => composeRecordInt(tail, num + 1, seenField = false, res + (num -> z))
    }

    composeRecordInt(record, 1, seenField = false, Map())
  }

  def records(stream: Stream[Lexeme[T]]): Stream[Map[Int, T]] = {
    if (stream.isEmpty) {
      Stream.empty
    } else {
      val (record, tail) = stream.span {
        case RecordBreak => false
        case _ => true
      }

      composeRecord(record) #:: (if (tail.nonEmpty) records(tail.drop(1)) else Stream.empty)
    }
  }

  def withHeader(stream: Stream[Lexeme[T]]): (Option[Map[Int, T]], Stream[Map[Int, T]]) = {
    val r = records(stream)
    (r.headOption, r.drop(1))
  }
}
