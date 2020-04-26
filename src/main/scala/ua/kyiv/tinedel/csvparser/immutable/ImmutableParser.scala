package ua.kyiv.tinedel.csvparser.immutable

import java.io.InputStream

import ua.kyiv.tinedel.csvparser.lexer.{Field, FieldBreak, Lexeme, RecordBreak}

import scala.annotation.tailrec
import scala.io.{Codec, Source}

/**
 * Provides parsing capability in the immutable fashion convering the input stream of lexemes to the stream of records
 * represented as Map of field numbers to field contents. Parametrized to support non-String representation of the field
 * data.
 *
 * @param z zero value of the chosen type. "", or null is meaningful for strings
 * @tparam T type for the field values
 */
class ImmutableParser[T](z: T) {

  private def composeRecord(record: Stream[Lexeme[T]]): Map[Int, T] = {
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

  /**
   * Produces stream of records
   *
   * @param stream stream of lexemes
   * @return stream of records
   */
  final def records(stream: Stream[Lexeme[T]]): Stream[Map[Int, T]] = {
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

  /**
   * Produces tuple of header record, and the stream of the body records.
   *
   * @param stream stream of lexemes
   * @return tuple of header record and the stream of body records
   */
  final def withHeader(stream: Stream[Lexeme[T]]): (Option[Map[Int, T]], Stream[Map[Int, T]]) = {
    val r = records(stream)
    (r.headOption, r.drop(1))
  }
}

/**
 * Convenient shortcuts to produce stream of records directly from input stream.
 */
object ImmutableParser {
  /**
   * Produces stream of records from csv input stream
   *
   * @param is              open stream. Not closed.
   * @param quotingString   string to quote fields. By default - double quotes (")
   * @param recordSeparator string to separate records. By default - new line (\n)
   * @param fieldSeparator  string to separate fields. By default - comma (,)
   * @param escapeString    optional string to escape next character. By default disabled.
   * @param codec           imlicit codec to fine-tune reading from input stream
   * @return Stream of records in form of map from field number to field value
   */
  def csv(is: InputStream, quotingString: String = "\"",
          recordSeparator: String = "\n",
          fieldSeparator: String = ",",
          escapeString: Option[String] = Some("\\"))(implicit codec: Codec): Stream[Map[Int, String]] = {
    new ImmutableParser("").records(
      ImmutableLexer(quotingString, recordSeparator, fieldSeparator, escapeString).lexemesStream(
        ImmutableTokenizer(quotingString, recordSeparator, fieldSeparator, escapeString)
          .tokenize(Source.fromInputStream(is))
      )
    )
  }

  /**
   * Produces stream of records from csv input stream with header record
   *
   * @param is              open stream. Not closed.
   * @param quotingString   string to quote fields. By default - double quotes (")
   * @param recordSeparator string to separate records. By default - new line (\n)
   * @param fieldSeparator  string to separate fields. By default - comma (,)
   * @param escapeString    optional string to escape next character. By default disabled.
   * @param codec           imlicit codec to fine-tune reading from input stream
   * @return Pair of header record and stream of records for body in form of map from field number to field value
   */
  def csvWithHeader(is: InputStream, quotingString: String = "\"",
                    recordSeparator: String = "\n",
                    fieldSeparator: String = ",",
                    escapeString: Option[String] = Some("\\"))(implicit codec: Codec):
  (Option[Map[Int, String]], Stream[Map[Int, String]]) = {

    new ImmutableParser("").withHeader(
      ImmutableLexer(quotingString, recordSeparator, fieldSeparator, escapeString).lexemesStream(
        ImmutableTokenizer(quotingString, recordSeparator, fieldSeparator, escapeString)
          .tokenize(Source.fromInputStream(is))
      )
    )

  }
}
