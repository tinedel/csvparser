package ua.kyiv.tinedel.csvparser.lexer.immutable

import ua.kyiv.tinedel.csvparser.lexer._
import ua.kyiv.tinedel.csvparser.tokenizer._


class ImmutableLexer[T](val tokenMap: Map[Token[_], T],
                        val z: T,
                        val concat: (T, T) => T) extends Lexer[T] {


  override def lexemesStream(tokens: Stream[Token[T]]): Stream[Lexeme[T]] = {
    Stream.iterate[LexerState](InitialState(tokens))(_.next())
      .takeWhile({
        case FinalState(lexemes) => lexemes.nonEmpty
        case _ => true
      })
      .collect({
        case InitialState(_, _, lexemes) if lexemes.nonEmpty => lexemes
        case FinalState(lexemes) if lexemes.nonEmpty => lexemes
      }).flatten
  }

  sealed trait LexerState {
    def tokens: Stream[Token[T]]

    def buffer: List[Block[T]]

    def lexemes: List[Lexeme[T]]

    protected def concatBlocks(tokens: List[Block[T]]): List[Field[T]] = {
      tokens
        .map {
          case Block(v) => Field(v)
        }
        .reduceLeftOption[Field[T]] {
          case (Field(a), Field(v)) => Field(concat(v, a))
        }
    }.toList

    val emptyStream: PartialFunction[Stream[Token[T]], LexerState] = {
      case Stream.Empty => FinalState(concatBlocks(buffer))
    }

    def next(): LexerState
  }

  /**
   * @inheritdoc
   */
  case class InitialState(tokens: Stream[Token[T]], buffer: List[Block[T]] = Nil, lexemes: List[Lexeme[T]] = Nil) extends LexerState {
    override def next(): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: tail => InitialState(tail, b :: buffer, Nil)
      case QuotationMark #:: tail => MaybeQuotedState(tail, buffer, Nil)
      case FieldSeparator #:: tail => InitialState(tail, Nil, concatBlocks(buffer) :+ FieldBreak)
      case RecordSeparator #:: tail => InitialState(tail, Nil, concatBlocks(buffer) :+ RecordBreak)
      case Escape #:: tail => EscapeState(tail, buffer, Nil)
    }
  }

  /**
   * @inheritdoc
   */
  case class EscapeState(tokens: Stream[Token[T]], buffer: List[Block[T]], lexemes: List[Lexeme[T]]) extends LexerState {
    override def next(): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: tail => InitialState(tail, b :: buffer, Nil)
      case (t: Token[_]) #:: tail => InitialState(tail, Block(tokenMap(t)) :: buffer, Nil)
    }
  }

  /**
   * @inheritdoc
   */
  case class QuotedEscapeState(tokens: Stream[Token[T]], buffer: List[Block[T]], lexemes: List[Lexeme[T]]) extends LexerState {
    override def next(): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: tail => QuotedState(tail, b :: buffer, Nil)
      case (t: Token[_]) #:: tail => QuotedState(tail, Block(tokenMap(t)) :: buffer, Nil)
    }
  }

  /**
   * @inheritdoc
   */
  case class MaybeQuotedState(tokens: Stream[Token[T]], buffer: List[Block[T]], lexemes: List[Lexeme[T]]) extends LexerState {
    override def next(): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: tail => QuotedState(tail, b :: buffer, Nil)
      case QuotationMark #:: tail => StillNotSureIfQuotedState(tail, buffer, Nil)
      case Escape #:: tail => QuotedEscapeState(tail, buffer, Nil)
      case (t: Token[_]) #:: tail => QuotedState(tail, Block(tokenMap(t)) :: buffer, Nil)
    }
  }

  /**
   * @inheritdoc
   */
  case class StillNotSureIfQuotedState(tokens: Stream[Token[T]], buffer: List[Block[T]], lexemes: List[Lexeme[T]]) extends LexerState {
    override def next(): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(Block(z) :: buffer))
      case (b@Block(_)) #:: tail => InitialState(tail, b :: buffer, Nil)
      case QuotationMark #:: tail => QuotedState(tail, Block(tokenMap(QuotationMark)) :: buffer, Nil)
      case Escape #:: tail => EscapeState(tail, buffer, Nil)
      case FieldSeparator #:: tail => InitialState(tail, Nil, concatBlocks(buffer) :+ FieldBreak)
      case RecordSeparator #:: tail => InitialState(tail, Nil, concatBlocks(buffer) :+ RecordBreak)
    }
  }


  /**
   * @inheritdoc
   */
  case class MaybeBlockState(tokens: Stream[Token[T]], buffer: List[Block[T]], lexemes: List[Lexeme[T]]) extends LexerState {
    override def next(): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: tail => InitialState(tail, b :: buffer, Nil)
      case QuotationMark #:: tail => QuotedState(tail, Block(tokenMap(QuotationMark)) :: buffer, Nil)
      case FieldSeparator #:: tail => InitialState(tail, Nil, concatBlocks(buffer) :+ FieldBreak)
      case RecordSeparator #:: tail => InitialState(tail, Nil, concatBlocks(buffer) :+ RecordBreak)
      case Escape #:: tail => EscapeState(tail, buffer, Nil)
    }
  }

  /**
   * @inheritdoc
   */
  case class QuotedState(tokens: Stream[Token[T]], buffer: List[Block[T]], lexemes: List[Lexeme[T]]) extends LexerState {
    override def next(): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: tail => QuotedState(tail, b :: buffer, Nil)
      case QuotationMark #:: tail => MaybeBlockState(tail, buffer, Nil)
      case Escape #:: tail => QuotedEscapeState(tail, buffer, Nil)
      case (t: Token[_]) #:: tail => QuotedState(tail, Block(tokenMap(t)) :: buffer, Nil)

    }
  }

  /**
   * @inheritdoc
   */
  case class FinalState(lexemes: List[Lexeme[T]]) extends LexerState {
    override def next(): LexerState = FinalState(Nil)

    override def tokens: Stream[Token[T]] = Stream.Empty

    override def buffer: List[Block[T]] = Nil
  }

}

/**
 * Factory object producing SimpleLexers
 */
object ImmutableLexer {
  /**
   * Creates [[ua.kyiv.tinedel.csvparser.lexer.immutable.ImmutableLexer]] from provided parameters
   *
   * @param quotingString   string for quoting fields
   * @param recordSeparator string separating records
   * @param fieldSeparator  string separating fields
   * @param escapeString    optional escape string to enable escaping support which is not standard
   * @return configured [[ua.kyiv.tinedel.csvparser.lexer.immutable.ImmutableLexer]]
   */
  def apply(quotingString: String = "\"",
            recordSeparator: String = "\n",
            fieldSeparator: String = ",",
            escapeString: Option[String] = Some("\\")): ImmutableLexer[String] = {
    new ImmutableLexer[String](Map(QuotationMark -> quotingString,
      FieldSeparator -> fieldSeparator, RecordSeparator -> recordSeparator) ++ escapeString.map(es => Escape -> es).toMap,
      "", _ + _)
  }
}
