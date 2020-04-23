package ua.kyiv.tinedel.csvparser.immutable

import ua.kyiv.tinedel.csvparser.lexer._
import ua.kyiv.tinedel.csvparser.tokenizer._

import scala.collection.immutable.Stream.Empty


class ImmutableLexer[T](val tokenMap: Map[Token[_], T],
                        val z: T,
                        val concat: (T, T) => T) extends Lexer[T] {


  final override def lexemesStream(tokens: Stream[Token[T]]): Stream[Lexeme[T]] = {
    InitialState().nextStream(tokens)
      .takeWhile({
        case FinalState(lexemes) => lexemes.nonEmpty
        case _ => true
      })
      .collect({
        case InitialState(_, lexemes) if lexemes.nonEmpty => lexemes
        case FinalState(lexemes) if lexemes.nonEmpty => lexemes
      }).flatten
  }

  sealed trait LexerState {

    def buffer: List[Block[T]]

    def lexemes: List[Lexeme[T]] = Nil

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

    val fieldSeparator: PartialFunction[Stream[Token[T]], LexerState] = {
      case FieldSeparator #:: tail => InitialState(Nil, concatBlocks(buffer) :+ FieldBreak)
    }

    val recordSeparator: PartialFunction[Stream[Token[T]], LexerState] = {
      case RecordSeparator #:: tail => InitialState(Nil, concatBlocks(buffer) :+ RecordBreak)
    }

    def nextState(tokens: Stream[Token[T]]): LexerState

    final def nextStream(tokens: Stream[Token[T]]): Stream[LexerState] = {
      val ns = nextState(tokens)
      ns match {
        case FinalState(_) => ns #:: Empty
        case _ => ns #:: ns.nextStream(tokens.tail)
      }
    }
  }

  /**
   * @inheritdoc
   */
  case class InitialState(buffer: List[Block[T]] = Nil, override val lexemes: List[Lexeme[T]] = Nil) extends LexerState {
    override def nextState(tokens: Stream[Token[T]]): LexerState = tokens matchComposite (
      emptyStream orElse fieldSeparator orElse recordSeparator orElse {
        case (b@Block(_)) #:: _ => InitialState(b :: buffer, Nil)
        case QuotationMark #:: _ => MaybeQuotedState(buffer)
        case Escape #:: _ => EscapeState(buffer)
      })
  }

  /**
   * @inheritdoc
   */
  case class EscapeState(buffer: List[Block[T]]) extends LexerState {
    override def nextState(tokens: Stream[Token[T]]): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: _ => InitialState(b :: buffer, Nil)
      case (t: Token[_]) #:: _ => InitialState(Block(tokenMap(t)) :: buffer, Nil)
    }
  }

  /**
   * @inheritdoc
   */
  case class QuotedEscapeState(buffer: List[Block[T]]) extends LexerState {
    override def nextState(tokens: Stream[Token[T]]): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: _ => QuotedState(b :: buffer)
      case (t: Token[_]) #:: _ => QuotedState(Block(tokenMap(t)) :: buffer)
    }
  }

  /**
   * @inheritdoc
   */
  case class MaybeQuotedState(buffer: List[Block[T]]) extends LexerState {
    override def nextState(tokens: Stream[Token[T]]): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: _ => QuotedState(b :: buffer)
      case QuotationMark #:: _ => StillNotSureIfQuotedState(buffer)
      case Escape #:: _ => QuotedEscapeState(buffer)
      case (t: Token[_]) #:: _ => QuotedState(Block(tokenMap(t)) :: buffer)
    }
  }

  /**
   * @inheritdoc
   */
  case class StillNotSureIfQuotedState(buffer: List[Block[T]]) extends LexerState {
    override def nextState(tokens: Stream[Token[T]]): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(Block(z) :: buffer))
      case (b@Block(_)) #:: _ => InitialState(b :: buffer, Nil)
      case QuotationMark #:: _ => QuotedState(Block(tokenMap(QuotationMark)) :: buffer)
      case Escape #:: _ => EscapeState(buffer)
      case FieldSeparator #:: _ => InitialState(Nil, concatBlocks(buffer) :+ FieldBreak)
      case RecordSeparator #:: _ => InitialState(Nil, concatBlocks(buffer) :+ RecordBreak)
    }
  }


  /**
   * @inheritdoc
   */
  case class MaybeBlockState(buffer: List[Block[T]]) extends LexerState {
    override def nextState(tokens: Stream[Token[T]]): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: _ => InitialState(b :: buffer, Nil)
      case QuotationMark #:: _ => QuotedState(Block(tokenMap(QuotationMark)) :: buffer)
      case FieldSeparator #:: _ => InitialState(Nil, concatBlocks(buffer) :+ FieldBreak)
      case RecordSeparator #:: _ => InitialState(Nil, concatBlocks(buffer) :+ RecordBreak)
      case Escape #:: _ => EscapeState(buffer)
    }
  }

  /**
   * @inheritdoc
   */
  case class QuotedState(buffer: List[Block[T]]) extends LexerState {
    override def nextState(tokens: Stream[Token[T]]): LexerState = tokens match {
      case Stream.Empty => FinalState(concatBlocks(buffer))
      case (b@Block(_)) #:: _ => QuotedState(b :: buffer)
      case QuotationMark #:: _ => MaybeBlockState(buffer)
      case Escape #:: _ => QuotedEscapeState(buffer)
      case (t: Token[_]) #:: _ => QuotedState(Block(tokenMap(t)) :: buffer)

    }
  }

  /**
   * @inheritdoc
   */
  case class FinalState(override val lexemes: List[Lexeme[T]]) extends LexerState {
    override def nextState(tokens: Stream[Token[T]]): LexerState = FinalState(Nil)

    override def buffer: List[Block[T]] = Nil
  }

}

/**
 * Factory object producing ImmutableLexers
 */
object ImmutableLexer {
  /**
   * Creates [[ImmutableLexer]] from provided parameters
   *
   * @param quotingString   string for quoting fields
   * @param recordSeparator string separating records
   * @param fieldSeparator  string separating fields
   * @param escapeString    optional escape string to enable escaping support which is not standard
   * @return configured [[ImmutableLexer]]
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
