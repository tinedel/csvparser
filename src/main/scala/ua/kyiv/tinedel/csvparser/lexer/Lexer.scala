package ua.kyiv.tinedel.csvparser.lexer

import ua.kyiv.tinedel.csvparser.tokenizer._

import scala.annotation.tailrec
import scala.collection.immutable.Queue

trait Lexer[T] extends Iterator[Lexeme[T]] {

}

class LexerException(message: String) extends RuntimeException(message)

class GenericCSVLexer[T](val tokenizer: Tokenizer[T],
                         val tokens: Map[Token[_], T],
                         val z: T,
                         val concat: (T, T) => T) extends Lexer[T] {

  case class LexerContext(buffer: List[Token[T]], state: LexerState)

  sealed trait LexerState {
    def consume(): (List[Lexeme[T]], LexerContext)
  }

  def concatBlocks(tokens: List[Token[T]]): Field[T] = {
    tokens.foldLeft(Field[T](z)) {
      case (Field(a), Block(v)) => Field(concat(v, a))
      case _ => throw new LexerException("We met block where it shouldn't have bin")
    }
  }

  case object InitialState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = {
      assert(context.buffer.isEmpty)
      if (tokenizer.isEmpty) (Nil, context.copy(state = FinalState)) else {
        nextToken match {
          case b@Block(_) => (Nil, LexerContext(buffer = prependToContext(b), BlockState))
          case QuotationMark => (Nil, context.copy(state = MaybeQuotedState))
          case FieldSeparator => (List(FieldBreak), context)
          case RecordSeparator => (List(RecordBreak), context)
          case Escape => (Nil, context.copy(state = EscapeState))
        }
      }
    }
  }

  case object EscapeState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = {
      if (tokenizer.isEmpty) {
        (List(concatBlocks(context.buffer)), LexerContext(List.empty, state = FinalState))
      } else {
        nextToken match {
          case b@Block(_) => (Nil, LexerContext(buffer = prependToContext(b), BlockState))
          case t: Token[_] => (Nil, context.copy(buffer = prependToContext(Block(tokens(t))), BlockState))
        }
      }
    }
  }

  case object QuotedEscapeState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = {
      if (tokenizer.isEmpty) {
        (List(concatBlocks(context.buffer)), LexerContext(List.empty, state = FinalState))
      } else {
        nextToken match {
          case b@Block(_) => (Nil, LexerContext(buffer = prependToContext(b), BlockState))
          case t: Token[_] => (Nil, context.copy(buffer = prependToContext(Block(tokens(t))), QuotedState))
        }
      }
    }
  }

  case object BlockState extends LexerState {

    override def consume(): (List[Lexeme[T]], LexerContext) = {
      if (tokenizer.isEmpty) {
        (List(concatBlocks(context.buffer)), LexerContext(List.empty, state = FinalState))
      } else {
        nextToken match {
          case b@Block(_) => (Nil, context.copy(buffer = prependToContext(b)))
          case QuotationMark => (Nil, context.copy(state = MaybeQuotedState))
          case FieldSeparator =>
            (if (context.buffer.nonEmpty) List(concatBlocks(context.buffer), FieldBreak) else List(FieldBreak),
              LexerContext(List.empty, InitialState))
          case RecordSeparator =>
            (if (context.buffer.nonEmpty) List(concatBlocks(context.buffer), RecordBreak) else List(RecordBreak),
              LexerContext(List.empty, InitialState))
          case Escape => (Nil, context.copy(state = EscapeState))
        }
      }
    }
  }

  protected def prependToContext(b: Block[T]): List[Token[T]] = {
    b :: context.buffer
  }

  case object MaybeQuotedState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = {
      if (tokenizer.isEmpty) {
        (Nil, context.copy(state = QuotedState))
      } else {
        nextToken match {
          case b@Block(_) => (Nil, context.copy(prependToContext(b), QuotedState))
          case QuotationMark => (Nil, context.copy(state = StillNotSureIfQuotedState))
          case Escape => (Nil, context.copy(state = QuotedEscapeState))
          case t: Token[_] => (Nil, context.copy(prependToContext(Block(tokens(t))), QuotedState))
        }
      }
    }
  }

  case object StillNotSureIfQuotedState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = {
      if (tokenizer.isEmpty) {
        (Nil, context.copy(buffer = prependToContext(Block(z)), state = BlockState))
      } else {
        nextToken match {
          case b@Block(_) => (Nil, context.copy(prependToContext(b), BlockState))
          case QuotationMark => (Nil, context.copy(prependToContext(Block(tokens(QuotationMark))), QuotedState))
          case Escape => (Nil, context.copy(state = EscapeState))
          case FieldSeparator =>
            (if (context.buffer.nonEmpty) List(concatBlocks(context.buffer), FieldBreak) else List(FieldBreak),
              LexerContext(List.empty, InitialState))
          case RecordSeparator =>
            (if (context.buffer.nonEmpty) List(concatBlocks(context.buffer), RecordBreak) else List(RecordBreak),
              LexerContext(List.empty, InitialState))
        }
      }
    }
  }

  case object MaybeBlockState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = {
      if (tokenizer.isEmpty) {
        (Nil, context.copy(state = BlockState))
      } else {
        nextToken match {
          case b@Block(_) => (Nil, context.copy(prependToContext(b), BlockState))
          case QuotationMark => (Nil, context.copy(prependToContext(Block(tokens(QuotationMark))), QuotedState))
          case FieldSeparator =>
            (if (context.buffer.nonEmpty) List(concatBlocks(context.buffer), FieldBreak) else List(FieldBreak),
              LexerContext(List.empty, InitialState))
          case RecordSeparator =>
            (if (context.buffer.nonEmpty) List(concatBlocks(context.buffer), RecordBreak) else List(RecordBreak),
              LexerContext(List.empty, InitialState))
          case Escape => (Nil, context.copy(state = EscapeState))
        }
      }
    }
  }

  case object QuotedState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = {
      if (tokenizer.isEmpty) {
        (List(concatBlocks(context.buffer)), LexerContext(List.empty, state = FinalState))
      } else {
        nextToken match {
          case b@Block(_) => (Nil, context.copy(buffer = prependToContext(b)))
          case QuotationMark => (Nil, context.copy(state = MaybeBlockState))
          case Escape => (Nil, context.copy(state = QuotedEscapeState))
          case t: Token[_] => (Nil, context.copy(buffer = prependToContext(Block(tokens(t)))))
        }
      }
    }
  }

  protected def nextToken: Token[T] = {
    val res = tokenizer.next()
    // TODO: think of proper way to debug final state automate
    // if (1 == 2) log = log.enqueue(res)
    res
  }

  case object FinalState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = throw new LexerException("Should not consume from final state")
  }

  @volatile private var lexemes: Queue[Lexeme[T]] = Queue.empty
  @volatile private var context: LexerContext = LexerContext(List(), InitialState)
  @volatile private var log = Queue[Token[T]]()

  @tailrec
  private def tryFetch(): Boolean = {
    synchronized {
      if (lexemes.nonEmpty) return true
      if (tokenizer.isEmpty && context.state == FinalState) return false

      val (foundLexemes, nextContext) = context.state.consume()
      // compact buffer to avoid keeping a lot of unnecessary blocks
      if (nextContext.buffer.size > 10) {
        context = nextContext.copy(buffer = List(Block(concatBlocks(nextContext.buffer).value)))
      } else {
        context = nextContext
      }
      lexemes = lexemes.enqueue(foundLexemes)
    }

    tryFetch()
  }

  override def hasNext: Boolean = {
    tryFetch()
  }

  override def next(): Lexeme[T] = {
    if (!hasNext) throw new IllegalStateException("Iterator is empty")
    synchronized {
      val (res, nq) = lexemes.dequeue
      lexemes = nq

      res
    }
  }
}
