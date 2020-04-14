package ua.kyiv.tinedel.csvparser.lexer

import ua.kyiv.tinedel.csvparser.tokenizer._

import scala.annotation.tailrec
import scala.collection.immutable.Queue

/**
 * Any iterator providing lexemes are lexer
 */
trait Lexer[T] extends Iterator[Lexeme[T]]

/**
 * Thrown when lexer ended up in the unexpected state
 *
 * @param message describes what went wrong
 */
class LexerException(message: String) extends RuntimeException(message)


/**
 * Generic lexer consuming tokens from tokenizer and build lexemes.
 *
 * Lexer using final state automate when considering what lexeme it should build from token's stream
 *
 * Slightly simplified graph of FSA used in lexer <img src="src/main/doc-resources/fsa.png" />
 * Does not include escaped block and escaped quoted block states which are essentially capture the next
 * token and convert it into block before returning to block or quoted block states
 *
 * The main method is the tryFetch which is feeding the tokens into FSA and calling itself recursively until at least one
 * lexeme is found, or end of file or FinalState is reached.
 *
 * Some threadsafety and synchronization is attempted but not guaranteed
 *
 * @param tokenizer iterator providing Tokens
 * @param tokens    map of tokens to field content type used when lexer finds out it needs to add literal value of token in field
 * @param z         empty value corresponding to the data type
 * @param concat    way to merge values of data type together
 * @tparam T type contained in tokens and to be produced in fields
 */
class GenericCSVLexer[T](val tokenizer: Tokenizer[T],
                         val tokens: Map[Token[_], T],
                         val z: T,
                         val concat: (T, T) => T) extends Lexer[T] {

  /**
   * Stores current state and accumulated blocks waiting to form a field
   *
   * @param buffer reversed list of blocks to cut costs
   * @param state  current stae
   */
  case class LexerContext(buffer: List[Token[T]], state: LexerState)

  /**
   * Represent state of the FSA which can consume next token and return any found lexemes and the new state of the FSA
   * All of the states are stateless and thus represented as case objects
   */
  sealed trait LexerState {
    /**
     * Always reads next from tokenizer and MUST finish processing and switch to the FinalState if tokenizer is empty
     *
     * @return List of any found lexemes in the same order and the new State of the FSA
     */
    def consume(): (List[Lexeme[T]], LexerContext)
  }

  /**
   * Helper function to go through reversed list of found blocks which need to be glued together to a field.
   *
   * @param tokens reversed blocks put into buffer e.g. when reading group of tokens all of which are blocks.
   * @return Field with content of the blocks glued in the correct order
   */
  private def concatBlocks(tokens: List[Token[T]]): Field[T] = {
    tokens.foldLeft(Field[T](z)) {
      case (Field(a), Block(v)) => Field(concat(v, a))
      case _ => throw new LexerException("We met a non block where it shouldn't have bin")
    }
  }

  /**
   * @inheritdoc
   */
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

  /**
   * @inheritdoc
   */
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

  /**
   * @inheritdoc
   */
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

  /**
   * @inheritdoc
   */
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

  /**
   * Adds found block in front of the buffer, can be overriden if descendant needs to react when block is put in the buffer
   * Which means it will for sure be added to a field, as blocks from buffer are never discarded
   *
   * @param b block to add
   * @return buffer with the block added in front
   */
  protected def prependToContext(b: Block[T]): List[Token[T]] = {
    b :: context.buffer
  }

  /**
   * @inheritdoc
   */
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

  /**
   * @inheritdoc
   */
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

  /**
   * @inheritdoc
   */
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

  /**
   * @inheritdoc
   */
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

  /**
   * Reads next token from tokenizer. All of the block read operations are going through this method, so children could
   * override it if the need to know when the token is read from the tokenizer
   *
   * @return next token
   */
  protected def nextToken: Token[T] = {
    val res = tokenizer.next()
    // TODO: think of proper way to debug final state automate
    // if (1 == 2) log = log.enqueue(res)
    res
  }

  /**
   * @inheritdoc
   */
  case object FinalState extends LexerState {
    override def consume(): (List[Lexeme[T]], LexerContext) = throw new LexerException("Should not consume from final state")
  }

  // lexemes identified in the tokens read from tokenizer and not yet consumed by parser
  // if the queue is empty tryFetch() will be called
  @volatile private var lexemes: Queue[Lexeme[T]] = Queue.empty
  // current FSA context - accumulated blocks and current state
  @volatile private var context: LexerContext = LexerContext(List(), InitialState)

  // uncomment if need to debug FSA
  // @volatile private var log = Queue[Token[T]]()

  /**
   * Tries to get the next lexeme(s) from tokenizer by running FSA with the context. Could be thread-safe, but not guaranteed
   *
   * @return true if more lexemes available
   */
  @tailrec
  private def tryFetch(): Boolean = {
    synchronized {
      if (lexemes.nonEmpty) return true
      if (tokenizer.isEmpty && context.state == FinalState) return false

      val (foundLexemes, nextContext) = context.state.consume()
      // compact buffer to avoid keeping a lot of unnecessary blocks
      // while taking a very small amount of memory, it is still a waste to keep series of blocks in the buffer
      // if only one will be needed. Useful for files with extremely long fields
      // or with extra complex quoting/escaping
      if (nextContext.buffer.size > 10) {
        context = nextContext.copy(buffer = List(Block(concatBlocks(nextContext.buffer).value)))
      } else {
        context = nextContext
      }
      lexemes = lexemes.enqueue(foundLexemes)
    }

    tryFetch()
  }

  /**
   * Returns true if more lexemes are available.
   *
   * Thread safety attempted, but not guaranteed
   */
  override def hasNext: Boolean = {
    tryFetch()
  }

  /**
   * Returns next lexem if available, throws exception if attempted when hasNext is false
   *
   * Thread-safety is attempted but not guaranteed
   *
   * @return next lexem
   */
  override def next(): Lexeme[T] = {
    if (!hasNext) throw new IllegalStateException("Iterator is empty")
    synchronized {
      val (res, nq) = lexemes.dequeue
      lexemes = nq

      res
    }
  }
}
