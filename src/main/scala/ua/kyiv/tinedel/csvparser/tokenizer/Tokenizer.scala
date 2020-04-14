package ua.kyiv.tinedel.csvparser.tokenizer

import java.nio.channels.ReadableByteChannel
import java.nio.charset.CharsetDecoder
import java.nio.{ByteBuffer, CharBuffer}

import scala.collection.immutable.Queue
import scala.io.Codec

/**
 * Any class able to iterate on tokens is Tokenizer
 *
 * @tparam T type of data which represent Block
 */
trait Tokenizer[T] extends Iterator[Token[T]]

/**
 * Wraps any exceptional situations happened during producing tokens
 *
 * @param message description of what went wrong
 */
class TokenizerException(message: String) extends RuntimeException(message)

/**
 * Generic tokenizer reading from provided byte channel and wrapping received data into tokens.
 *
 * Class provides generic nio channel based implementation of tokenizer. The main functionality is performed in
 * refillBuffersAndProcess private method which tries to read from channel, decode the byte buffer using Codec
 * and produce tokens of types corresponding to tokensTrie or wrap anything which do not fit in [[ua.kyiv.tinedel.csvparser.tokenizer.Block]]
 * using provided buildBlock callback
 *
 * Some mild thread safety is attempted, but haven't been precisely tested. Safer to consider non-threadsafe
 *
 * @param readableByteChannel supplies data. Theoretically any kind of readable data channel could be used.
 *                            Only FileChannel and ReadableByteChannel from ByteArrayInputStream were tested.
 * @param tokensTrie          [[ua.kyiv.tinedel.csvparser.tokenizer.Trie]] with Tokens
 * @param blockSize           block to try to read from channel at once. Tokenizer will create character and byte buffer
 *                            which are able to store data of this size.
 * @param buildBlock          callback provided by descendant to convert charBuffer content into Block token. MUST NOT MOVE position
 * @param codec               codec to make byte data into string. Can be configured to replace, report or ignore errors during decoding.
 *                            In case if errors are reported parsing will throw exception if malformed or unmappable byte sequence is found
 * @tparam T type of data which represent Block
 */
abstract class GenericTokenizer[T](val readableByteChannel: ReadableByteChannel,
                                   val tokensTrie: Trie[Token[Nothing]],
                                   val blockSize: Int = 512 * 1024,
                                   val buildBlock: (CharBuffer, Int, Int, Codec) => Block[T])(implicit codec: Codec)
  extends Tokenizer[T] {

  private val decoder: CharsetDecoder = codec.decoder
  private val buffer: ByteBuffer = ByteBuffer.allocate(blockSize)
  private val charBuffer: CharBuffer = CharBuffer.allocate((blockSize * codec.decoder.maxCharsPerByte()).round)

  // queue of what have been read so far. if empty need to refill
  @volatile private var queue: Queue[Token[T]] = Queue.empty
  // have we seen eof. if queue is empty and eof - file is completely processed
  @volatile private var eof = false

  private def refillBuffersAndProcess(): Boolean = {
    // something in the queue - no need to bother
    if (queue.nonEmpty) return true
    // no new info going to arrive
    if (eof) return false

    // fetch new portion of data
    val bytesRead = readableByteChannel.read(buffer)
    if (bytesRead == -1) {
      eof = true
      return false
    }

    // decode it
    buffer.flip()
    val result = decoder.decode(buffer, charBuffer, false)

    // helper method
    def cutAndQueueBlock(from: Int, to: Int): Unit = {
      if (to - from > 0) {
        charBuffer.position(from)
        queue = queue.enqueue(buildBlock(charBuffer, from, to, codec))
      }
    }

    // we might have exhausted one of the buffers - time to process charbuffer
    if (result.isOverflow || result.isUnderflow) {
      charBuffer.flip()
      var pos = charBuffer.position()
      // char buffer position will never be in the middle of the token - so starting over each time
      var trieQueue: Queue[(Int, Trie[Token[Nothing]])] = Queue.empty
      while (charBuffer.hasRemaining) {
        val char = charBuffer.get()
        val currentPos = charBuffer.position()

        // advance any tries we have
        val tries = trieQueue.map {
          case (p, t) => (p, t.matchChar(char))
        } :+
          // adding any trie which might have started with last read char
          (charBuffer.position() - 1, tokensTrie.matchChar(char))

        // have any of them resolved into token
        val tokenOpt = tries.find {
          case (_, Right(_)) => true
          case _ => false
        }

        // yes we found token!
        if (tokenOpt.isDefined) {
          val (tokenPos, Right(token)) = tokenOpt.get
          trieQueue = Queue.empty
          // cut whatever we encountered before start of the token and put to the output token queue
          cutAndQueueBlock(pos, tokenPos)
          // and token
          queue = queue.enqueue(token)
          // reset position after token and proceed
          pos = currentPos
          charBuffer.position(currentPos)
        } else {
          // get rid of any exhausted tries which failed to match any tokens
          trieQueue = tries.collect {
            case (i, Left(trie)) if trie.nonEmpty => (i, trie)
          }
        }
      }
      // at the end of char buffer there are parts of some potential tokens
      if (trieQueue.nonEmpty) {
        // find earliest candidate position
        val anomalyStart = trieQueue.map(_._1).min
        // cut whatever is for sure not a token and enqueue it
        cutAndQueueBlock(pos, anomalyStart)
        charBuffer.position(anomalyStart)
        trieQueue = Queue.empty
      } else {
        // whole rest of the buffer is not a token make a block and adjust position
        if (pos < charBuffer.limit()) {
          cutAndQueueBlock(pos, charBuffer.limit())
          charBuffer.position(charBuffer.limit())
        }
      }
      // make it ready to refill
      charBuffer.compact()

    } else {
      // if result of the decoding is not under or over fill - the incoming data are malformed
      // let the caller know
      throw new TokenizerException("Malformed input file or wrong encoding")
    }
    // make it ready to refill
    buffer.compact()
    true
  }

  /**
   * Returns true if input channel has more [[ua.kyiv.tinedel.csvparser.tokenizer.Token]]s
   */
  override def hasNext: Boolean = {
    synchronized {
      refillBuffersAndProcess()
    }
  }

  /**
   * Returns the next [[ua.kyiv.tinedel.csvparser.tokenizer.Token]] from input data
   */
  override def next(): Token[T] = {
    synchronized {
      if (!hasNext) throw new IllegalStateException("Iterator is empty")
      val (x, nq) = queue.dequeue
      queue = nq
      x
    }
  }
}
