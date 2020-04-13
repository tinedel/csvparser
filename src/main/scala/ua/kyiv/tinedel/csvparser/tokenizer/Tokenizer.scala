package ua.kyiv.tinedel.csvparser.tokenizer

import java.nio.channels.ReadableByteChannel
import java.nio.charset.CharsetDecoder
import java.nio.{ByteBuffer, CharBuffer}

import scala.collection.immutable.Queue
import scala.io.Codec

trait Tokenizer[T] extends Iterator[Token[T]]

class TokenizerException(message: String) extends RuntimeException(message)

abstract class GenericTokenizer[T](val file: ReadableByteChannel,
                                   val tokensTrie: Trie[Token[Nothing]],
                                   val blockSize: Int = 512 * 1024,
                                   val buildBlock: (CharBuffer, Int, Int, Codec) => Block[T])(implicit codec: Codec)
  extends Tokenizer[T]
    with Iterator[Token[T]] {

  private val decoder: CharsetDecoder = codec.decoder
  private val buffer: ByteBuffer = ByteBuffer.allocate(blockSize)
  private val charBuffer: CharBuffer = CharBuffer.allocate((blockSize * codec.decoder.maxCharsPerByte()).round)

  @volatile private var queue: Queue[Token[T]] = Queue.empty
  @volatile private var eof = false

  private def refillBuffersAndProcess(): Boolean = {
    if (queue.nonEmpty) return true
    if (eof) return false
    val bytesRead = file.read(buffer)
    if (bytesRead == -1) {
      eof = true
      return false
    }

    buffer.flip()
    val result = decoder.decode(buffer, charBuffer, false)

    def cutAndQueueBlock(from: Int, to: Int): Unit = {
      if (to - from > 0) {
        charBuffer.position(from)
        queue = queue.enqueue(buildBlock(charBuffer, from, to, codec))
      }
    }

    if (result.isOverflow || result.isUnderflow) {
      charBuffer.flip()
      var pos = charBuffer.position()
      var trieQueue: Queue[(Int, Trie[Token[Nothing]])] = Queue.empty
      while (charBuffer.hasRemaining) {
        val char = charBuffer.get()
        val currentPos = charBuffer.position()

        val tries = trieQueue.map {
          case (p, t) => (p, t.matchChar(char))
        } :+ (charBuffer.position() - 1, tokensTrie.matchChar(char))

        val tokenOpt = tries.find {
          case (_, Right(_)) => true
          case _ => false
        }

        if (tokenOpt.isDefined) {
          val (tokenPos, Right(token)) = tokenOpt.get
          trieQueue = Queue.empty
          cutAndQueueBlock(pos, tokenPos)
          queue = queue.enqueue(token)
          pos = currentPos
          charBuffer.position(currentPos)
        } else {
          trieQueue = tries.collect {
            case (i, Left(trie)) if trie.nonEmpty => (i, trie)
          }
        }
      }
      if (trieQueue.nonEmpty) {
        val anomalyStart = trieQueue.map(_._1).min
        cutAndQueueBlock(pos, anomalyStart)
        charBuffer.position(anomalyStart)
        trieQueue = Queue.empty
      } else {
        if (pos < charBuffer.limit()) {
          cutAndQueueBlock(pos, charBuffer.limit())
          charBuffer.position(charBuffer.limit())
        }
      }
      charBuffer.compact()

    } else {
      throw new TokenizerException("Malformed input file or wrong encoding")
    }
    buffer.compact()
    true
  }

  override def hasNext: Boolean = {
    synchronized {
      refillBuffersAndProcess()
    }
  }

  override def next(): Token[T] = {
    synchronized {
      if (!hasNext) throw new IllegalStateException("Iterator is empty")
      val (x, nq) = queue.dequeue
      queue = nq
      x
    }
  }
}
