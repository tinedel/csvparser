package ua.kyiv.tinedel.csvparser.tokenizer

import java.nio.channels.ReadableByteChannel
import java.nio.charset.CharsetDecoder
import java.nio.{ByteBuffer, CharBuffer}

import scala.collection.immutable.Queue
import scala.io.Codec

class SimpleTokenizer(val file: ReadableByteChannel,
                      val tokensTrie: Trie,
                      val blockSize: Int = 512 * 1024)(implicit codec: Codec) extends Tokenizer[String] {

  override def tokenize(): Iterator[Token[String]] = {
    new SimpleTokenizingIterator()
  }

  class SimpleTokenizingIterator extends Iterator[Token[String]] {

    private val decoder: CharsetDecoder = codec.decoder
    private val buffer: ByteBuffer = ByteBuffer.allocate(blockSize)
    private val charBuffer: CharBuffer = CharBuffer.allocate((blockSize * codec.decoder.maxCharsPerByte()).round)

    private var queue: Queue[Token[String]] = Queue.empty
    private var eof = false

    def refillBuffersAndProcess(): Boolean = {
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
          queue = queue.enqueue(Block(charBuffer.subSequence(0, to - from).toString))
        }
      }

      if (result.isOverflow || result.isUnderflow) {
        charBuffer.flip()
        var pos = charBuffer.position()
        var trieQueue: Queue[(Int, Trie)] = Queue.empty
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
            charBuffer.position(pos)
            queue = queue.enqueue(Block(charBuffer.toString))
            charBuffer.position(charBuffer.limit())
          }
        }
        charBuffer.compact()

      }
      buffer.compact()
      true
    }

    override def hasNext: Boolean = {
      refillBuffersAndProcess()
    }

    override def next(): Token[String] = {
      if (!hasNext) throw new IllegalStateException("Iterator is empty")
      val (x, nq) = queue.dequeue
      queue = nq
      x
    }
  }

}
