package ua.kyiv.tinedel.csvparser

import java.io.{FileInputStream, InputStream}
import java.nio.ByteBuffer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

import scala.io.Codec

class MinimalMemoryCSVParserTest extends AnyFlatSpec with Matchers with HugeFile {

  behavior of "A minimal parser"

  def readStringData(stream: InputStream, data: List[Map[Int, ByteSeq]])(implicit codec: Codec): List[List[String]] = {
    var pos = 0L

    def read(from: Long, to: Long): String = {
      stream.skip(from - pos)
      val size = (to - from).intValue()
      val bytes = new Array[Byte](size)
      stream.read(bytes)
      pos = to
      val bb = ByteBuffer.wrap(bytes)
      codec.decoder.decode(bb).toString
    }

    data.map {
      m => m.toList.sortBy(_._1).map(p => p._2.seq.map(r => read(r.from, r.to)).mkString)
    }
  }

  it must "handle correctness test" in {
    val parser = MinimalMemoryCSVParser.fromStream(getClass.getResourceAsStream("/correctness.csv"))
    val data = parser.toList

    val stringData = readStringData(getClass.getResourceAsStream("/correctness.csv"), data)

    data must not be empty
    stringData must contain theSameElementsInOrderAs List(
      List("Year", "Make", "Model", "Description", "Price"),
      List("1997", "Ford", "E350", "ac, abs, moon", "3000.00"),
      List("1999", "Chevy", "Venture \"Extended Edition\"", "", "4900.00"),
      List("1996", "Jeep", "Grand Cherokee", "MUST SELL!\nair, moon roof, loaded", "4799.00"),
      List("1999", "Chevy", "Venture \"Extended Edition, Very Large\"", "", "5000.00"),
      List("", "", "Venture \"Extended Edition\"", "", "4900.00")
    )
  }

  /* Test create 1GiB file in /tmp directory and will run succesfully only on linux
  and may be other Unix like systems as it uses
  bash, /dev/urandom and dd to initially create file
  use sbt hugeFile:test to run
  to exclude in IntelliJ idea add "-l HugeFileTest" to test options
  -l is -L but lowercase */
  it must "be able to deal with huge files, e.g. /dev/urandom" taggedAs HugeFileTest in withFile(1024, 1024 * 1024) { file =>
    val channel = new FileInputStream(file).getChannel

    try {
      info("Read started")
      var count = 0L
      var fieldCount = 0L

      val time = System.currentTimeMillis()
      var lastReport = time
      MinimalMemoryCSVParser.fromChannel(channel).foreach { record =>
        if ((System.currentTimeMillis() - lastReport) > 10 * 1000) {
          lastReport = System.currentTimeMillis()
          info(s"${count * 1000 / (lastReport - time)} r/s, ${fieldCount * 1000 / (lastReport - time)} f/s")
        }
        count += 1
        fieldCount += record.size
      }

    } finally {
      channel.close()
    }
  }

}
