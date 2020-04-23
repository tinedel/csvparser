package ua.kyiv.tinedel.csvparser

import java.io.{File, FileInputStream}
import java.nio.charset.CodingErrorAction
import java.util.concurrent.TimeUnit

import ua.kyiv.tinedel.csvparser.immutable.{ImmutableLexer, ImmutableParser, ImmutableTokenizer}

import scala.concurrent.duration.Duration
import scala.io.{Codec, Source}

object TestApp {

  def parseArgs(args: Array[String]): (File, String) = args match {
    case Array() => throw new RuntimeException("Please provide file name and  if needed parser class")
    case Array(fileName) => (new File(fileName), "ImmutableParser")
    case Array(fileName, parserClassName) => (new File(fileName), parserClassName)
  }

  implicit val codec: Codec = Codec.UTF8.onMalformedInput(CodingErrorAction.IGNORE)
    .onUnmappableCharacter(CodingErrorAction.IGNORE)

  def parseImmutably(fis: FileInputStream): Stream[Map[Int, String]] = {
    new ImmutableParser[String]("").records(
      ImmutableLexer().lexemesStream(
        ImmutableTokenizer().tokenize(Source.fromInputStream(fis))
      )
    )
  }

  def parseRelaxed(fis: FileInputStream): Stream[Map[Int, String]] = {
    RelaxedCSVParser.fromStream(fis).toStream
  }

  def process(recordsStream: Stream[Map[Int, String]]): Unit = {
    val time = System.currentTimeMillis()
    val (records, fields, _) = recordsStream.foldLeft((0L, 0L, time)) {
      case ((records, fields, lastReportTime), record) if System.currentTimeMillis() - lastReportTime > 30000 =>
        val secondsPassed = (System.currentTimeMillis() - time) / 1000
        println(s"${records / secondsPassed} r/s ${fields / secondsPassed} f/s")
        (records + 1, fields + record.size, System.currentTimeMillis())
      case ((records, fields, lastReportTime), record) =>
        (records + 1, fields + record.size, lastReportTime)
    }

    val inTime = Duration(System.currentTimeMillis() - time, TimeUnit.MILLISECONDS)
    println("Time to parse: %02d:%02d:%02d".format(inTime.toHours, inTime.toMinutes % 60, inTime.toSeconds % 60))

    val secondsPassed = inTime.toSeconds
    println(s"Overall performance: ${records / secondsPassed} r/s ${fields / secondsPassed} f/s")
  }

  // taken from https://stackoverflow.com/questions/35609587/human-readable-size-units-file-sizes-for-scala-code-like-duration
  // as it was not mission critical
  def formatSize(size: Long): String = {
    if (size <= 0) {
      "0 B"
    } else {
      // kilo, Mega, Giga, Tera, Peta, Exa, Zetta, Yotta
      val units: Array[String] = Array("B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
      val digitGroup: Int = (Math.log10(size) / Math.log10(1024)).toInt
      f"${size / Math.pow(1024, digitGroup)}%3.3f ${units(digitGroup)}"
    }
  }

  def main(args: Array[String]): Unit = {
    val (file, parserClassName) = parseArgs(args)
    val fis = new FileInputStream(file)
    try {
      val recordsStream = parserClassName match {
        case "ImmutableParser" => parseImmutably(fis)
        case "RelaxedCSVParser" => parseRelaxed(fis)
      }

      println(s"Processing ${formatSize(file.length())} with $parserClassName")
      process(recordsStream)

    } finally {
      fis.close()
    }
  }

}
