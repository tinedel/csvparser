package ua.kyiv.tinedel.csvparser

import scala.io.Source


class TypedCSVParser[F <: Product with Serializable](val source: Source, val quotingString: String = "\"",
                                                     val lineSeparator: String = "\n",
                                                     val fieldDelimiter: String = ",",
                                                     val header: Boolean = false) extends Iterator[Either[F, String]] {

  override def hasNext: Boolean = ???

  override def next(): Either[F, String] = ???
}


