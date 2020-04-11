package ua.kyiv.tinedel.csvparser.tokenizer

trait Tokenizer[T] {
  def tokenize(): Iterator[Token[T]]
}
