package ua.kyiv.tinedel.csvparser.tokenizer

trait Tokenizer[T] extends Iterator[Token[T]]
