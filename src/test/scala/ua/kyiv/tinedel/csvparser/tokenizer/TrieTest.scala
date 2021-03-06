package ua.kyiv.tinedel.csvparser.tokenizer

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers


class TrieTest extends AnyFlatSpec with Matchers {

  behavior of "A trie"

  it must "be possible to populate" in {
    val updatedTrie = Trie("\n" -> RecordSeparator,
      "," -> FieldSeparator,
      "\"" -> QuotationMark)
    updatedTrie.children must contain('\n' -> Trie(token = Some(RecordSeparator)))
    updatedTrie.children must contain(',' -> Trie(token = Some(FieldSeparator)))
    updatedTrie.children must contain('"' -> Trie(token = Some(QuotationMark)))
  }

  behavior of "A multichar trie"

  it must "be possible to populate" in {
    val updatedTrie = Trie(
      "\r\n" -> RecordSeparator,
      "::" -> FieldSeparator,
      "\"" -> QuotationMark)
    updatedTrie.children must contain('\r' -> Trie(children = Map('\n' -> Trie(token = Some(RecordSeparator)))))
    updatedTrie.children must contain(':' -> Trie(children = Map(':' -> Trie(token = Some(FieldSeparator)))))
    updatedTrie.children must contain('"' -> Trie(token = Some(QuotationMark)))
  }

  it must "detect ambiguous separators" in {
    an[TrieException] must be thrownBy Trie(":" -> 1, "::" -> 2)
    an[TrieException] must be thrownBy Trie("::" -> 1, ":" -> 2)
  }

  it must "be possible to traverse" in {
    val updatedTrie = Trie[Token[Nothing]]()
      .add("\r\n", RecordSeparator)
      .add("::", FieldSeparator)
      .add(":\"", QuotationMark)
      .add("'", QuotationMark)

    testTrie(updatedTrie, "\r") must matchPattern {
      case Left(Trie(ch, _)) if ch.contains('\n') =>
    }

    testTrie(updatedTrie, "\r\nabc") must matchPattern {
      case Right(RecordSeparator) =>
    }

    testTrie(updatedTrie, ":\"") must matchPattern {
      case Right(QuotationMark) =>
    }

    testTrie(updatedTrie, "::") must matchPattern {
      case Right(FieldSeparator) =>
    }

    testTrie(updatedTrie, "abc") must matchPattern {
      case Left(t@Trie(_, _)) if t.isEmpty =>
    }
  }

  private def testTrie[T](trie: Trie[T], testString: String) = {
    testString.foldLeft(Left(trie).asInstanceOf[Either[Trie[T], T]])((e, c) => e match {
      case token@Right(_) => token
      case Left(trie) if trie.nonEmpty => trie.matchChar(c)
      case _ => Left(Trie())
    })
  }
}
