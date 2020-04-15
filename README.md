# csvparser
![Scala CI](https://github.com/tinedel/csvparser/workflows/Scala%20CI/badge.svg)

Scala implementation of csv parser provides two implementations out of the box and also
generic tokenizer and lexer to build your own version if needed.

Both standard and moderately non-standrard inputs are supported. If non-standard input is
provided parser attempts to make as much sense of it as possible. Intention is to never silently 
skip any data. Record length is not enforced, so any record can contain any number of fields.

Parser does not distinguish between empty field and field containing empty string. 
They will be both returned as empty string

## Quick start

Simplest possible usage is as follows:
```scala
import ua.kyiv.tinedel.csvparser.RelaxedCSVParser
import java.io.File

val parser = RelaxedCSVParser(new File("fileToParse.csv"), header = true)

// prints header
println(parser.headerRecord.toList.sortBy(_._1).map(_._2).mkString(","))

// iterates through records
parser.map { record =>
  // do something with record
}
```

## Tunable separators

Parser supports configurable line and field separators, quoting strings and optional escape characters

```scala
import ua.kyiv.tinedel.csvparser.RelaxedCSVParser
import java.io.File

val parser = RelaxedCSVParser(new File("fileToParse.csv"), header = true,
  quotingString = "'",fieldSeparator = ":", recordSeparator = "\r\n", escapeString = Some("\\"))

parser.map { record =>
  // do something with record
}
```

The parser configured as above will be able to parse

```text
header:field1:field2 <- dos break here \r\n
value\:1:value\:2:value\:3 <- dos break again
'value: all this in one field with '' inside '
```
into map with "value:1", "value:2", "value:3" as the first record and just "value: all this in one field with ' inside"
as the second record.

### Default settings

```text
Unix newline (\n) as record separator 
Double quote (") as quoting string
Comma (,) as field separator
Escape functionality is turned off
```

## Charsets and unmappable/malformed characters

Parser supports different encodings through usage of implicit scala.io.Codec value

```scala
import ua.kyiv.tinedel.csvparser.RelaxedCSVParser
import java.io.File
import scala.io.Codec
import java.nio.charset.StandardCharsets

implicit val codec: Codec = Codec(StandardCharsets.UTF_16BE)

val parser = RelaxedCSVParser(new File("fileToParse.csv"), header = true)

// prints header
println(parser.headerRecord.toList.sortBy(_._1).map(_._2).mkString(","))

// iterates through records
parser.map { record =>
  // do something with record
}
 
```

Default behavior is to throw TokenizerException if any of the malformed or unmappable byte sequences are encountered during
reading the file. However by configuring implicit codec to ignore or replace them - irregular byte sequences could be processed

```scala
import scala.io.Codec
import java.nio.charset.CodingErrorAction

    implicit val codec: Codec = Codec.UTF8
      .onMalformedInput(CodingErrorAction.REPLACE)
      .onUnmappableCharacter(CodingErrorAction.IGNORE)
      .decodingReplaceWith("?")

``` 

## Huge files support and memory requirements
This parser should be able to support huge files with some exceptions. It holds internally 2 buffers of blockSize size 
(512 kB by default) for tokenizing thus producing the blocks with strings of at most 512*1024 character lengts, Lexer in turn
keeps in buffer blocks until it is able to  build a field out of it. And then parser itself keeps fields until it can make a record.

Having said above the problems can happen if either field or record does not fit into memory and in this case 
OutOfMemoryError will happen. Otherwise all should be fine. If really huge fields/records are anticipated MinimalMemoryParser
can be used. It does not store any actual string data and only produces sequences of byte ranges in the file which should be read
to get the content of the field. See Unit tests for usage example. While being almost bulletproof against OOM it's very 
uncomfortable to use and rises all kinds of questions regarding handling of the open files and housekeeping around it.

## Building and testing
The project uses sbt as a build tool. 
```shell script
> sbt test # will test the project
> sbt hugeFiles:test # will run huge file tests. see below
> sbt publishLocal # will make the parser available for other local projects in ivy repo
> sbt publishM2 # will publish to local maven repo
```

### Huge file tests
Huge file tests will run successfully exclusively on Linux machines were bash, dd and tr utilities are available and 
/dev/urandom is accessible.
Tests generate random data into file up to 1GiB and try to read the files using lexer, tokenizer and parser printing some
statistics on the way.

Each test runs for approx 6m on my laptop

## Known problems

### Style issues

* Implementation and design of tokenizers, lexers and parsers are slightly different in style and need to be thought about 
for a while to make it uniform and concise. 
* Some refactoring is required in regard of usage lambdas and inheritance when plugging the functionality in generic methods
* Code duplication for MinimalMemory and Relaxed parsers. Could be extracted to shared class

### Immutability and thread-safety

* Side effects of reading the file could have been isolated better. 
* Almost all objects in implementation are iterators and thus have state. If they'd be read from different threads it will procduce
inconsistent results. Making parser thread safe is a matter of hiding constructor and making sure lexer is owned by parser but it
haven't been done yet.
* Making truly immutable parser is hard as e.g. 
```scala
val parser1 = getParser
val parser2 = parser1

assert parser1.next == parser2.next // will fail in current design
```

implies storing the state somehow and will intervene with huge files processing
  
### Performance issues and strange design
* As tokenizer is decouped from lexer, lexer needs to reverse engineer representation of the token if it finds out that 
the token is escaped or quoted
* Generic tokenizer is based on characters to push responsibilities for byte->char conversion to Codec which means if we need
actual byte sizes as in MinimalMemoryTokenizer conversion back to bytes needed. We might hve been better off with converting
special strings (separators etc.) to byte representation and building tokenization without any string conversion. Downside of 
this decision is at some point conversion should indeed happen and it might be too late to discover anomalities.

### Not enough end to end test cases.
* Only test case for correctness of parsing  