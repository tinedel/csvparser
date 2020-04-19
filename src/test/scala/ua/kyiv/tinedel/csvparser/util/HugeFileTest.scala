package ua.kyiv.tinedel.csvparser.util

import java.io.File

import org.scalatest.Tag

object HugeFileTest extends Tag("HugeFileTest")

trait HugeFile {
  def withFile(bs: Int, count: Int)(testCode: File => Any) {
    val file = File.createTempFile("token", "data") // create the fixture
    val process = new ProcessBuilder("/bin/bash", "-c",
      s"""</dev/urandom tr -dc 'A-Za-z0-9!\"#$$%&'\\''()*+,-./:;<=>?@[\\]^_`{|}~\\n' | dd bs=$bs count=$count 2>/dev/null > ${file.getAbsolutePath}""")
      .start()
    process.waitFor()
    try {
      testCode(file) // "loan" the fixture to the test
    }
    finally file.delete() // clean up the fixture
  }
}
