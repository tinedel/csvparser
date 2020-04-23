package ua.kyiv.tinedel.csvparser

package object immutable {

  implicit class PostfixMatcher[K](s: K) {
    def matchComposite[T](f: K => T): T = f(s)
  }

}
