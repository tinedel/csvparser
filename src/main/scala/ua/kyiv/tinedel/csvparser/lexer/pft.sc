val pf1: PartialFunction[Int, String] = {
  case 1 => "1"
  case 3 => "3"
}
val pf2: PartialFunction[Int, String] = {
  case 2 => "2"
}

val pf3: PartialFunction[Int, String] = {
  case _ => "many"
}

val cs = pf1 orElse pf2 orElse pf3

cs(1)
cs(2)
cs(3)
cs(4)

implicit class IntPostfixMatcher(i: Int) {
  def matchComposite[T](f: Int => T) = f(i)
}

(1 to 4).map(_ matchComposite (pf1 orElse {
  case _: Int => "many"
}))
