package mist.api.data

trait JsSyntax[A] {
  def js(a: A): JsData
}

trait LowPriority {

}

object JsSyntax {

  def apply[A](f: A => JsData): JsSyntax[A] = new JsSyntax[A] {
    override def js(a: A): JsData = f(a)
  }

  implicit val boolSyntax: JsSyntax[Boolean] = JsSyntax(b => JsBoolean(b))
  implicit val intSyntax: JsSyntax[Int] = JsSyntax(i => JsNumber(i))
  implicit val longSyntax: JsSyntax[Long] = JsSyntax(l => JsNumber(l))
  implicit val doubleSyntax: JsSyntax[Double] = JsSyntax(d => JsNumber(d))

  implicit val stringSyntax: JsSyntax[String] = JsSyntax(s => JsString(s))
  implicit def seqSyntax[A](implicit syn: JsSyntax[A]): JsSyntax[Seq[A]] = JsSyntax(s => JsList(s.map(syn.js)))

  implicit class JsOps[A](a: A)(implicit syntax: JsSyntax[A]) {
    def js: JsData = syntax.js(a)
  }
}


