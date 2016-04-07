package com.provectus.mist.contexts

import com.provectus.mist.Specification

private[mist] trait ContextSpecification extends Specification[ContextWrapper]

/** Predicate for search the first available [[ContextWrapper]] */
private[mist] class DummyContextSpecification extends ContextSpecification {
  override def specified(context: ContextWrapper): Boolean = {
    true
  }
}

/** Predicate for search [[ContextWrapper]] by namespace
  *
  * @param name namespace
  */
private[mist] class NamedContextSpecification(name: String) extends ContextSpecification {
  override def specified(context: ContextWrapper): Boolean = {
    context.asInstanceOf[NamedContextWrapper].name == name
  }
}
