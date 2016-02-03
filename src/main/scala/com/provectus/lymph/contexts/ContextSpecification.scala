package com.provectus.lymph.contexts

import com.provectus.lymph.Specification

private[lymph] trait ContextSpecification extends Specification[ContextWrapper]

/** Predicate for search the first available [[ContextWrapper]] */
private[lymph] class DummyContextSpecification extends ContextSpecification {
  override def specified(context: ContextWrapper): Boolean = {
    true
  }
}

/** Predicate for search [[ContextWrapper]] by namespace
  *
  * @param name namespace
  */
private[lymph] class NamedContextSpecification(name: String) extends ContextSpecification {
  override def specified(context: ContextWrapper): Boolean = {
    context.asInstanceOf[NamedContextWrapper].name == name
  }
}
