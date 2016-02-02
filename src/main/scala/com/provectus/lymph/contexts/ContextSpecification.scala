package com.provectus.lymph.contexts

import com.provectus.lymph.Specification

private[lymph] trait ContextSpecification extends Specification[ContextWrapper]

private[lymph] class DummyContextSpecification extends ContextSpecification {
  override def specified(context: ContextWrapper): Boolean = {
    true
  }
}

private[lymph] class NamedContextSpecification(name: String) extends ContextSpecification {
  override def specified(context: ContextWrapper): Boolean = {
    context.asInstanceOf[NamedContextWrapper].name == name
  }
}
