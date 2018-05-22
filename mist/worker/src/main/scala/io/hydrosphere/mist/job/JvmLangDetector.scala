package io.hydrosphere.mist.job

import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm._
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Opcodes._
import io.hydrosphere.mist.core.FunctionInfoData

class CheckSource extends ClassVisitor(ASM5) {

  var isScala = false

  override def visitSource(s: String, s1: String): Unit = {
    isScala = s.endsWith(".scala")
    super.visitEnd()
  }
}

object JvmLangDetector {

  def lang(cls: Class[_]): String = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val reader = new ClassReader(cls.getResourceAsStream(className))
    val finder = new CheckSource()
    reader.accept(finder, 0)

    if (finder.isScala) FunctionInfoData.ScalaLang else FunctionInfoData.JavaLang
  }
}
