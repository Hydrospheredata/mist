addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.10.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.1.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0-M15")

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.8"
