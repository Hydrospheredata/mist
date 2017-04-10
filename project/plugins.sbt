addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.10.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.1.0")
addSbtPlugin("org.scoverage" % "sbt-coveralls" % "1.1.0")
addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "1.1")
addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.3")

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.8"
