addSbtPlugin("com.jsuereth" % "sbt-pgp" % "1.1.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "2.0")
addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.2")
addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.5.0")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.7.0")
addSbtPlugin("com.47deg"  % "sbt-microsites" % "0.7.13")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.1")

libraryDependencies += "org.apache.commons" % "commons-compress" % "1.8"
