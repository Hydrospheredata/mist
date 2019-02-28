package io.hydrosphere.mist.master.store

class HikariH2RepoSpec extends HikariRepoSpec("org.h2.Driver",
  "jdbc:h2:mem:mist_test;DB_CLOSE_DELAY=-1", "sa", "",
  "/db/migrations/h2") {
}
