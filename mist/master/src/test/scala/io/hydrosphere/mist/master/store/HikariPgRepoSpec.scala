package io.hydrosphere.mist.master.store

// TODO: Create testing environment for PostgreSQL and remove an abstract qualifier
class HikariPgRepoSpec extends HikariRepoSpec("org.postgresql.Driver",
  "jdbc:postgresql:mist", "postgres", "") {
}
