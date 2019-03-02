package io.hydrosphere.mist.master.store

// TODO: Create testing environment for PostgreSQL and remove an abstract qualifier
abstract class HikariPgRepoSpec extends HikariRepoSpec("org.postgresql.Driver",
  "jdbc:postgresql:mist", "postgres", "") {
}
