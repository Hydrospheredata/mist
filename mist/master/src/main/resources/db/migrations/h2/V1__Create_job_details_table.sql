CREATE TABLE `job_details`(
    `path`          TEXT NOT NULL,
    `class_name`    TEXT NOT NULL,
    `namespace`     TEXT NOT NULL,
    `parameters`    TEXT NOT NULL,
    `external_id`   TEXT,
    `endpoint`      TEXT,
    `action`        TEXT NOT NULL,
    `source`        TEXT NOT NULL,
    `job_id`        VARCHAR(36) NOT NULL PRIMARY KEY,
    `start_time`    BIGINT,
    `end_time`      BIGINT,
    `job_result`    TEXT,
    `status`        TEXT NOT NULL,
    `worker_id`     TEXT,
    `create_time`   BIGINT NOT NULL
);