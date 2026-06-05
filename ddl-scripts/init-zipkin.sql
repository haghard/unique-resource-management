CREATE TABLE IF NOT EXISTS zipkin_spans (
  `trace_id_high` BIGINT NOT NULL DEFAULT 0,
  `trace_id` BIGINT NOT NULL,
  `id` BIGINT NOT NULL,
  `name` VARCHAR(255) NOT NULL,
  `remote_service_name` VARCHAR(255),
  `parent_id` BIGINT,
  `debug` BIT(1),
  `start_ts` BIGINT COMMENT 'Span.timestamp(): microseconds',
  `duration` BIGINT COMMENT 'Span.duration(): microseconds',
PRIMARY KEY (`trace_id_high`, `trace_id`, `id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;

CREATE TABLE IF NOT EXISTS zipkin_annotations (
  `trace_id_high` BIGINT NOT NULL DEFAULT 0,
  `trace_id` BIGINT NOT NULL,
  `span_id` BIGINT NOT NULL,
  `a_key` VARCHAR(255) NOT NULL,
  `a_value` BLOB,
  `a_type` INT NOT NULL,
  `a_timestamp` BIGINT COMMENT 'Annotation.timestamp(): microseconds',
  `endpoint_ipv4` INT,
  `endpoint_ipv6` BINARY(16),
  `endpoint_port` SMALLINT,
  `endpoint_service_name` VARCHAR(255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;

CREATE TABLE IF NOT EXISTS zipkin_dependencies (
  `day` DATE NOT NULL,
  `parent` VARCHAR(255) NOT NULL,
  `child` VARCHAR(255) NOT NULL,
  `call_count` BIGINT,
  `error_count` BIGINT,
  PRIMARY KEY (`day`, `parent`, `child`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=COMPRESSED;