using Cherry.Infrastructure.Data;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("20260718142000_AddHeatFrames")]
public sealed class AddHeatFrames : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            CREATE TABLE heat_day_manifests (
                day date PRIMARY KEY,
                status smallint NOT NULL,
                coverage_status smallint NOT NULL,
                codec_version smallint NOT NULL,
                shard_count smallint NOT NULL,
                entry_count bigint NOT NULL,
                manifest_sha256 bytea NOT NULL,
                created_at timestamptz NOT NULL DEFAULT NOW(),
                CONSTRAINT ck_heat_day_manifest_status CHECK (status = 1),
                CONSTRAINT ck_heat_day_manifest_coverage CHECK (coverage_status IN (1, 2)),
                CONSTRAINT ck_heat_day_manifest_codec CHECK (codec_version = 1 AND shard_count = 64),
                CONSTRAINT ck_heat_day_manifest_entries CHECK (entry_count >= 0),
                CONSTRAINT ck_heat_day_manifest_digest CHECK (octet_length(manifest_sha256) = 32)
            );

            CREATE TABLE heat_day_frames (
                day date NOT NULL,
                shard smallint NOT NULL,
                codec_version smallint NOT NULL,
                entry_count integer NOT NULL,
                payload_sha256 bytea NOT NULL,
                payload bytea COMPRESSION lz4 NOT NULL,
                PRIMARY KEY (day, shard),
                CONSTRAINT fk_heat_day_frames_manifest FOREIGN KEY (day)
                    REFERENCES heat_day_manifests(day) ON DELETE CASCADE,
                CONSTRAINT ck_heat_day_frame_shard CHECK (shard BETWEEN 0 AND 63),
                CONSTRAINT ck_heat_day_frame_codec CHECK (codec_version = 1),
                CONSTRAINT ck_heat_day_frame_entries CHECK (entry_count >= 0),
                CONSTRAINT ck_heat_day_frame_digest CHECK (octet_length(payload_sha256) = 32)
            );

            CREATE TABLE heat_projection_watermarks (
                index_generation varchar(128) PRIMARY KEY,
                index_uid varchar(128) NOT NULL,
                projected_through date NULL,
                rebuild_required boolean NOT NULL DEFAULT TRUE,
                updated_at timestamptz NOT NULL DEFAULT NOW()
            );

            CREATE TABLE heat_projection_tasks (
                index_generation varchar(128) NOT NULL,
                target_day date NOT NULL,
                shard smallint NOT NULL,
                after_id bigint NULL,
                range_start_id bigint NULL,
                range_end_id bigint NULL,
                pending_task_uid bigint NULL,
                payload_sha256 bytea NULL,
                source_manifest_sha256 bytea NOT NULL,
                status smallint NOT NULL DEFAULT 0,
                updated_at timestamptz NOT NULL DEFAULT NOW(),
                PRIMARY KEY (index_generation, target_day, shard),
                CONSTRAINT fk_heat_projection_tasks_watermark FOREIGN KEY (index_generation)
                    REFERENCES heat_projection_watermarks(index_generation) ON DELETE CASCADE,
                CONSTRAINT ck_heat_projection_task_shard CHECK (shard BETWEEN 0 AND 63),
                CONSTRAINT ck_heat_projection_task_status CHECK (status IN (0, 1)),
                CONSTRAINT ck_heat_projection_task_range CHECK (
                    (range_start_id IS NULL AND range_end_id IS NULL) OR
                    (range_start_id IS NOT NULL AND range_end_id >= range_start_id)),
                CONSTRAINT ck_heat_projection_task_digest CHECK (
                    payload_sha256 IS NULL OR octet_length(payload_sha256) = 32),
                CONSTRAINT ck_heat_projection_task_source_digest CHECK (
                    octet_length(source_manifest_sha256) = 32)
            );
            """);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            DROP TABLE IF EXISTS heat_projection_tasks;
            DROP TABLE IF EXISTS heat_projection_watermarks;
            DROP TABLE IF EXISTS heat_day_frames;
            DROP TABLE IF EXISTS heat_day_manifests;
            """);
    }
}
