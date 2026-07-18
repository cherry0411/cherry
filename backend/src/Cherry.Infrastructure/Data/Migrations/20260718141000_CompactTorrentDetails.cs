using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Cherry.Infrastructure.Data.Migrations
{
    /// <inheritdoc />
    public partial class CompactTorrentDetails : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql(
                """
                LOCK TABLE torrents, torrent_files, torrent_extension_summaries
                    IN ACCESS EXCLUSIVE MODE;

                CREATE OR REPLACE FUNCTION pg_temp.cherry_detail_uvarint(value bigint)
                RETURNS bytea
                LANGUAGE plpgsql
                IMMUTABLE STRICT
                AS $function$
                DECLARE
                    remaining bigint := value;
                    next_byte integer;
                    result bytea := decode('', 'hex');
                BEGIN
                    IF value < 0 THEN
                        RAISE EXCEPTION 'compact detail cannot encode negative value %', value;
                    END IF;
                    LOOP
                        next_byte := (remaining & 127)::integer;
                        remaining := remaining >> 7;
                        IF remaining <> 0 THEN
                            next_byte := next_byte | 128;
                        END IF;
                        result := result || set_byte(decode('00', 'hex'), 0, next_byte);
                        EXIT WHEN remaining = 0;
                    END LOOP;
                    RETURN result;
                END
                $function$;

                CREATE OR REPLACE FUNCTION pg_temp.cherry_detail_common_prefix(left_value bytea, right_value bytea)
                RETURNS integer
                LANGUAGE plpgsql
                IMMUTABLE STRICT
                AS $function$
                DECLARE
                    result integer := 0;
                    bound integer := LEAST(octet_length(left_value), octet_length(right_value));
                BEGIN
                    WHILE result < bound AND
                          get_byte(left_value, result) = get_byte(right_value, result)
                    LOOP
                        result := result + 1;
                    END LOOP;
                    RETURN result;
                END
                $function$;

                CREATE OR REPLACE FUNCTION pg_temp.cherry_detail_encode(torrent_key bigint)
                RETURNS bytea
                LANGUAGE plpgsql
                STABLE STRICT
                AS $function$
                DECLARE
                    result bytea := decode('01', 'hex');
                    previous_path bytea := decode('', 'hex');
                    current_path bytea;
                    common_prefix integer;
                    file_rows bigint;
                    extension_rows bigint;
                    file_row record;
                    extension_row record;
                BEGIN
                    SELECT COUNT(*) INTO file_rows
                      FROM torrent_files
                     WHERE torrent_id = torrent_key;
                    result := result || pg_temp.cherry_detail_uvarint(file_rows);

                    FOR file_row IN
                        SELECT convert_to(path_text, 'UTF8') AS path_bytes, length
                          FROM torrent_files
                         WHERE torrent_id = torrent_key
                         ORDER BY convert_to(path_text, 'UTF8'), length
                    LOOP
                        current_path := file_row.path_bytes;
                        common_prefix := pg_temp.cherry_detail_common_prefix(
                            previous_path,
                            current_path);
                        result := result ||
                            pg_temp.cherry_detail_uvarint(common_prefix) ||
                            pg_temp.cherry_detail_uvarint(
                                octet_length(current_path) - common_prefix) ||
                            substring(current_path FROM common_prefix + 1) ||
                            pg_temp.cherry_detail_uvarint(file_row.length);
                        previous_path := current_path;
                    END LOOP;

                    SELECT COUNT(*) INTO extension_rows
                      FROM torrent_extension_summaries
                     WHERE torrent_id = torrent_key;
                    result := result || pg_temp.cherry_detail_uvarint(extension_rows);
                    FOR extension_row IN
                        SELECT convert_to(extension, 'UTF8') AS extension_bytes,
                               file_count,
                               total_length
                          FROM torrent_extension_summaries
                         WHERE torrent_id = torrent_key
                         ORDER BY convert_to(extension, 'UTF8')
                    LOOP
                        result := result ||
                            pg_temp.cherry_detail_uvarint(
                                octet_length(extension_row.extension_bytes)) ||
                            extension_row.extension_bytes ||
                            pg_temp.cherry_detail_uvarint(extension_row.file_count) ||
                            pg_temp.cherry_detail_uvarint(extension_row.total_length);
                    END LOOP;
                    RETURN result;
                END
                $function$;

                DO $validation$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM torrent_files
                         GROUP BY torrent_id HAVING COUNT(*) > 10000) THEN
                        RAISE EXCEPTION 'legacy compact detail exceeds 10000 file entries';
                    END IF;
                    IF EXISTS (
                        SELECT 1 FROM torrent_extension_summaries
                         GROUP BY torrent_id HAVING COUNT(*) > 128) THEN
                        RAISE EXCEPTION 'legacy compact detail exceeds 128 extension entries';
                    END IF;
                    IF EXISTS (
                        SELECT 1 FROM torrent_files
                         WHERE char_length(path_text) = 0
                            OR octet_length(convert_to(path_text, 'UTF8')) > 16384
                            OR length < 0) THEN
                        RAISE EXCEPTION 'legacy compact detail contains an invalid path or file length';
                    END IF;
                    IF EXISTS (
                        SELECT 1 FROM torrent_extension_summaries
                         WHERE char_length(extension) = 0
                            OR octet_length(convert_to(extension, 'UTF8')) > 32
                            OR file_count <= 0
                            OR total_length < 0) THEN
                        RAISE EXCEPTION 'legacy compact detail contains an invalid extension aggregate';
                    END IF;
                END
                $validation$;

                CREATE TABLE torrent_details (
                    torrent_id bigint NOT NULL,
                    payload bytea COMPRESSION lz4 NOT NULL,
                    CONSTRAINT "PK_torrent_details" PRIMARY KEY (torrent_id),
                    CONSTRAINT "CK_torrent_details_payload"
                        CHECK (octet_length(payload) BETWEEN 3 AND 67108864),
                    CONSTRAINT "FK_torrent_details_torrents_torrent_id"
                        FOREIGN KEY (torrent_id) REFERENCES torrents (id) ON DELETE CASCADE);

                CREATE TEMP TABLE compact_detail_backfill ON COMMIT DROP AS
                SELECT id, pg_temp.cherry_detail_encode(id) AS payload
                  FROM torrents
                 ORDER BY id;

                DO $validation$
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM compact_detail_backfill
                         WHERE octet_length(payload) > 67108864) THEN
                        RAISE EXCEPTION 'legacy compact detail payload exceeds 67108864 bytes';
                    END IF;
                END
                $validation$;

                INSERT INTO torrent_details (torrent_id, payload)
                SELECT id, payload FROM compact_detail_backfill ORDER BY id;

                DROP TABLE torrent_extension_summaries;
                DROP TABLE torrent_files;
                ANALYZE torrent_details;
                """);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.Sql(
                """
                LOCK TABLE torrents, torrent_details IN ACCESS EXCLUSIVE MODE;

                CREATE OR REPLACE FUNCTION pg_temp.cherry_detail_read_uvarint(payload bytea, start_position integer)
                RETURNS bigint[]
                LANGUAGE plpgsql
                IMMUTABLE STRICT
                AS $function$
                DECLARE
                    position integer := start_position;
                    next_byte integer;
                    part integer;
                    value bigint := 0;
                    byte_index integer;
                BEGIN
                    IF position < 0 THEN
                        RAISE EXCEPTION 'negative compact detail position';
                    END IF;
                    FOR byte_index IN 0..8 LOOP
                        IF position >= octet_length(payload) THEN
                            RAISE EXCEPTION 'truncated compact detail varint';
                        END IF;
                        next_byte := get_byte(payload, position);
                        position := position + 1;
                        part := next_byte & 127;
                        value := value | (part::bigint << (byte_index * 7));
                        IF (next_byte & 128) = 0 THEN
                            IF byte_index > 0 AND part = 0 THEN
                                RAISE EXCEPTION 'non-canonical compact detail varint';
                            END IF;
                            RETURN ARRAY[value, position::bigint];
                        END IF;
                    END LOOP;
                    RAISE EXCEPTION 'overflowing compact detail varint';
                END
                $function$;

                CREATE OR REPLACE FUNCTION pg_temp.cherry_detail_common_prefix(left_value bytea, right_value bytea)
                RETURNS integer
                LANGUAGE plpgsql
                IMMUTABLE STRICT
                AS $function$
                DECLARE
                    result integer := 0;
                    bound integer := LEAST(octet_length(left_value), octet_length(right_value));
                BEGIN
                    WHILE result < bound AND
                          get_byte(left_value, result) = get_byte(right_value, result)
                    LOOP
                        result := result + 1;
                    END LOOP;
                    RETURN result;
                END
                $function$;

                CREATE OR REPLACE FUNCTION pg_temp.cherry_detail_decode(payload bytea)
                RETURNS TABLE (
                    entry_kind smallint,
                    text_value text,
                    file_length bigint,
                    aggregate_count integer,
                    aggregate_length bigint)
                LANGUAGE plpgsql
                IMMUTABLE STRICT
                AS $function$
                DECLARE
                    position integer := 1;
                    state bigint[];
                    file_rows bigint;
                    extension_rows bigint;
                    row_index bigint;
                    prefix_length bigint;
                    suffix_length bigint;
                    previous_path bytea := decode('', 'hex');
                    previous_file_length bigint := 0;
                    current_path bytea;
                    previous_extension bytea := NULL;
                    current_extension bytea;
                BEGIN
                    IF octet_length(payload) < 3 OR get_byte(payload, 0) <> 1 THEN
                        RAISE EXCEPTION 'unsupported or truncated compact detail payload';
                    END IF;
                    state := pg_temp.cherry_detail_read_uvarint(payload, position);
                    file_rows := state[1];
                    position := state[2]::integer;
                    IF file_rows > 10000 THEN
                        RAISE EXCEPTION 'compact detail has too many file entries';
                    END IF;

                    IF file_rows > 0 THEN
                        FOR row_index IN 1..file_rows LOOP
                            state := pg_temp.cherry_detail_read_uvarint(payload, position);
                            prefix_length := state[1];
                            position := state[2]::integer;
                            state := pg_temp.cherry_detail_read_uvarint(payload, position);
                            suffix_length := state[1];
                            position := state[2]::integer;
                            IF prefix_length > octet_length(previous_path) OR
                               prefix_length + suffix_length > 16384 OR
                               suffix_length > octet_length(payload) - position THEN
                                RAISE EXCEPTION 'invalid compact detail path bounds';
                            END IF;
                            current_path :=
                                substring(previous_path FROM 1 FOR prefix_length::integer) ||
                                substring(payload FROM position + 1 FOR suffix_length::integer);
                            position := position + suffix_length::integer;
                            IF octet_length(current_path) = 0 THEN
                                RAISE EXCEPTION 'compact detail path cannot be empty';
                            END IF;
                            IF pg_temp.cherry_detail_common_prefix(
                                   previous_path,
                                   current_path) <> prefix_length THEN
                                RAISE EXCEPTION 'non-canonical compact detail path prefix';
                            END IF;
                            state := pg_temp.cherry_detail_read_uvarint(payload, position);
                            file_length := state[1];
                            position := state[2]::integer;
                            IF previous_path > current_path OR
                               (row_index > 1 AND previous_path = current_path AND
                                previous_file_length > file_length) THEN
                                RAISE EXCEPTION 'non-canonical compact detail path/length order';
                            END IF;
                            entry_kind := 1;
                            text_value := convert_from(current_path, 'UTF8');
                            aggregate_count := NULL;
                            aggregate_length := NULL;
                            RETURN NEXT;
                            previous_path := current_path;
                            previous_file_length := file_length;
                        END LOOP;
                    END IF;

                    state := pg_temp.cherry_detail_read_uvarint(payload, position);
                    extension_rows := state[1];
                    position := state[2]::integer;
                    IF extension_rows > 128 THEN
                        RAISE EXCEPTION 'compact detail has too many extension entries';
                    END IF;
                    IF extension_rows > 0 THEN
                        FOR row_index IN 1..extension_rows LOOP
                            state := pg_temp.cherry_detail_read_uvarint(payload, position);
                            suffix_length := state[1];
                            position := state[2]::integer;
                            IF suffix_length = 0 OR suffix_length > 32 OR
                               suffix_length > octet_length(payload) - position THEN
                                RAISE EXCEPTION 'invalid compact detail extension bounds';
                            END IF;
                            current_extension := substring(
                                payload FROM position + 1 FOR suffix_length::integer);
                            position := position + suffix_length::integer;
                            IF previous_extension IS NOT NULL AND
                               previous_extension >= current_extension THEN
                                RAISE EXCEPTION 'non-canonical compact detail extension order';
                            END IF;
                            state := pg_temp.cherry_detail_read_uvarint(payload, position);
                            aggregate_count := state[1]::integer;
                            position := state[2]::integer;
                            IF aggregate_count <= 0 THEN
                                RAISE EXCEPTION 'invalid compact detail extension file count';
                            END IF;
                            state := pg_temp.cherry_detail_read_uvarint(payload, position);
                            aggregate_length := state[1];
                            position := state[2]::integer;
                            entry_kind := 2;
                            text_value := convert_from(current_extension, 'UTF8');
                            file_length := NULL;
                            RETURN NEXT;
                            previous_extension := current_extension;
                        END LOOP;
                    END IF;
                    IF position <> octet_length(payload) THEN
                        RAISE EXCEPTION 'compact detail payload has trailing bytes';
                    END IF;
                END
                $function$;

                CREATE TABLE torrent_files (
                    torrent_id bigint NOT NULL,
                    path_text text NOT NULL,
                    length bigint NOT NULL,
                    CONSTRAINT "FK_torrent_files_torrents_torrent_id"
                        FOREIGN KEY (torrent_id) REFERENCES torrents (id) ON DELETE CASCADE);

                CREATE TABLE torrent_extension_summaries (
                    torrent_id bigint NOT NULL,
                    extension character varying(32) NOT NULL,
                    file_count integer NOT NULL,
                    total_length bigint NOT NULL,
                    CONSTRAINT "PK_torrent_extension_summaries"
                        PRIMARY KEY (torrent_id, extension),
                    CONSTRAINT "FK_torrent_extension_summaries_torrents_torrent_id"
                        FOREIGN KEY (torrent_id) REFERENCES torrents (id) ON DELETE CASCADE);

                INSERT INTO torrent_files (torrent_id, path_text, length)
                SELECT detail.torrent_id, decoded.text_value, decoded.file_length
                  FROM torrent_details AS detail
                  CROSS JOIN LATERAL pg_temp.cherry_detail_decode(detail.payload) AS decoded
                 WHERE decoded.entry_kind = 1;

                INSERT INTO torrent_extension_summaries (
                    torrent_id, extension, file_count, total_length)
                SELECT detail.torrent_id,
                       decoded.text_value,
                       decoded.aggregate_count,
                       decoded.aggregate_length
                  FROM torrent_details AS detail
                  CROSS JOIN LATERAL pg_temp.cherry_detail_decode(detail.payload) AS decoded
                 WHERE decoded.entry_kind = 2;

                CREATE INDEX idx_torrent_files_torrent_id ON torrent_files (torrent_id);
                DROP TABLE torrent_details;
                ANALYZE torrent_files;
                ANALYZE torrent_extension_summaries;
                """);
        }
    }
}
