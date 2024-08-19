-- Drop existing functions, views, indexes, and tables if they exist
DROP FUNCTION IF EXISTS insert_response CASCADE;
DROP FUNCTION IF EXISTS search_responses CASCADE;
DROP FUNCTION IF EXISTS latest_responses CASCADE;
DROP FUNCTION IF EXISTS retrieve_response CASCADE;
DROP FUNCTION IF EXISTS retrieve_response_full CASCADE;

DROP VIEW IF EXISTS responses_full CASCADE;

DROP INDEX IF EXISTS idx_uris_uri;
DROP INDEX IF EXISTS idx_files_file;
DROP INDEX IF EXISTS idx_ips_ip;
DROP INDEX IF EXISTS idx_contentType_type;
DROP INDEX IF EXISTS idx_resourceType_type;
DROP INDEX IF EXISTS idx_responses_file_id;
DROP INDEX IF EXISTS idx_responses_content_type_id;
DROP INDEX IF EXISTS idx_responses_resource_type_id;

DROP TABLE IF EXISTS responses CASCADE;
DROP TABLE IF EXISTS uris CASCADE;
DROP TABLE IF EXISTS files CASCADE;
DROP TABLE IF EXISTS ips CASCADE;
DROP TABLE IF EXISTS contentType CASCADE;
DROP TABLE IF EXISTS resourceType CASCADE;

-- Creating the necessary tables
CREATE TABLE uris (
    id SERIAL PRIMARY KEY,
    uri VARCHAR NOT NULL UNIQUE,
    uri_hash BIGINT GENERATED ALWAYS AS (hashtext(uri)) STORED
);

CREATE TABLE files (
    id SERIAL PRIMARY KEY,
    file VARCHAR NOT NULL UNIQUE,
    file_hash BIGINT GENERATED ALWAYS AS (hashtext(file)) STORED
);

CREATE TABLE ips (
    id SERIAL PRIMARY KEY,
    ip VARCHAR NOT NULL UNIQUE,
    ip_hash BIGINT GENERATED ALWAYS AS (hashtext(ip)) STORED
);

CREATE TABLE contentType (
    id SERIAL PRIMARY KEY,
    type VARCHAR NOT NULL UNIQUE,
    type_hash BIGINT GENERATED ALWAYS AS (hashtext(type)) STORED
);

CREATE TABLE resourceType (
    id SERIAL PRIMARY KEY,
    type VARCHAR NOT NULL UNIQUE,
    type_hash BIGINT GENERATED ALWAYS AS (hashtext(type)) STORED
);

CREATE TABLE responses (
    id SERIAL PRIMARY KEY,
    uri_id INT REFERENCES uris(id),
    file_id INT REFERENCES files(id),
    content_type_id INT REFERENCES contentType(id),
    resource_type_id INT REFERENCES resourceType(id),
    record_length BIGINT,
    record_offset BIGINT,
    content_offset BIGINT,
    content_length BIGINT,
    status SMALLINT,
    meta JSONB,
    date_added TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Creating indexes on relevant columns
CREATE INDEX idx_uris_uri ON uris (uri);
CREATE INDEX idx_files_file ON files (file);
CREATE INDEX idx_ips_ip ON ips (ip);
CREATE INDEX idx_contentType_type ON contentType (type);
CREATE INDEX idx_resourceType_type ON resourceType (type);
CREATE INDEX idx_responses_file_id ON responses (file_id);
CREATE INDEX idx_responses_content_type_id ON responses (content_type_id);
CREATE INDEX idx_responses_resource_type_id ON responses (resource_type_id);

-- Creating the 'responses_full' view
CREATE OR REPLACE VIEW responses_full AS
SELECT
    r.id AS response_id,
    u.uri AS uri,
    f.file AS file,
    ct.type AS content_type,
    rt.type AS resource_type,
    i.ip AS ip,
    r.record_length,
    r.record_offset,
    r.content_offset,
    r.content_length,
    r.status,
    r.meta,
    r.date_added
FROM
    responses r
JOIN
    uris u ON r.uri_id = u.id
JOIN
    files f ON r.file_id = f.id
JOIN
    contentType ct ON r.content_type_id = ct.id
LEFT JOIN
    resourceType rt ON r.resource_type_id = rt.id
LEFT JOIN
    ips i ON r.file_id = i.id;

-- Create the optimized 'insert_response' function
CREATE OR REPLACE FUNCTION insert_response(
    uri_string VARCHAR,
    file_string VARCHAR,
    content_type_string VARCHAR,
    resource_type_string VARCHAR,
    record_length BIGINT,
    record_offset BIGINT,
    content_offset BIGINT,
    content_length BIGINT,
    status SMALLINT,
    meta JSONB
)
RETURNS VOID AS $$
DECLARE
    uri_id INT;
    file_id INT;
    content_type_id INT;
    resource_type_id INT;
BEGIN
    -- Upsert the URI and retrieve ID
    INSERT INTO uris (uri)
    VALUES (uri_string)
    ON CONFLICT (uri) DO NOTHING
    RETURNING id INTO uri_id;
    IF NOT FOUND THEN
        SELECT id INTO uri_id FROM uris WHERE uri = uri_string;
    END IF;

    -- Upsert the file and retrieve ID
    INSERT INTO files (file)
    VALUES (file_string)
    ON CONFLICT (file) DO NOTHING
    RETURNING id INTO file_id;
    IF NOT FOUND THEN
        SELECT id INTO file_id FROM files WHERE file = file_string;
    END IF;

    -- Upsert the content type and retrieve ID
    INSERT INTO contentType (type)
    VALUES (content_type_string)
    ON CONFLICT (type) DO NOTHING
    RETURNING id INTO content_type_id;
    IF NOT FOUND THEN
        SELECT id INTO content_type_id FROM contentType WHERE type = content_type_string;
    END IF;

    -- Upsert the resource type and retrieve ID
    INSERT INTO resourceType (type)
    VALUES (resource_type_string)
    ON CONFLICT (type) DO NOTHING
    RETURNING id INTO resource_type_id;
    IF NOT FOUND THEN
        SELECT id INTO resource_type_id FROM resourceType WHERE type = resource_type_string;
    END IF;

    -- Insert the response record
    INSERT INTO responses (
        uri_id,
        file_id,
        content_type_id,
        resource_type_id,
        record_length,
        record_offset,
        content_offset,
        content_length,
        status,
        meta
    ) VALUES (
        uri_id,
        file_id,
        content_type_id,
        resource_type_id,
        record_length,
        record_offset,
        content_offset,
        content_length,
        status,
        meta
    );
END;
$$ LANGUAGE plpgsql;

-- Creating the search_responses function with conditional queries
CREATE OR REPLACE FUNCTION search_responses(
    search_uri_a VARCHAR,
    limit_num_a INT DEFAULT 32,
    offset_num_a INT DEFAULT 0,
    search_ip_a VARCHAR DEFAULT NULL,
    search_content_type_a VARCHAR DEFAULT NULL
)
RETURNS TABLE (
    response_id_r INT,
    uri_r VARCHAR,
    file_r VARCHAR,
    content_type_r VARCHAR,
    resource_type_r VARCHAR,
    ip_r VARCHAR,
    record_length_r BIGINT,
    record_offset_r BIGINT,
    content_offset_r BIGINT,
    content_length_r BIGINT,
    status_r SMALLINT,
    meta_r JSONB
) AS $$
BEGIN
    RETURN QUERY 
    WITH matched_uris_v AS (
        SELECT id AS uri_id, uri AS matched_uri
        FROM uris
        WHERE uri ILIKE '%' || search_uri_a || '%'
        LIMIT limit_num_a
        OFFSET offset_num_a
    ),
    matched_content_types_v AS (
        SELECT id AS matched_content_type_id
        FROM contentType
        WHERE search_content_type_a IS NOT NULL AND type ILIKE '%' || search_content_type_a || '%'
    ),
    matched_ips_v AS (
        SELECT id AS matched_ip_id
        FROM ips
        WHERE search_ip_a IS NOT NULL AND ip ILIKE '%' || search_ip_a || '%'
    )
    SELECT
        r.id AS response_id_r,
        u.matched_uri AS uri_r,
        f.file AS file_r,
        ct.type AS content_type_r,
        rt.type AS resource_type_r,
        i.ip AS ip_r,
        r.record_length AS record_length_r,
        r.record_offset AS record_offset_r,
        r.content_offset AS content_offset_r,
        r.content_length AS content_length_r,
        r.status AS status_r,
        r.meta::jsonb AS meta_r  -- Cast meta to jsonb
    FROM
        responses r
    JOIN
        matched_uris_v u ON r.uri_id = u.uri_id
    JOIN
        files f ON r.file_id = f.id
    JOIN
        contentType ct ON r.content_type_id = ct.id
    LEFT JOIN
        resourceType rt ON r.resource_type_id = rt.id
    LEFT JOIN
        matched_content_types_v mct ON r.content_type_id = mct.matched_content_type_id
    LEFT JOIN
        matched_ips_v mi ON r.file_id = mi.matched_ip_id
    LEFT JOIN
        ips i ON mi.matched_ip_id = i.id
    WHERE
        (search_content_type_a IS NULL OR r.content_type_id IN (SELECT matched_content_type_id FROM matched_content_types_v)) AND
        (search_ip_a IS NULL OR i.id IS NOT NULL);
END;
$$ LANGUAGE plpgsql;

-- Creating the latest_responses function
CREATE OR REPLACE FUNCTION latest_responses(
    limit_num_a INT DEFAULT 24
)
RETURNS TABLE (
    response_id_r INT,
    uri_r VARCHAR,
    file_r VARCHAR,
    content_type_r VARCHAR,
    resource_type_r VARCHAR,
    ip_r VARCHAR,
    record_length_r BIGINT,
    record_offset_r BIGINT,
    content_offset_r BIGINT,
    content_length_r BIGINT,
    status_r SMALLINT,
    meta_r JSONB,
    date_added_r TIMESTAMPTZ  -- Use TIMESTAMPTZ to match the actual column type
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        response_id,
        uri,
        file,
        content_type,
        resource_type,
        ip,
        record_length,
        record_offset,
        content_offset,
        content_length,
        status,
        meta,
        date_added
    FROM
        responses_full
    ORDER BY
        date_added DESC
    LIMIT limit_num_a;
END;
$$ LANGUAGE plpgsql;

-- Creating the retrieve_response function
-- Creating the retrieve_response function
CREATE OR REPLACE FUNCTION retrieve_response(
    uri_string VARCHAR
)
RETURNS TABLE (
    response_id_r INT,
    uri_r VARCHAR,
    file_r VARCHAR,
    content_type_r VARCHAR,
    resource_type_r VARCHAR,
    ip_r VARCHAR,
    record_length_r BIGINT,
    record_offset_r BIGINT,
    content_offset_r BIGINT,
    content_length_r BIGINT,
    status_r SMALLINT,
    meta_r JSONB
) AS $$
DECLARE
    uri_hash_d BIGINT;  -- Changed variable name to avoid ambiguity
BEGIN
    -- Compute the hash from the uri_string
    SELECT hashtext(uri_string) INTO uri_hash_d;

    RETURN QUERY
    SELECT
        r.id AS response_id_r,
        u.uri AS uri_r,
        f.file AS file_r,
        ct.type AS content_type_r,
        rt.type AS resource_type_r,
        i.ip AS ip_r,
        r.record_length AS record_length_r,
        r.record_offset AS record_offset_r,
        r.content_offset AS content_offset_r,
        r.content_length AS content_length_r,
        r.status AS status_r,
        r.meta AS meta_r
    FROM
        responses r
    JOIN
        uris u ON r.uri_id = u.id
    JOIN
        files f ON r.file_id = f.id
    JOIN
        contentType ct ON r.content_type_id = ct.id
    LEFT JOIN
        resourceType rt ON r.resource_type_id = rt.id
    LEFT JOIN
        ips i ON r.file_id = i.id
    WHERE
        u.uri_hash = uri_hash_d;  -- Use uri_hash_d variable here
END;
$$ LANGUAGE plpgsql;

-- Creating the retrieve_response_full function
-- Creating the retrieve_response_full function
CREATE OR REPLACE FUNCTION retrieve_response_full(
    uri_string VARCHAR
)
RETURNS TABLE (
    response_id_r INT,
    uri_r VARCHAR,
    file_r VARCHAR,
    content_type_r VARCHAR,
    resource_type_r VARCHAR,
    ip_r VARCHAR,
    record_length_r BIGINT,
    record_offset_r BIGINT,
    content_offset_r BIGINT,
    content_length_r BIGINT,
    status_r SMALLINT,
    meta_r JSONB,
    date_added_r TIMESTAMPTZ
) AS $$
DECLARE
    uri_hash_d BIGINT;  -- Changed variable name to avoid ambiguity
BEGIN
    -- Compute the hash from the uri_string
    SELECT hashtext(uri_string) INTO uri_hash_d;

    RETURN QUERY
    SELECT
        r.id AS response_id_r,
        u.uri AS uri_r,
        f.file AS file_r,
        ct.type AS content_type_r,
        rt.type AS resource_type_r,
        i.ip AS ip_r,
        r.record_length AS record_length_r,
        r.record_offset AS record_offset_r,
        r.content_offset AS content_offset_r,
        r.content_length AS content_length_r,
        r.status AS status_r,
        r.meta AS meta_r,
        r.date_added AS date_added_r
    FROM
        responses r
    JOIN
        uris u ON r.uri_id = u.id
    JOIN
        files f ON r.file_id = f.id
    JOIN
        contentType ct ON r.content_type_id = ct.id
    LEFT JOIN
        resourceType rt ON r.resource_type_id = rt.id
    LEFT JOIN
        ips i ON r.file_id = i.id
    WHERE
        u.uri_hash = uri_hash_d;  -- Use uri_hash_d variable here
END;
$$ LANGUAGE plpgsql;
