--
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Drop tables if they exist
DROP TABLE IF EXISTS responses CASCADE;
DROP TABLE IF EXISTS types CASCADE;
DROP TABLE IF EXISTS uris CASCADE;
DROP TABLE IF EXISTS files CASCADE;
DROP TABLE IF EXISTS transfer_encoding CASCADE;
DROP TABLE IF EXISTS records CASCADE;
DROP TABLE IF EXISTS recorded_ip_addresses CASCADE;
DROP TABLE IF EXISTS content_types CASCADE;

-- Drop functions if they exist
DROP FUNCTION IF EXISTS insert_response(TEXT, TEXT, TEXT, BIGINT, BIGINT, BIGINT, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH TIME ZONE, INT, TEXT) CASCADE;
DROP FUNCTION IF EXISTS insert_record(TEXT, TEXT, TEXT, TEXT, TEXT, TIMESTAMP WITH TIME ZONE, TEXT, TEXT, TEXT, TEXT, BIGINT) CASCADE;
DROP FUNCTION IF EXISTS retrieve_responses(TEXT, TIMESTAMP WITH TIME ZONE, BIGINT, BIGINT) CASCADE;
DROP FUNCTION IF EXISTS count_responses_uri(TEXT, TIMESTAMP WITH TIME ZONE) CASCADE;

-- Table: files
CREATE TABLE IF NOT EXISTS files (
    id BIGSERIAL,
    filename TEXT COLLATE pg_catalog."default",
    CONSTRAINT files_pkey PRIMARY KEY (id),
    CONSTRAINT unique_filename UNIQUE (filename)
);

-- Table: types
CREATE TABLE IF NOT EXISTS types (
    id BIGSERIAL,
    type TEXT COLLATE pg_catalog."default",
    CONSTRAINT types_pkey PRIMARY KEY (id),
    CONSTRAINT unique_type UNIQUE (type)
);

-- Table: uris
CREATE TABLE IF NOT EXISTS uris (
    id BIGSERIAL,
    uri TEXT COLLATE pg_catalog."default",
    CONSTRAINT uris_pkey PRIMARY KEY (id),
    CONSTRAINT unique_uri UNIQUE (uri)
);

-- Table: recorded_ip_addresses
CREATE TABLE IF NOT EXISTS recorded_ip_addresses (
    id BIGSERIAL,
    ip_address TEXT COLLATE pg_catalog."default",
    CONSTRAINT recorded_ip_addresses_pkey PRIMARY KEY (id),
    CONSTRAINT unique_ip_address UNIQUE (ip_address)
);

-- Table: content_types
CREATE TABLE IF NOT EXISTS content_types (
    id BIGSERIAL,
    type TEXT COLLATE pg_catalog."default",
    CONSTRAINT content_types_pkey PRIMARY KEY (id),
    CONSTRAINT unique_content_type UNIQUE (type)
);

-- Table: transfer_encoding
CREATE TABLE IF NOT EXISTS transfer_encoding (
    id SERIAL PRIMARY KEY,
    type TEXT NOT NULL
);

INSERT INTO transfer_encoding (type) VALUES ('identity'), ('chunked');

-- Table: responses
CREATE TABLE IF NOT EXISTS responses (
    id BIGSERIAL,
    status INT,
    uri_id BIGINT,
    location_id BIGINT,
    type_id BIGINT,
    filename_id BIGINT,
    offset_header BIGINT,
    offset_content BIGINT,
    content_length BIGINT,
    date TIMESTAMP WITH TIME ZONE,
    last_modified TIMESTAMP WITH TIME ZONE,
    transfer_encoding_id BIGINT DEFAULT 1,
    CONSTRAINT responses_pkey PRIMARY KEY (id),
    CONSTRAINT fk_responses_filename_id FOREIGN KEY (filename_id)
        REFERENCES files (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_responses_type_id FOREIGN KEY (type_id)
        REFERENCES types (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_responses_uri_id FOREIGN KEY (uri_id)
        REFERENCES uris (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_responses_location_id FOREIGN KEY (location_id)
        REFERENCES uris (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_responses_transfer_encoding_id FOREIGN KEY (transfer_encoding_id)
        REFERENCES transfer_encoding (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

-- Function to insert response
CREATE OR REPLACE FUNCTION insert_response(
    _uri TEXT,
    _location TEXT,
    _type TEXT,
    _filename TEXT,
    _offset_header BIGINT,
    _offset_content BIGINT,
    _content_length BIGINT,
    _last_modified TIMESTAMP WITH TIME ZONE,
    _date TIMESTAMP WITH TIME ZONE,
    _status INT DEFAULT NULL,
    _transfer_encoding TEXT DEFAULT 'identity'
)
RETURNS VOID AS $$
BEGIN
    -- Insert URI if not exists
    INSERT INTO uris (uri)
    SELECT _uri
    WHERE NOT EXISTS (SELECT 1 FROM uris WHERE uri = _uri);

    -- Insert Type if not exists
    INSERT INTO types (type)
    SELECT _type
    WHERE NOT EXISTS (SELECT 1 FROM types WHERE type = _type);

    -- Insert Filename if not exists
    INSERT INTO files (filename)
    SELECT _filename
    WHERE NOT EXISTS (SELECT 1 FROM files WHERE filename = _filename);

    -- Insert Location if not exists
    INSERT INTO uris (uri)
    SELECT _location
    WHERE NOT EXISTS (SELECT 1 FROM uris WHERE uri = _location);

    -- Insert Transfer Encoding if not exists
    INSERT INTO transfer_encoding (type)
    SELECT _transfer_encoding
    WHERE NOT EXISTS (SELECT 1 FROM transfer_encoding WHERE type = _transfer_encoding);

    -- Insert Response
    INSERT INTO responses (
        uri_id,
        location_id,
        type_id,
        filename_id,
        offset_header,
        offset_content,
        content_length,
        last_modified,
        date,
        status,
        transfer_encoding_id
    )
    SELECT
        (SELECT id FROM uris WHERE uri = _uri),
        (SELECT id FROM uris WHERE uri = _location),
        (SELECT id FROM types WHERE type = _type),
        (SELECT id FROM files WHERE filename = _filename),
        _offset_header,
        _offset_content,
        _content_length,
        _last_modified,
        _date,
        _status,
        (SELECT id FROM transfer_encoding WHERE type = _transfer_encoding);
END;
$$ LANGUAGE plpgsql;

-- Function to retrieve responses
CREATE OR REPLACE FUNCTION retrieve_responses(
    _uri TEXT DEFAULT NULL, 
    _date TIMESTAMP WITH TIME ZONE DEFAULT NULL, 
    _limit BIGINT DEFAULT 32, 
    _page BIGINT DEFAULT 1,
    _status INT DEFAULT NULL, -- Adding status parameter
    _type TEXT DEFAULT NULL -- Adding type parameter
)
RETURNS TABLE (
    uri TEXT,
    status INT,
    location TEXT,
    type TEXT,
    filename TEXT,
    offset_header BIGINT,
    offset_content BIGINT,
    content_length BIGINT,
    last_modified TIMESTAMP WITH TIME ZONE,
    date TIMESTAMP WITH TIME ZONE,
    transfer_encoding TEXT
) AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        u.uri AS uri,
        r.status,
        l.uri AS location,
        t.type,
        f.filename,
        r.offset_header,
        r.offset_content,
        r.content_length,
        r.last_modified,
        r.date,
        te.type AS transfer_encoding
    FROM 
        responses r
    JOIN 
        uris u ON r.uri_id = u.id
    JOIN 
        types t ON r.type_id = t.id
    JOIN 
        files f ON r.filename_id = f.id
    LEFT JOIN
        uris l ON r.location_id = l.id
    LEFT JOIN
        transfer_encoding te ON r.transfer_encoding_id = te.id
    WHERE 
        (_uri IS NULL OR u.uri ILIKE '%' || _uri || '%') -- ILIKE for case-insensitive matching
        AND (_date IS NULL OR r.date >= _date)
        AND (_status IS NULL OR r.status = _status) -- Filtering based on status if it is not null
        AND (_type IS NULL OR t.type ILIKE '%' || _type || '%') -- Filtering based on type if it is not null
    ORDER BY 
        CASE WHEN _uri IS NULL THEN 1 ELSE 1 - similarity(u.uri, _uri) END, -- Similarity for URI matching
        ABS(EXTRACT(EPOCH FROM (r.date - COALESCE(_date, CURRENT_TIMESTAMP)))) -- Order by the difference in timestamps
    LIMIT 
        _limit
    OFFSET 
        ((_page - 1) * _limit)::INT; -- Cast the OFFSET to INT for better performance
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION retrieve_latest_responses(
    _limit BIGINT DEFAULT 20
)
RETURNS TABLE (
    uri TEXT,
    last_modified TIMESTAMP WITH TIME ZONE,
    date TIMESTAMP WITH TIME ZONE,
    filename TEXT
) AS $$
BEGIN
    RETURN QUERY 
    SELECT 
        u.uri AS uri,
        r.last_modified,
        r.date,
        f.filename
    FROM 
        responses r
    JOIN 
        uris u ON r.uri_id = u.id
    JOIN 
        types t ON r.type_id = t.id
    JOIN 
        files f ON r.filename_id = f.id
    WHERE 
        t.type ILIKE '%text/html%'
        AND r.status = 200
    ORDER BY 
        r.id DESC
    LIMIT _limit;
END;
$$ LANGUAGE plpgsql;
