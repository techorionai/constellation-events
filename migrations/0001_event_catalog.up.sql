CREATE TABLE registrations (
    app           text        PRIMARY KEY,
    source        text        NOT NULL,
    version       integer     NOT NULL,
    hash          bytea       NOT NULL,
    registered_at timestamptz NOT NULL,
    registered_by text        NOT NULL
);

CREATE TABLE event_types (
    type        text        PRIMARY KEY,
    app         text        NOT NULL REFERENCES registrations (app),
    description text        NOT NULL,
    data_schema jsonb       NOT NULL,
    deprecated  boolean     NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    removed_at  timestamptz NULL
);

CREATE INDEX event_types_app_type_idx ON event_types (app, type);
