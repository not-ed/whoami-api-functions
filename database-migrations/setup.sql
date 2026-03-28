CREATE TABLE github_events(
    id bigint PRIMARY KEY NOT NULL,
    body json NOT NULL,
    created_at datetime2 NOT NULL
);