# Demonstration tables (no explict foreign keys)

## Simple: Two tables

### Nominal: Before version control

```postgresql
CREATE TABLE a (
    a_id INTEGER PRIMARY KEY,
    x INTEGER
);

CREATE TABLE b (
    b_id INTEGER PRIMARY KEY,
    a_id INTEGER REFERENCES a (a_id),
    y INTEGER
);
```

### After version control

History tables

```postgresql
CREATE TABLE a_hx (
    -- History columns
    a_hx_id INTEGER PRIMARY KEY,
    deleted BOOLEAN NOT NULL DEFAULT FALSE, -- is this item marked deleted?
    create_time timestamp WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
    creator character varying(64) COLLATE pg_catalog."default" NOT NULL DEFAULT CURRENT_USER,
    -- Data columns
    a_id INTEGER,
    x INTEGER
    -- Foreign key columns (none in this table)
);

CREATE TABLE b_hx (
    -- History columns
   b_hx_id INTEGER PRIMARY KEY,
   deleted BOOLEAN NOT NULL DEFAULT FALSE,
   create_time timestamp WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
   creator character varying(64) COLLATE pg_catalog."default" NOT NULL DEFAULT CURRENT_USER,
    -- Data columns
   b_id INTEGER,
   a_id INTEGER,
   y INTEGER,
    -- Foreign key columns. ON DELETE CASCADE is required to support referential 
    -- integrity checking. 
   a_hx_id INTEGER REFERENCES a_hx(a_hx_id) ON DELETE CASCADE 
);
```

### Views

```postgresql
CREATE OR REPLACE VIEW a
 AS
 SELECT DISTINCT ON (a_hx.a_id) 
    a_hx.create_time AS mod_time,
    a_hx.creator,
    a_hx.a_id,
    a_hx.x
   FROM a_hx
  WHERE NOT (a_hx.a_id IN ( SELECT a_hx_1.a_id
           FROM a_hx a_hx_1
          WHERE a_hx_1.deleted))
  ORDER BY a_hx.a_id ASC, a_hx.a_hx_id DESC;

CREATE OR REPLACE VIEW b
AS
SELECT DISTINCT ON (b_hx.b_id) 
    b_hx.create_time AS mod_time,
    b_hx.creator,
    b_hx.b_id,
    b_hx.a_id,
    b_hx.y
FROM b_hx
WHERE NOT (b_hx.b_id IN ( SELECT b_hx_1.b_id
                          FROM b_hx b_hx_1
                          WHERE b_hx_1.deleted))
ORDER BY b_hx.b_id ASC, b_hx.b_hx_id DESC;
```

### Sequences

```postgresql
CREATE SEQUENCE IF NOT EXISTS a_hx_id_seq;
CREATE SEQUENCE IF NOT EXISTS b_hx_id_seq;
```

To reset (for testing)

```postgresql
ALTER SEQUENCE a_hx_id_seq RESTART WITH 1;
ALTER SEQUENCE b_hx_id_seq RESTART WITH 1;
```


