## Tests

```postgresql
TRUNCATE a CASCADE;
TRUNCATE b CASCADE;
TRUNCATE b_hx CASCADE ;
TRUNCATE a_hx CASCADE ;
ALTER SEQUENCE a_hx_id_seq RESTART WITH 1;
ALTER SEQUENCE b_hx_id_seq RESTART WITH 1;

-- Table a
INSERT INTO a(x) VALUES (100), (200), (300), (400);
SELECT * FROM a;

UPDATE a SET x = 101 WHERE a_id = 1;
UPDATE a SET x = 102 WHERE a_id = 1;
SELECT * FROM a;

DELETE FROM a WHERE a_id = 1;
SELECT * FROM a;

UPDATE a SET x = 201 WHERE a_id = 2;
UPDATE a SET x = 202 WHERE a_id = 2;
UPDATE a SET x = 301 WHERE a_id = 3;

SELECT * FROM a; 
-- Should show 
-- mod_time                    | creator | a_id |  x  
-- ----------------------------+---------+------+-----
--  2024-09-19 12:44:56.110416 | crmp    |    2 | 202
--  2024-09-19 12:44:56.111954 | crmp    |    3 | 301
--  2024-09-19 12:44:56.011242 | crmp    |    4 | 400

-- Table b

INSERT INTO b(a_id, y) VALUES (1, 99);
-- ERROR

INSERT INTO b(a_id, y) VALUES (2, 200), (3, 300);

SELECT * FROM b;
--          mod_time          | creator | b_id | a_id |  y  
-- ---------------------------+---------+------+------+-----
--  2024-09-19 12:49:37.50815 | crmp    |    2 |    2 | 200
--  2024-09-19 12:49:37.50815 | crmp    |    3 |    3 | 300

SELECT a_id, max(a_hx_id) FROM a_hx GROUP BY a_id ORDER BY a_id;
--  a_id | max 
-- ------+-----
--     1 |   7
--     2 |   9
--     3 |  10
--     4 |   4

SELECT * FROM b_hx;
-- Should show
--  b_hx_id | deleted |        create_time        | creator | b_id | a_id |  y  | a_hx_id 
-- ---------+---------+---------------------------+---------+------+------+-----+---------
--        2 | f       | 2024-09-19 12:49:37.50815 | crmp    |    2 |    2 | 200 |       9
--        3 | f       | 2024-09-19 12:49:37.50815 | crmp    |    3 |    3 | 300 |      10

UPDATE b SET a_id = 1 WHERE b_id = 2;
-- ERROR

UPDATE b SET a_id = 3 WHERE b_id = 2;
select * from b;
--           mod_time          | creator | b_id | a_id |  y  
-- ----------------------------+---------+------+------+-----
--  2024-09-19 12:59:39.257054 | crmp    |    2 |    3 | 200
--  2024-09-19 12:49:37.50815  | crmp    |    3 |    3 | 300

DELETE FROM a WHERE a_id = 2;
-- This should succeed, and it does.

select * from a;
-- mod_time          | creator | a_id |  x  
-- ----------------------------+---------+------+-----
--  2024-09-24 11:41:29.580364 | crmp    |    3 | 301
--  2024-09-24 11:40:21.656636 | crmp    |    4 | 400
-- (2 rows)

select * from a_hx;


DELETE FROM a WHERE a_id = 3;
-- This should raise an exception
-- ERROR:  Error: Metadata item in collection "b" with "b_id" = 3 uses item to be deleted
-- CONTEXT:  PL/pgSQL function mark_delete_enforce_ref_integ_before() line 192 at RAISE
-- SQL statement "DELETE FROM ONLY "md_hx_6"."b_hx" WHERE $1 OPERATOR(pg_catalog.=) "a_hx_id""
-- SQL statement "DELETE FROM a_hx WHERE a_id = $1"
-- PL/pgSQL function mark_delete_enforce_ref_integ_before() line 133 at EXECUTE
-- SQL statement "INSERT INTO a_hx VALUES ($1, $2, $3.*)"
-- PL/pgSQL function view_ops_to_hx_table() line 54 at EXECUTE
-- db/crmp=> select * from a;
--           mod_time          | creator | a_id |  x  
-- ----------------------------+---------+------+-----
--  2024-10-02 16:43:14.635333 | crmp    |    3 | 301
--  2024-10-02 16:42:50.828513 | crmp    |    4 | 400

DELETE FROM a WHERE a_id = 4;
-- This should succeed.
select * from a;
--           mod_time          | creator | a_id |  x  
-- ----------------------------+---------+------+-----
--  2024-10-02 16:43:14.635333 | crmp    |    3 | 301

```


```postgresql
CREATE OR REPLACE PROCEDURE test_table_setup()
    LANGUAGE plpgsql
AS
$$
BEGIN
    -- Primary tables
    CREATE TABLE a (
        mod_time timestamp WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        creator character varying(64) COLLATE pg_catalog."default" NOT NULL DEFAULT CURRENT_USER,
        a_id SERIAL PRIMARY KEY,
        x INTEGER
    );

    CREATE TRIGGER t100_primary_ops_to_hx
        AFTER INSERT OR DELETE OR UPDATE
        ON a
        FOR EACH ROW
    EXECUTE FUNCTION mdhx_primary_ops_to_hx();

    CREATE TABLE b (
        mod_time timestamp WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        creator character varying(64) COLLATE pg_catalog."default" NOT NULL DEFAULT CURRENT_USER,
        b_id SERIAL PRIMARY KEY,
        a_id INTEGER REFERENCES a (a_id),
        y INTEGER
    );

    CREATE TRIGGER t100_primary_ops_to_hx
        AFTER INSERT OR DELETE OR UPDATE
        ON b
        FOR EACH ROW
    EXECUTE FUNCTION mdhx_primary_ops_to_hx();

    -- History sequences
    CREATE SEQUENCE IF NOT EXISTS a_hx_id_seq;
    CREATE SEQUENCE IF NOT EXISTS b_hx_id_seq;

    -- History tables
    CREATE TABLE a_hx (
        -- History columns
        a_hx_id INTEGER PRIMARY KEY,
        deleted BOOLEAN NOT NULL DEFAULT FALSE, -- is this item marked deleted?
        -- Must parallel primary table cols
        create_time timestamp WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        creator character varying(64) COLLATE pg_catalog."default" NOT NULL DEFAULT CURRENT_USER,
        a_id INTEGER,
        x INTEGER
        -- Foreign key columns (none in this table)
    );

    CREATE TABLE b_hx (
        -- History columns
        b_hx_id INTEGER PRIMARY KEY,
        deleted BOOLEAN NOT NULL DEFAULT FALSE,
        -- Must parallel primary table cols
        create_time timestamp WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
        creator character varying(64) COLLATE pg_catalog."default" NOT NULL DEFAULT CURRENT_USER,
        b_id INTEGER,
        a_id INTEGER,
        y INTEGER,
        -- Foreign key columns. 
        a_hx_id INTEGER REFERENCES a_hx (a_hx_id)
    );

    CREATE TRIGGER t100_add_foreign_hx_keys
        BEFORE INSERT
        ON b_hx
        FOR EACH ROW
    EXECUTE FUNCTION mdhx_add_foreign_hx_keys('{{a, a_id}}');

END;
$$;


CREATE OR REPLACE PROCEDURE test_table_teardown()
    LANGUAGE plpgsql
AS
$$
BEGIN
    DROP TABLE IF EXISTS b;
    DROP TABLE IF EXISTS a;
    DROP TABLE IF EXISTS b_hx;
    DROP TABLE IF EXISTS a_hx;
    DROP SEQUENCE IF EXISTS b_hx_id_seq;
    DROP SEQUENCE IF EXISTS a_hx_id_seq;
END;
$$;


CREATE OR REPLACE PROCEDURE test_table_reset()
    LANGUAGE plpgsql
AS
$$
BEGIN
    TRUNCATE b CASCADE;
    TRUNCATE a CASCADE;
    ALTER SEQUENCE a_a_id_seq RESTART WITH 1;
    ALTER SEQUENCE b_b_id_seq RESTART WITH 1;
    TRUNCATE b_hx CASCADE;
    TRUNCATE a_hx CASCADE;
    ALTER SEQUENCE a_hx_id_seq RESTART WITH 1;
    ALTER SEQUENCE b_hx_id_seq RESTART WITH 1;
END;
$$;


CREATE OR REPLACE PROCEDURE test_print_table(name text)
    LANGUAGE plpgsql
AS
$$
DECLARE
    row record;
BEGIN
    RAISE NOTICE '';
    RAISE NOTICE '--- Table % ---', name;
    FOR row IN EXECUTE format('SELECT * FROM %I', name)
        LOOP
            RAISE NOTICE '%', row;
        END LOOP;
    RAISE NOTICE '--- End Table % ---', name;
    RAISE NOTICE '';
END;
$$;

CREATE OR REPLACE PROCEDURE test_check_exception(title text, error_msg text, query text)
    LANGUAGE plpgsql
AS
$$
BEGIN
    RAISE NOTICE '>>> %', title;
    BEGIN
        EXECUTE query;
        ASSERT FALSE, error_msg;
    EXCEPTION
        WHEN OTHERS THEN NULL;
    END;
    RAISE NOTICE '>>> Success: %', title;
END;
$$;


CREATE OR REPLACE PROCEDURE test_run()
    LANGUAGE plpgsql
AS
$$
DECLARE
    result_bool bool;
BEGIN
    RAISE NOTICE '*** BEGIN TESTS ***';
    RAISE NOTICE '';
    RAISE NOTICE '>>> Reset tables and verify';
    CALL test_table_reset();
    ASSERT (SELECT count(*) FROM a) = 0, 'Collection a is not empty';
    ASSERT (SELECT count(*) FROM a_hx) = 0, 'Hx table a is not empty';
    ASSERT (SELECT count(*) FROM b) = 0, 'Collection b is not empty';
    ASSERT (SELECT count(*) FROM b_hx) = 0, 'Hx table b is not empty';
    RAISE NOTICE '>>> Success';

    -- Collection a

    ------
    RAISE NOTICE 'Populate collection a';
    INSERT INTO a(x) VALUES (100), (200), (300), (400);

    UPDATE a SET x = 101 WHERE a_id = 1;
    UPDATE a SET x = 102 WHERE a_id = 1;
    DELETE FROM a WHERE a_id = 1;

    UPDATE a SET x = 201 WHERE a_id = 2;
    UPDATE a SET x = 202 WHERE a_id = 2;

    UPDATE a SET x = 301 WHERE a_id = 3;

    ------
    RAISE NOTICE '>>> Check table a contains expected rows';
    CALL test_print_table('a');
    WITH expected(a_id, x) AS (VALUES (2, 202), (3, 301), (4, 400)),
        t AS (SELECT x.a_id,
                  x.x,
                  coalesce(a.a_id = x.a_id AND a.x = x.x, FALSE)
                      AS eql
              FROM a
                       FULL OUTER JOIN expected x ON a.a_id = x.a_id)
    SELECT bool_and(eql)
    FROM t
    INTO result_bool;
    ASSERT result_bool, 'Table a does not contain expected rows';
    RAISE NOTICE '>>> Success';

    -- Collection b

    ------
    CALL test_check_exception(
            'Test insert into collection b of deleted item from a',
            'Collection b did not trap invalid reference to a',
            'INSERT INTO b(a_id, y) VALUES (1, 99)'
         );

    ------
    RAISE NOTICE 'Populate collection b with legal values';
    INSERT INTO b(a_id, y) VALUES (2, 200), (3, 300);

    ------
    RAISE NOTICE '>>> Check table b contains expected rows';
    WITH expected(b_id, a_id, y) AS (VALUES (2, 2, 200), (3, 3, 300)),
        t AS (SELECT coalesce(b.b_id = x.b_id AND b.a_id = x.a_id AND b.y = x.y, FALSE)
            AS eql
              FROM b
                       FULL OUTER JOIN expected x USING (b_id))
    SELECT bool_and(eql)
    FROM t
    INTO result_bool;
    ASSERT result_bool, 'Table b does not contain expected rows';
    RAISE NOTICE '>>> Success';

    ------
    CALL test_check_exception(
            'Attempt to update item in b with non-existent item in a',
            'Collection b did not trap invalid reference to a',
            'UPDATE b SET a_id = 1 WHERE b_id = 2'
         );

    ------
    RAISE NOTICE 'Update item in b with valid ref to item in a';
    UPDATE b SET a_id = 3 WHERE b_id = 2;

    RAISE NOTICE '>>> Check table b contains expected rows.';
    WITH expected(b_id, a_id, y) AS (VALUES (2, 3, 200), (3, 3, 300)),
        t AS (SELECT coalesce(b.b_id = x.b_id AND b.a_id = x.a_id AND b.y = x.y, FALSE)
            AS eql
              FROM b
                       FULL OUTER JOIN expected x USING (b_id))
    SELECT bool_and(eql)
    FROM t
    INTO result_bool;
    ASSERT result_bool, 'Table b does not contain expected rows';
    RAISE NOTICE '>>> Success';

    ------
    -- TODO:
    CALL test_check_exception(
            'Attempt to delete item from collection "a" that is in use.',
            'Collection a did not trap deletion of item in use',
            'delete from a where a_id = 3'
         );

    RAISE NOTICE '*** END TESTS, NO ERRORS ***';
END;
$$;
```