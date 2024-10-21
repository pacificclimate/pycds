## Tests

```postgresql
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
AS $$
    BEGIN
       -- Sequences
       CREATE SEQUENCE IF NOT EXISTS a_hx_id_seq;
       CREATE SEQUENCE IF NOT EXISTS b_hx_id_seq;
       
       -- History tables
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
       
       CREATE TRIGGER t100_add_check_foreign_keys
          BEFORE INSERT
          ON b_hx
          FOR EACH ROW
       EXECUTE FUNCTION add_check_foreign_keys('{{a, a_id}}');

       CREATE TRIGGER t200_mark_delete_enforce_ref_integ_before
          BEFORE INSERT OR UPDATE OR DELETE
          ON a_hx
          FOR EACH ROW
       EXECUTE FUNCTION mark_delete_enforce_ref_integ_before('a', 'a_id');

       CREATE TRIGGER t200_mark_delete_enforce_ref_integ_before
          BEFORE INSERT OR UPDATE OR DELETE
          ON b_hx
          FOR EACH ROW
       EXECUTE FUNCTION mark_delete_enforce_ref_integ_before('b', 'b_id');

       CREATE TRIGGER t201_mark_delete_enforce_ref_integ_after
          AFTER DELETE
          ON a_hx
          FOR EACH ROW
       EXECUTE FUNCTION mark_delete_enforce_ref_integ_after('a', 'a_id');

       CREATE TRIGGER t201_mark_delete_enforce_ref_integ_after
          AFTER DELETE
          ON b_hx
          FOR EACH ROW
       EXECUTE FUNCTION mark_delete_enforce_ref_integ_after('b', 'b_id');
       
       -- Views
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

       CREATE TRIGGER t100_view_ops
          INSTEAD OF INSERT OR DELETE OR UPDATE
          ON a
          FOR EACH ROW
       EXECUTE FUNCTION view_ops_to_hx_table('a', 'a_id');

       CREATE TRIGGER t100_view_ops
          INSTEAD OF INSERT OR DELETE OR UPDATE
          ON b
          FOR EACH ROW
       EXECUTE FUNCTION view_ops_to_hx_table('b', 'b_id');    
    END;
$$;


CREATE OR REPLACE PROCEDURE test_table_teardown()
   LANGUAGE plpgsql
AS $$
    BEGIN
       DROP VIEW b;
       DROP VIEW a;
       DROP TABLE IF EXISTS b_hx;
       DROP TABLE IF EXISTS a_hx;
       DROP SEQUENCE IF EXISTS b_hx_id_seq;
       DROP SEQUENCE IF EXISTS a_hx_id_seq;
    END;
$$;


CREATE OR REPLACE PROCEDURE test_table_reset()
   LANGUAGE plpgsql
AS $$
    BEGIN
       TRUNCATE b_hx CASCADE ;
       TRUNCATE a_hx CASCADE ;
       ALTER SEQUENCE a_hx_id_seq RESTART WITH 1;
       ALTER SEQUENCE b_hx_id_seq RESTART WITH 1;        
    END;
$$;


CREATE OR REPLACE PROCEDURE test_print_table(name text)
   LANGUAGE plpgsql
AS $$
    DECLARE 
        row record;
    BEGIN
        RAISE NOTICE '';
        RAISE NOTICE '--- Table % ---', name;
        FOR row IN EXECUTE format('SELECT * FROM %I', name) LOOP
            RAISE NOTICE '%', row;
        END LOOP;
        RAISE NOTICE '--- End Table % ---', name;
        RAISE NOTICE '';
    END;
$$;


CREATE OR REPLACE PROCEDURE test_run()
   LANGUAGE plpgsql
AS $$
    DECLARE 
        result_bool bool;
    BEGIN
        RAISE NOTICE '';
        RAISE NOTICE 'Reset tables and verify';
        CALL test_table_reset();
        ASSERT (SELECT count(*) FROM a) = 0, 'Collection a is not empty';
        ASSERT (SELECT count(*) FROM a_hx) = 0, 'Hx table a is not empty';
        ASSERT (SELECT count(*) FROM b) = 0, 'Collection b is not empty';
        ASSERT (SELECT count(*) FROM b_hx) = 0, 'Hx table b is not empty';
        
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
        RAISE NOTICE 'Check view a contains expected rows';
        -- CALL test_print_table('a');
        WITH expected(a_id, x) AS (
            VALUES (2, 202), (3, 301), (4, 400)
        ),
        t AS (
           select x.a_id, x.x, coalesce(a.a_id = x.a_id AND a.x = x.x, FALSE) 
              AS eql
           from a full outer join expected x on a.a_id = x.a_id
        )
        SELECT bool_and(eql) FROM t INTO result_bool;
        ASSERT result_bool, 'View a does not contain expected rows';
        RAISE NOTICE '    Success';
        
        -- Collection b

        ------
        RAISE NOTICE 'Test insert into collection b of deleted item from a - expect exception';
        BEGIN
           INSERT INTO b(a_id, y) VALUES (1, 99);
           ASSERT FALSE, 'Collection b did not trap invalid reference to a';
        EXCEPTION 
            WHEN OTHERS THEN NULL ;
        END;
        RAISE NOTICE '    Success';

        ------
        RAISE NOTICE 'Populate collection b with legal values';
        INSERT INTO b(a_id, y) VALUES (2, 200), (3, 300);

        ------
        RAISE NOTICE 'Check view b contains expected rows';
        WITH expected(b_id, a_id, y) AS (
           VALUES (2, 2, 200), (3, 3, 300)
        ),
        t AS (
            select coalesce(b.b_id = x.b_id AND b.a_id = x.a_id AND b.y = x.y, FALSE)
                AS eql
            from b full outer join expected x using (b_id)
        )
        SELECT bool_and(eql) FROM t INTO result_bool;
        ASSERT result_bool, 'View b does not contain expected rows';
        RAISE NOTICE '    Success';

        ------
        RAISE NOTICE 'Attempt to update item in b with non-existent item in a';
        BEGIN
            UPDATE b SET a_id = 1 WHERE b_id = 2;
            ASSERT FALSE, 'Collection b did not trap invalid reference to a';
        EXCEPTION
            WHEN OTHERS THEN NULL ;
        END;
        RAISE NOTICE '    Success';

        ------
        RAISE NOTICE 'Update item in b with valid ref to item in a';
        UPDATE b SET a_id = 3 WHERE b_id = 2;

        RAISE NOTICE 'Check view b contains expected rows.';
        WITH expected(b_id, a_id, y) AS (
            VALUES (2, 3, 200), (3, 3, 300)
        ),
             t AS (
                 select coalesce(b.b_id = x.b_id AND b.a_id = x.a_id AND b.y = x.y, FALSE)
                            AS eql
                 from b full outer join expected x using (b_id)
             )
        SELECT bool_and(eql) FROM t INTO result_bool;
        ASSERT result_bool, 'View b does not contain expected rows';
        RAISE NOTICE '    Success';

        ------
    END;
$$;
```