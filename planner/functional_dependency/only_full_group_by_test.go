package functional_dependency_test

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestOnlyFullGroupByOldCases(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	// test case 1
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("CREATE TABLE t1 (  c1 INT,  c2 INT,  c4 DATE,  c5 VARCHAR(1));")
	tk.MustExec("CREATE TABLE t2 (  c1 INT,  c2 INT,  c3 INT,  c5 VARCHAR(1));")
	tk.MustExec("CREATE VIEW v1 AS  SELECT alias1.c4 AS field1  FROM t1 AS alias1  INNER JOIN t1 AS alias2  ON 1 GROUP BY field1 ORDER BY alias1.c5;")
	_, err := tk.Exec("SELECT * FROM v1;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #2 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t1.c5' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

	// test case 2
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop view if exists v1")
	tk.MustExec("CREATE TABLE t1 (c1 INT, c2 INT, c4 DATE, c5 VARCHAR(1));")
	tk.MustExec("CREATE TABLE t2 (c1 INT, c2 INT, c3 INT, c5 VARCHAR(1));")
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v1 AS  SELECT alias1.c4 AS field1, alias1.c4 AS field2  FROM t1 AS alias1  INNER JOIN t1 AS alias2 ON (alias2.c1 = alias1.c2) WHERE ( NOT EXISTS (  SELECT SQ1_alias1.c5 AS SQ1_field1   FROM t2 AS SQ1_alias1  WHERE SQ1_alias1.c3 < alias1.c1 ))   AND (alias1.c5 = alias1.c5    AND alias1.c5 = 'd'   ) GROUP BY field1, field2 ORDER BY alias1.c5, field1, field2")
	tk.MustQuery("SELECT * FROM v1;")

	// test case 3
	// need to resolve the name resolver problem first (view and base table's column can not refer each other)

	// test case 4
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("drop view if exists v2")
	tk.MustExec("CREATE TABLE t1 ( col_varchar_10_utf8 VARCHAR(10) CHARACTER SET utf8,  col_int_key INT,  pk INT PRIMARY KEY);")
	tk.MustExec("CREATE TABLE t2 ( col_varchar_10_utf8 VARCHAR(10) CHARACTER SET utf8 DEFAULT NULL, col_int_key INT DEFAULT NULL,  pk INT PRIMARY KEY);")
	tk.MustExec("CREATE ALGORITHM=MERGE definer='root'@'localhost' VIEW v2 AS SELECT t2.pk, COALESCE(t2.pk, 3) AS coa FROM t1 LEFT JOIN t2 ON 0;")
	tk.MustQuery("SELECT v2.pk, v2.coa FROM t1 LEFT JOIN v2 AS v2 ON 0 GROUP BY v2.pk;")

	// test case 5
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t ( a INT, c INT GENERATED ALWAYS AS (a+2), d INT GENERATED ALWAYS AS (c+2) );")
	tk.MustQuery("SELECT c FROM t GROUP BY a;")
	tk.MustQuery("SELECT d FROM t GROUP BY c;")
	tk.MustQuery("SELECT d FROM t GROUP BY a;")
	tk.MustQuery("SELECT 1+c FROM t GROUP BY a;")
	tk.MustQuery("SELECT 1+d FROM t GROUP BY c;")
	tk.MustQuery("SELECT 1+d FROM t GROUP BY a;")
	tk.MustQuery("SELECT t1.d FROM t as t1, t as t2 WHERE t2.d=t1.c GROUP BY t2.a;")
	_, err = tk.Exec("SELECT t1.d FROM t as t1, t as t2 WHERE t2.d>t1.c GROUP BY t2.a;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.d' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

	// test case 6
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t ( a INT, c INT GENERATED ALWAYS AS (a+2), d INT GENERATED ALWAYS AS (c+2) );")
	_, err = tk.Exec("SELECT t1.d FROM t as t1, t as t2 WHERE t2.d>t1.c GROUP BY t2.a;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.d' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	_, err = tk.Exec("SELECT (SELECT t1.c FROM t as t1 GROUP BY -3) FROM t as t2;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.c' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	_, err = tk.Exec("SELECT DISTINCT t1.a FROM t as t1 ORDER BY t1.d LIMIT 1;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.t.d' which is not in SELECT list; this is incompatible with DISTINCT")
	_, err = tk.Exec("SELECT DISTINCT t1.a FROM t as t1 ORDER BY t1.d LIMIT 1;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.t.d' which is not in SELECT list; this is incompatible with DISTINCT")
	_, err = tk.Exec("SELECT (SELECT DISTINCT t1.a FROM t as t1 ORDER BY t1.d LIMIT 1) FROM t as t2;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.t.d' which is not in SELECT list; this is incompatible with DISTINCT")

	// test case 7
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(a INT NULL, b INT NOT NULL, c INT, UNIQUE(a,b));")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IS NOT NULL GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE NOT (a IS NULL) GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a > 3 GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a = 3 GROUP BY b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a BETWEEN 3 AND 6 GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a <> 3 GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IN (3,4) GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IN (SELECT b FROM t) GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IS TRUE GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE (a <> 3) IS TRUE GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a IS FALSE GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE (a <> 3) IS FALSE GROUP BY a,b;")
	tk.MustQuery("SELECT a,b,c FROM t WHERE a LIKE \"%abc%\" GROUP BY a,b;")
	// todo: eval not-null refuse NOT wrapper.
	// tk.MustQuery("SELECT a,b,c FROM t WHERE NOT(a IN (3,4)) GROUP BY a,b;")
	// tk.MustQuery("SELECT a,b,c FROM t WHERE a NOT IN (3,4) GROUP BY a,b;")
	// tk.MustQuery("SELECT a,b,c FROM t WHERE a NOT LIKE \"%abc%\" GROUP BY a,b;")
	_, err = tk.Exec("SELECT a,b,c FROM t WHERE a<=>NULL GROUP BY b;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #3 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.c' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	// is-not-true will let the null value pass, so evaluating it won't derive to a with not-null attribute.
	_, err = tk.Exec("SELECT a,b,c FROM t WHERE a IS NOT TRUE GROUP BY a,b;")
	require.NotNil(t, err)
	require.Equal(t, err.Error(), "[planner:1055]Expression #3 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t.c' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")

}
