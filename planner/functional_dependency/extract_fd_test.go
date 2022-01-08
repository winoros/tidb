package functional_dependency_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/hint"
	"github.com/stretchr/testify/assert"
)

func testGetIS(ass *assert.Assertions, ctx sessionctx.Context) infoschema.InfoSchema {
	dom := domain.GetDomain(ctx)
	// Make sure the table schema is the new schema.
	err := dom.Reload()
	ass.Nil(err)
	return dom.InfoSchema()
}

func TestFDSet_ExtractFD(t *testing.T) {
	t.Parallel()
	ass := assert.New(t)

	store, clean := testkit.CreateMockStore(t)
	defer clean()
	par := parser.New()
	par.SetParserConfig(parser.ParserConfig{EnableWindowFunction: true, EnableStrictDoubleTypeCheck: true})

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int key, b int, c int, unique(b,c))")
	tk.MustExec("create table t2(m int key, n int, p int, unique(m,n))")

	tests := []struct {
		sql  string
		best string
		fd   string
	}{
		{
			sql:  "select a from t1",
			best: "DataScan(t1)->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {}",
		},
		{
			sql:  "select a,b from t1",
			best: "DataScan(t1)->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(2)}",
		},
		{
			sql:  "select a,c,b+1 from t1",
			best: "DataScan(t1)->Projection",
			// 4 is the extended column from (b+1) determined by b, also determined by a.
			fd: "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(3,4)}",
		},
		{
			sql:  "select a,b+1,c+b from t1",
			best: "DataScan(t1)->Projection",
			// 4, 5 is the extended column from (b+1),(c+b) determined by b,c, also determined by a.
			fd: "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(4,5)}",
		},
		{
			sql:  "select a,a+b,1 from t1",
			best: "DataScan(t1)->Projection",
			// 4 is the extended column from (b+1) determined by b, also determined by a.
			fd: "{(1)-->(2,3), (2,3)~~>(1)} >>> {(1)-->(4), ()-->(5)}",
		},
		{
			sql: "select b+1, sum(a) from t1 group by(b)",
			// since b is projected out, b --> b+1 and b ~~> sum(a) is eliminated.
			best: "DataScan(t1)->Aggr(sum(test.t1.a),firstrow(test.t1.b))->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {(2)~~>(4)} >>> {}",
		},
		{
			sql:  "select b+1, b, sum(a) from t1 group by(b)",
			best: "DataScan(t1)->Aggr(sum(test.t1.a),firstrow(test.t1.b))->Projection",
			fd:   "{(1)-->(2,3), (2,3)~~>(1)} >>> {(2)~~>(4)} >>> {(2)~~>(4), (2)-->(5)}",
		},
	}

	ctx := context.TODO()
	is := testGetIS(ass, tk.Session())
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := par.ParseOneStmt(tt.sql, "", "")
		ass.Nil(err, comment)
		tk.Session().GetSessionVars().PlanID = 0
		tk.Session().GetSessionVars().PlanColumnID = 0
		err = plannercore.Preprocess(tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		ass.Nil(err)
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session(), is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		ass.Nil(err)
		p, err = plannercore.LogicalOptimize(ctx, builder.GetOptFlag(), p.(plannercore.LogicalPlan))
		ass.Nil(err)
		ass.Equal(tt.best, plannercore.ToString(p), comment)
		// extract FD to every OP
		p.(plannercore.LogicalPlan).ExtractFD()
		ass.Equal(tt.fd, plannercore.FDToString(p.(plannercore.LogicalPlan)), comment)
	}
}
