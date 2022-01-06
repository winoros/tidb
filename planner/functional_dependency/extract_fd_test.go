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
			fd:   "",
		},
	}

	ctx := context.TODO()
	is := testGetIS(ass, tk.Session())
	for i, tt := range tests {
		comment := fmt.Sprintf("case:%v sql:%s", i, tt.sql)
		stmt, err := par.ParseOneStmt(tt.sql, "", "")
		ass.Nil(err, comment)
		err = plannercore.Preprocess(tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
		ass.Nil(err)
		builder, _ := plannercore.NewPlanBuilder().Init(tk.Session(), is, &hint.BlockHintProcessor{})
		p, err := builder.Build(ctx, stmt)
		ass.Nil(err)
		p, err = plannercore.LogicalOptimize(ctx, builder.GetOptFlag(), p.(plannercore.LogicalPlan))
		ass.Nil(err)
		ass.Equal(plannercore.ToString(p), tt.best, comment)
		ass.Equal(p.(plannercore.LogicalPlan).ExtractFD().String(), "")
	}
}
