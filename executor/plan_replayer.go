// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"archive/zip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// PlanReplayerSingleExec represents a plan replayer executor.
type PlanReplayerSingleExec struct {
	baseExecutor
	ExecStmt ast.StmtNode
	Analyze  bool
	Load     bool
	File     string

	endFlag bool
}

type tableNamePair struct {
	DBName    string
	TableName string
}

type tableNameExtractor struct {
	curDB string
	names map[tableNamePair]struct{}
}

func (tne *tableNameExtractor) Enter(in ast.Node) (ast.Node, bool) {
	if _, ok := in.(*ast.TableName); ok {
		return in, true
	}
	return in, false
}

func (tne *tableNameExtractor) Leave(in ast.Node) (ast.Node, bool) {
	if t, ok := in.(*ast.TableName); ok {
		tp := tableNamePair{DBName: t.Schema.L, TableName: t.Name.L}
		if tp.DBName == "" {
			tp.DBName = tne.curDB
		}
		if _, ok := tne.names[tp]; !ok {
			tne.names[tp] = struct{}{}
		}
	}
	return in, true
}

// Next implements the Executor Next interface.
func (e *PlanReplayerSingleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	if e.endFlag {
		return nil
	}
	if e.ExecStmt == nil {
		return errors.New("plan replayer: sql is empty")
	}
	res, err := e.dumpSingle(domain.GetPlanReplayerDirName())
	if err != nil {
		return err
	}
	req.AppendString(0, res)
	e.endFlag = true
	return nil
}

// dumpSingle will dump the information about a single sql.
// The files will be organized into the following format:
// |-meta.txt
// |-schema.sql
// |-stats
// |   |-stats1.json
// |   |-stats2.json
// |   |-....
// |-config.toml
// |-variables.toml
// |-bindings.sql
// |-sqls.sql
// |_explain
//     |-explain.txt
//
func (e *PlanReplayerSingleExec) dumpSingle(path string) (string, error) {
	// Create path
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return "", errors.AddStack(err)
	}

	// Generate key and create zip file
	time := time.Now().UnixNano()
	b := make([]byte, 16)
	_, err = rand.Read(b)
	if err != nil {
		return "", err
	}
	key := base64.URLEncoding.EncodeToString(b)
	fileName := fmt.Sprintf("replayer_single_%v_%v.zip", key, time)
	zf, err := os.Create(filepath.Join(path, fileName))
	if err != nil {
		return "", errors.AddStack(err)
	}

	// Create zip writer
	zw := zip.NewWriter(zf)
	defer func() {
		err := zw.Close()
		if err != nil {
			logutil.BgLogger().Warn("Closing zip writer failed", zap.Error(err))
		}
		err = zf.Close()
		if err != nil {
			logutil.BgLogger().Warn("Closing zip file failed", zap.Error(err))
		}
	}()

	// Dump config
	if err = dumpConfig(zw); err != nil {
		return "", err
	}

	// Dump meta
	if err = dumpMeta(zw); err != nil {
		return "", err
	}

	// Retrieve current DB
	sessionVars := e.ctx.GetSessionVars()
	dbName := model.NewCIStr(sessionVars.CurrentDB)
	do := domain.GetDomain(e.ctx)

	// Retrieve all tables
	pairs, err := extractTableNames(e.ExecStmt, dbName.L)
	if err != nil {
		return "", errors.AddStack(errors.New(fmt.Sprintf("plan replayer: invalid SQL text, err: %v", err)))
	}

	// Dump Schema
	if err = dumpSchemas(e.ctx, zw, pairs); err != nil {
		return "", err
	}

	// Dump stats
	if err = dumpStats(zw, pairs, do); err != nil {
		return "", err
	}

	// Dump variables
	if err = dumpVariables(e.ctx, zw); err != nil {
		return "", err
	}

	// Dump sql
	sql, err := zw.Create("sqls.sql")
	if err != nil {
		return "", nil
	}
	_, err = sql.Write([]byte(e.ExecStmt.Text()))
	if err != nil {
		return "", err
	}

	// Dump session bindings
	if err = dumpSessionBindings(e.ctx, zw); err != nil {
		return "", err
	}

	// Dump global bindings
	if err = dumpGlobalBindings(e.ctx, zw); err != nil {
		return "", err
	}

	// Dump explain
	if err = dumpExplain(e.ctx, zw, e.ExecStmt.Text(), e.Analyze); err != nil {
		return "", err
	}

	return fileName, nil
}

func dumpConfig(zw *zip.Writer) error {
	cf, err := zw.Create("config.toml")
	if err != nil {
		return errors.AddStack(err)
	}
	if err := toml.NewEncoder(cf).Encode(config.GetGlobalConfig()); err != nil {
		return errors.AddStack(err)
	}
	return nil
}

func dumpMeta(zw *zip.Writer) error {
	mt, err := zw.Create("meta.txt")
	if err != nil {
		return errors.AddStack(err)
	}
	_, err = mt.Write([]byte(printer.GetTiDBInfo()))
	if err != nil {
		return errors.AddStack(err)
	}
	return nil
}

func dumpSchemas(ctx sessionctx.Context, zw *zip.Writer, pairs map[tableNamePair]struct{}) error {
	for pair := range pairs {
		err := getShowCreateTable(pair, zw, ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func dumpStats(zw *zip.Writer, pairs map[tableNamePair]struct{}, do *domain.Domain) error {
	for pair := range pairs {
		jsonTbl, err := getStatsForTable(do, pair)
		if err != nil {
			return err
		}
		statsFw, err := zw.Create(fmt.Sprintf("stats/%v.%v.json", pair.DBName, pair.TableName))
		if err != nil {
			return errors.AddStack(err)
		}
		data, err := json.Marshal(jsonTbl)
		if err != nil {
			return errors.AddStack(err)
		}
		_, err = statsFw.Write(data)
		if err != nil {
			return errors.AddStack(err)
		}
	}
	return nil
}

func dumpVariables(ctx sessionctx.Context, zw *zip.Writer) error {
	varMap := make(map[string]string)
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), "show variables")
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return err
	}
	vf, err := zw.Create("variables.toml")
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		varMap[row[0]] = row[1]
	}
	if err := toml.NewEncoder(vf).Encode(varMap); err != nil {
		return errors.AddStack(err)
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func dumpSessionBindings(ctx sessionctx.Context, zw *zip.Writer) error {
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), "show bindings")
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return err
	}
	bf, err := zw.Create("session_bindings.sql")
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		fmt.Fprintf(bf, "%s\n", strings.Join(row, "\t"))
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func dumpGlobalBindings(ctx sessionctx.Context, zw *zip.Writer) error {
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), "show global bindings")
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return err
	}
	bf, err := zw.Create("global_bindings.sql")
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		fmt.Fprintf(bf, "%s\n", strings.Join(row, "\t"))
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func dumpExplain(ctx sessionctx.Context, zw *zip.Writer, sql string, isAnalyze bool) error {
	var recordSets []sqlexec.RecordSet
	var err error
	if isAnalyze {
		// Explain analyze
		recordSets, err = ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("explain analyze %s", sql))
		if err != nil {
			return err
		}
	} else {
		// Explain
		recordSets, err = ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("explain %s", sql))
		if err != nil {
			return err
		}
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return err
	}
	fw, err := zw.Create("explain.txt")
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func extractTableNames(ExecStmt ast.StmtNode, curDB string) (map[tableNamePair]struct{}, error) {
	extractor := &tableNameExtractor{
		curDB: curDB,
		names: make(map[tableNamePair]struct{}),
	}
	ExecStmt.Accept(extractor)
	return extractor.names, nil
}

func getStatsForTable(do *domain.Domain, pair tableNamePair) (*handle.JSONTable, error) {
	is := do.InfoSchema()
	h := do.StatsHandle()
	tbl, err := is.TableByName(model.NewCIStr(pair.DBName), model.NewCIStr(pair.TableName))
	if err != nil {
		return nil, err
	}
	js, err := h.DumpStatsToJSON(pair.DBName, tbl.Meta(), nil)
	return js, err
}

func getShowCreateTable(pair tableNamePair, zw *zip.Writer, ctx sessionctx.Context) error {
	recordSets, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), fmt.Sprintf("show create table `%v`.`%v`", pair.DBName, pair.TableName))
	if err != nil {
		return err
	}
	sRows, err := resultSetToStringSlice(context.Background(), recordSets[0])
	if err != nil {
		return err
	}
	fw, err := zw.Create(fmt.Sprintf("schema/%v.%v.schema.txt", pair.DBName, pair.TableName))
	if err != nil {
		return errors.AddStack(err)
	}
	for _, row := range sRows {
		fmt.Fprintf(fw, "%s\n", strings.Join(row, "\t"))
	}
	if len(recordSets) > 0 {
		if err := recordSets[0].Close(); err != nil {
			return err
		}
	}
	return nil
}

func resultSetToStringSlice(ctx context.Context, rs sqlexec.RecordSet) ([][]string, error) {
	rows, err := getRows(ctx, rs)
	if err != nil {
		return nil, err
	}
	err = rs.Close()
	if err != nil {
		return nil, err
	}
	sRows := make([][]string, len(rows))
	for i, row := range rows {
		iRow := make([]string, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				iRow[j], err = d.ToString()
				if err != nil {
					return nil, err
				}
			}
		}
		sRows[i] = iRow
	}
	return sRows, nil
}

func getRows(ctx context.Context, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	req := rs.NewChunk()
	// Must reuse `req` for imitating server.(*clientConn).writeChunks
	for {
		err := rs.Next(ctx, req)
		if err != nil {
			return nil, err
		}
		if req.NumRows() == 0 {
			break
		}

		iter := chunk.NewIterator4Chunk(req.CopyConstruct())
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
	}
	return rows, nil
}
