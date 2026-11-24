//go:build integration

package sql_test

import (
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"testing"

	"github.com/go-sql-driver/mysql"

	crewsql "crew-go/sql"
	"crew-go/store"
)

func checkMySQLStoreNumericDomain(t *testing.T, db *sql.DB) {
	ctx := t.Context()
	table := newSQLTestTable()
	dropSQLTestTablesOnCleanup(t, db, table)
	st := crewsql.NewStore(db, crewsql.MySQL{}, crewsql.WithStoreTable(table))
	if err := st.Setup(ctx); err != nil {
		t.Fatal(err)
	}

	const (
		beforeMax = "18446744073709551614"
		maxUint   = "18446744073709551615"
		power     = "18446744073709551616"
		above     = "18446744073709551617"
		precise   = "0.10000000000000000001"
	)
	for _, tc := range []struct{ group, key, input, kind, output string }{
		{"wide", "before", beforeMax, "UNSIGNED INTEGER", beforeMax},
		{"wide", "max", maxUint, "UNSIGNED INTEGER", maxUint},
		{"wide", "power", power, "DOUBLE", "1.8446744073709552e19"},
		{"wide", "above", above, "DOUBLE", "1.8446744073709552e19"},
		{"decimal", "tenth", "0.1", "DOUBLE", "0.1"},
		{"decimal", "precise", precise, "DOUBLE", "0.1"},
		{"decimal", "quarter", "0.25", "DOUBLE", "0.25"},
		{"decimal", "tiny", "1e-400", "DOUBLE", "0.0"},
		{"decimal", "zero", "0", "INTEGER", "0"},
	} {
		ns := store.NS(tc.group)
		if err := st.Put(ctx, ns, tc.key, map[string]any{"n": json.Number(tc.input)}); err != nil {
			t.Fatal(err)
		}
		item, err := st.Get(ctx, ns, tc.key)
		if err != nil || item == nil {
			t.Fatalf("%s 读取失败，错误 %v", tc.key, err)
		}
		obj, ok := item.Value.(map[string]any)
		num, number := obj["n"].(json.Number)
		if !ok || !number || num.String() != tc.output {
			t.Errorf("%s 回读 %v，期望 %s", tc.key, item.Value, tc.output)
		}
		var kind string
		if err := db.QueryRowContext(ctx, "SELECT JSON_TYPE(JSON_EXTRACT(value, '$.n')) FROM "+table+" WHERE data_key = ?", tc.key).Scan(&kind); err != nil || kind != tc.kind {
			t.Errorf("%s 存储类型 %s，期望 %s，错误 %v", tc.key, kind, tc.kind, err)
		}
	}

	for _, tc := range []struct {
		group, op string
		value     any
		keys      []string
	}{
		{"wide", "$eq", json.Number(maxUint), []string{"max"}},
		{"wide", "$lt", json.Number(maxUint), []string{"before"}},
		{"wide", "$lte", json.Number(maxUint), []string{"before", "max"}},
		{"wide", "$gt", uint64(math.MaxUint64), []string{"above", "power"}},
		{"wide", "$gte", json.Number(maxUint), []string{"above", "max", "power"}},
		{"wide", "$gt", json.Number(beforeMax), []string{"above", "max", "power"}},
		{"wide", "$lt", json.Number(power), []string{"before", "max"}},
		{"wide", "$eq", json.Number(power), []string{"above", "power"}},
		{"decimal", "$eq", json.Number(precise), []string{"precise", "tenth"}},
		{"decimal", "$gt", json.Number("0.1"), []string{"quarter"}},
		{"decimal", "$lte", json.Number("1e-400"), []string{"tiny", "zero"}},
	} {
		checkStoredNumericKeys(t, st, tc.group, tc.op, tc.value, tc.keys)
	}

	ns := store.NS("overflow")
	if err := st.Put(ctx, ns, "keep", json.Number(maxUint)); err != nil {
		t.Fatal(err)
	}
	old, err := st.Get(ctx, ns, "keep")
	if err != nil || old == nil || old.Value != json.Number(maxUint) {
		t.Fatalf("读取覆盖前的值失败：%v", err)
	}
	for _, key := range []string{"keep", "absent"} {
		err := st.Put(ctx, ns, key, json.Number("1e400"))
		var dbErr *mysql.MySQLError
		if !errors.As(err, &dbErr) || dbErr.Number != 3140 || string(dbErr.SQLState[:]) != "22032" {
			t.Errorf("%s 写入溢出应保留 MySQL 3140/22032，实际 %v", key, err)
		}
	}
	if item, err := st.Get(ctx, ns, "keep"); err != nil || item == nil || item.Value != json.Number(maxUint) || !item.UpdatedAt.Equal(old.UpdatedAt) || !item.CreatedAt.Equal(old.CreatedAt) {
		t.Errorf("失败的覆盖改变了原记录，写前 %+v，写后 %+v，错误 %v", old, item, err)
	}
	if item, err := st.Get(ctx, ns, "absent"); err != nil || item != nil {
		t.Errorf("失败的插入产生了记录：%+v，错误 %v", item, err)
	}
	for _, group := range []string{"wide", "empty"} {
		_, err := st.Search(ctx, store.NS(group), store.WithFilter(map[string]any{"n": map[string]any{"$gt": json.Number("1e400")}}))
		if !errors.Is(err, store.ErrFilterTypeMismatch) {
			t.Errorf("%s 里越界的范围条件应返回 ErrFilterTypeMismatch，实际 %v", group, err)
		}
	}
}
