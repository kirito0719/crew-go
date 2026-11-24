//go:build integration

package sql_test

import (
	"database/sql"
	"encoding/json"
	"math"
	"strings"
	"testing"

	crewsql "crew-go/sql"
	"crew-go/store"
)

func checkPostgresStoreNumericDomain(t *testing.T, db *sql.DB) {
	ctx := t.Context()
	table := newSQLTestTable()
	dropSQLTestTablesOnCleanup(t, db, table)
	st := crewsql.NewStore(db, crewsql.Postgres{}, crewsql.WithStoreTable(table))
	if err := st.Setup(ctx); err != nil {
		t.Fatal(err)
	}

	const (
		before   = "18446744073709551614"
		maximum  = "18446744073709551615"
		power    = "18446744073709551616"
		after    = "18446744073709551617"
		fraction = "0.10000000000000000001"
	)
	for _, tc := range []struct{ group, key, number, stored string }{
		{"wide", "before", before, before},
		{"wide", "max", maximum, maximum},
		{"wide", "power", power, power},
		{"wide", "after", after, after},
		{"fraction", "zero", "0", "0"},
		{"fraction", "tiny", "1e-400", "0." + strings.Repeat("0", 399) + "1"},
		{"fraction", "tenth", "0.1", "0.1"},
		{"fraction", "precise", fraction, fraction},
		{"fraction", "higher", "0.10000000000000000002", "0.10000000000000000002"},
	} {
		ns := store.NS(tc.group)
		if err := st.Put(ctx, ns, tc.key, map[string]any{"n": json.Number(tc.number)}); err != nil {
			t.Fatal(err)
		}
		item, err := st.Get(ctx, ns, tc.key)
		if err != nil || item == nil {
			t.Fatalf("读取 %s 失败，错误 %v", tc.key, err)
		}
		obj, ok := item.Value.(map[string]any)
		if !ok || obj["n"] != json.Number(tc.stored) {
			t.Fatalf("%s 回读 %v，期望 %s", tc.key, item.Value, tc.stored)
		}
	}

	for _, tc := range []struct {
		group, op string
		value     any
		keys      []string
	}{
		{"wide", "$eq", json.Number(maximum), []string{"max"}},
		{"wide", "$lt", json.Number(maximum), []string{"before"}},
		{"wide", "$lte", json.Number(maximum), []string{"before", "max"}},
		{"wide", "$gt", uint64(math.MaxUint64), []string{"after", "power"}},
		{"wide", "$gte", json.Number(power), []string{"after", "power"}},
		{"wide", "$eq", json.Number(after), []string{"after"}},
		{"fraction", "$eq", json.Number(fraction), []string{"precise"}},
		{"fraction", "$gt", json.Number(fraction), []string{"higher"}},
		{"fraction", "$lt", json.Number(fraction), []string{"tenth", "tiny", "zero"}},
		{"fraction", "$lte", json.Number("1e-400"), []string{"tiny", "zero"}},
	} {
		checkStoredNumericKeys(t, st, tc.group, tc.op, tc.value, tc.keys)
	}
}
