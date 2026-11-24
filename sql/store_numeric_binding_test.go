package sql_test

import (
	"database/sql/driver"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	crewsql "crew-go/sql"
	"crew-go/store"
)

type delegatedNumericDialect struct{ crewsql.Dialect }
type embeddedNumericMySQL struct{ crewsql.MySQL }

type customNumericPostgres struct {
	crewsql.Postgres
	received any
}

func (d *customNumericPostgres) JSONFieldNumericCompare(col, field, op string, num any, b crewsql.ParamBinder) string {
	d.received = num
	return d.Postgres.JSONFieldNumericCompare(col, field, op, num, b)
}

func TestStoreRangeBindsTextOnlyForBuiltinMySQLAndPostgres(t *testing.T) {
	custom := &customNumericPostgres{}
	for _, tc := range []struct {
		name      string
		dialect   crewsql.Dialect
		pathBinds int
		wantText  bool
	}{
		{"PG", crewsql.Postgres{}, 1, true}, {"PG_pointer", &crewsql.Postgres{}, 1, true},
		{"MySQL", crewsql.MySQL{}, 2, true}, {"MySQL_pointer", &crewsql.MySQL{}, 2, true},
		{"SQLite", crewsql.SQLite{}, 2, false},
		{"third_party_delegates", delegatedNumericDialect{crewsql.Postgres{}}, 1, false},
		{"third_party_embeds", embeddedNumericMySQL{}, 2, false},
		{"third_party_overrides", custom, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			st := crewsql.NewStore(db, tc.dialect)
			for _, threshold := range []struct {
				literal string
				numeric driver.Value
			}{
				{"1.25000000000000000001", float64(1.25)},
				{"9007199254740993", int64(9007199254740993)},
			} {
				want := threshold.numeric
				if tc.wantText {
					want = threshold.literal
				}
				args := []driver.Value{sqlmock.AnyArg(), sqlmock.AnyArg()}
				for range tc.pathBinds {
					args = append(args, sqlmock.AnyArg())
				}
				mock.ExpectQuery("SELECT").WithArgs(append(args, want)...).
					WillReturnRows(sqlmock.NewRows([]string{"ns", "data_key", "value", "created_at", "updated_at"}))
				_, err := st.Search(t.Context(), store.NS("audit"), store.WithFilter(map[string]any{
					"n": map[string]any{"$gt": json.Number(threshold.literal)},
				}))
				if err != nil {
					t.Fatal(err)
				}
				if tc.dialect == custom && custom.received != threshold.numeric {
					t.Fatalf("第三方比较方法收到 %T(%v)，期望 %T(%v)", custom.received, custom.received, threshold.numeric, threshold.numeric)
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
