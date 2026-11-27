package sql

import (
	"context"
	"crypto/sha256"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"crew-go/agent_v2"
	"crew-go/schema"
)

// DefaultHistoryTable 是 HistoryStore 的默认表名。
const DefaultHistoryTable = "crew_history"

// HistoryStore 是 agent_v2.HistoryStore 的 SQL 实现，给 agent_v2.WithHistory 用：会话历史跨进程
// 重启留存（内存版 agent_v2.NewInMemoryHistoryStore 重启即丢）。
//
// 存法：每条消息一行，整条消息的 JSON 存进二进制列；行序 = 自增主键序 = 写入序，Load 按它回放，游标
// （agent_v2.HistoryCursor）就是这一行的 id。owner 与 conv_id 分两列存、不拼串（拼串会让 {acme/eu, c-1} 与
// {acme, eu/c-1} 撞成同一个会话），按字节比较（PG `COLLATE "C"`、MySQL VARBINARY、SQLite 默认 BINARY）。
// HistoryQuery 的过滤条件（单个或批量消息 ID、任务章、归属）在写入时从消息上抽成三列，Load 在库里按它们过滤，
// 不在进程里逐条解码再筛；标注的值不是字符串时那一列存空串，跟 agent_v2.HistoryQuery.Matches 一样不命中
// （过滤条件非空才生效）。
//
// Append 的一批在一个事务里写：要么整批在、要么整批不在。半批落盘的后果是 assistant 的 tool_call 在、
// 配对的 tool_result 不在——下次 Load 喂给模型就是孤儿调用（OpenAI 422 / Anthropic 400），而且
// append-only 意味着这条坏记录永远留在历史里。
//
// 消息经 JSON 往返：Annotations 里的数字读回是 float64、结构体读回是 map（与 Store / Checkpointer
// 同一约定），读标注按 JSON 类型读。
//
// 部署时调用 Setup 建消息表、索引及会话写锁表。升级前须排空共用该表的旧写入器及在途事务。
// 普通追加和条件追加在分配消息主键之前锁住同一会话，防止旧事务晚提交到已确认的边界之前。
// 业务自研 dialect 自管 schema；UpsertSuffix 须支持事务行锁。PostgreSQL 方言可嵌入 Postgres 复用序列分配，
// 其他驱动通过 sql.Result.LastInsertId 返回实际插入的主键。
type HistoryStore struct {
	db      *dbsql.DB
	dialect Dialect
	table   string
}

var _ agent_v2.HistoryTailAppender = (*HistoryStore)(nil)

// HistoryOption 配置 HistoryStore。
type HistoryOption func(*HistoryStore)

// WithHistoryTable 改表名（默认 DefaultHistoryTable）。
func WithHistoryTable(name string) HistoryOption {
	return func(h *HistoryStore) { h.table = name }
}

// NewHistoryStore 建一个 HistoryStore。表名不合法是编程错误，直接 panic。
func NewHistoryStore(db *dbsql.DB, dialect Dialect, opts ...HistoryOption) *HistoryStore {
	h := &HistoryStore{db: db, dialect: dialect, table: DefaultHistoryTable}
	for _, opt := range opts {
		opt(h)
	}
	if err := validateTableName(h.table); err != nil {
		panic("crew-go/sql NewHistoryStore: " + err.Error())
	}
	return h
}

// Append 把 msgs 追加到会话末尾，整批一个事务。先把整批编码完再开事务：有一条编不出来（比如
// Annotations 里有 NaN）就整批不写。
func (h *HistoryStore) Append(ctx context.Context, key agent_v2.HistoryKey, msgs []*schema.Message) error {
	_, _, err := h.appendBatch(ctx, key, nil, msgs, -1, false)
	return err
}

// HistoryAppendOwner 返回当前 SQL 存储。
func (h *HistoryStore) HistoryAppendOwner() agent_v2.HistoryStore { return h }

// AppendAfter 在同会话的写事务中核对尾游标，并保存本次追加中经过确认的边界位置。
func (h *HistoryStore) AppendAfter(ctx context.Context, key agent_v2.HistoryKey, after agent_v2.HistoryCursor, msgs []*schema.Message, boundary int) (agent_v2.HistoryCursor, bool, error) {
	return h.appendBatch(ctx, key, &after, msgs, boundary, true)
}

// AppendUnverifiedAfter 为可恢复的普通追加比对尾游标，允许普通写入的触发器产生附加行。
func (h *HistoryStore) AppendUnverifiedAfter(ctx context.Context, key agent_v2.HistoryKey, after agent_v2.HistoryCursor, msgs []*schema.Message) (agent_v2.HistoryCursor, bool, error) {
	return h.appendBatch(ctx, key, &after, msgs, -1, false)
}

// HistoryUnverifiedAppendOwner 返回实际接管普通条件追加的 SQL 存储。
func (h *HistoryStore) HistoryUnverifiedAppendOwner() agent_v2.HistoryStore { return h }

// historyPayload 把证明绑定到数据库分配的行游标。复制消息或原始 payload 时，新行无法沿用该证明。
type historyPayload struct {
	*schema.Message
	VerifiedAt agent_v2.HistoryCursor `json:"crew_verified_at,omitempty"`
}

type historyRow struct {
	payload            []byte
	msgID, task, agent string
}

// insertHistoryRow 返回 INSERT 自己分配的主键。其他会话或触发器分配的主键不能代表这条消息。
func (h *HistoryStore) insertHistoryRow(ctx context.Context, tx *dbsql.Tx, key agent_v2.HistoryKey, row historyRow, prev int64) (int64, error) {
	ph := h.dialect.Placeholder
	var id int64
	var err error
	if pg, ok := h.dialect.(interface{ historyInsertSQL(string) string }); ok {
		err = tx.QueryRowContext(ctx, pg.historyInsertSQL(h.table), key.Owner, key.ConvID, row.payload, row.msgID, row.task, row.agent, prev).Scan(&id)
	} else {
		q := fmt.Sprintf("INSERT INTO %s (owner, conv_id, payload, msg_id, task, agent) VALUES (%s, %s, %s, %s, %s, %s)", h.table, ph(1), ph(2), ph(3), ph(4), ph(5), ph(6))
		var result dbsql.Result
		result, err = tx.ExecContext(ctx, q, key.Owner, key.ConvID, row.payload, row.msgID, row.task, row.agent)
		if err == nil {
			id, err = result.LastInsertId()
		}
	}
	if err != nil {
		return 0, err
	}
	if id <= prev {
		return 0, fmt.Errorf("history row id %d did not advance past %d", id, prev)
	}
	return id, nil
}

// historyInsertSQL 只消费本连接尚未用完的旧序号，直到分配到当前会话尾部之后。
// PostgreSQL 的 BIGSERIAL 使用 CACHE > 1 时，各连接领到的号段可能交错，RETURNING 返回实际插入的行。
func (Postgres) historyInsertSQL(table string) string {
	return fmt.Sprintf(`WITH RECURSIVE candidate(id) AS (
    SELECT nextval(pg_get_serial_sequence('%[1]s','id'))
    UNION ALL SELECT nextval(pg_get_serial_sequence('%[1]s','id')) FROM candidate WHERE id <= $7
) INSERT INTO %[1]s (owner,conv_id,payload,msg_id,task,agent,id)
SELECT $1,$2,$3,$4,$5,$6,id FROM candidate WHERE id > $7 RETURNING id`, table)
}

func (h *HistoryStore) appendBatch(ctx context.Context, key agent_v2.HistoryKey, after *agent_v2.HistoryCursor, msgs []*schema.Message, boundary int, verify bool) (agent_v2.HistoryCursor, bool, error) {
	if key.Owner == "" || key.ConvID == "" {
		return "", false, fmt.Errorf("sql history append: key 不完整 (owner=%q conv=%q)", key.Owner, key.ConvID)
	}
	if boundary < -1 || boundary >= len(msgs) {
		return "", false, fmt.Errorf("sql history append: invalid boundary index %d", boundary)
	}
	if len(msgs) == 0 && after == nil {
		return "", true, nil
	}
	rowsToWrite := make([]historyRow, len(msgs))
	for i, m := range msgs {
		b, err := json.Marshal(m)
		if err != nil {
			return "", false, fmt.Errorf("sql history append: 第 %d 条消息编码失败: %w", i, err)
		}
		r := historyRow{payload: b}
		if m != nil {
			r.msgID = m.ID
			r.task, _ = m.Annotations[agent_v2.HistoryTaskAnnotation].(string)
			r.agent, _ = m.Annotations[agent_v2.HistoryAgentAnnotation].(string)
		}
		rowsToWrite[i] = r
	}

	tx, err := h.db.BeginTx(ctx, &dbsql.TxOptions{Isolation: dbsql.LevelReadCommitted})
	if err != nil {
		return "", false, fmt.Errorf("sql history append: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // 提交后事务已结束，其他路径回滚失败也保留原错误。
	ph := h.dialect.Placeholder
	lock := fmt.Sprintf("INSERT INTO %s (owner, conv_id) VALUES (%s, %s) %s", h.lockTable(), ph(1), ph(2),
		h.dialect.UpsertSuffix([]string{"owner", "conv_id"}, []string{"owner"}))
	if _, err := tx.ExecContext(ctx, lock, key.Owner, key.ConvID); err != nil {
		return "", false, fmt.Errorf("sql history append: lock: %w", err)
	}
	tailQuery := fmt.Sprintf("SELECT MAX(id) FROM %s WHERE owner = %s AND conv_id = %s", h.table, ph(1), ph(2))
	var original dbsql.NullInt64
	if err := tx.QueryRowContext(ctx, tailQuery, key.Owner, key.ConvID).Scan(&original); err != nil {
		return "", false, fmt.Errorf("sql history append: tail: %w", err)
	}
	tail := historyCursor(original)
	if after != nil && tail != *after {
		return "", false, nil
	}
	last := original.Int64
	for i, r := range rowsToWrite {
		id, err := h.insertHistoryRow(ctx, tx, key, r, last)
		if err != nil {
			return "", false, fmt.Errorf("sql history append: row %d: %w", i, err)
		}
		last = id
		tail = agent_v2.HistoryCursor(strconv.FormatInt(id, 10))
		if i == boundary {
			// 证明只绑定这次 INSERT 返回的行，并在同一事务中提交。
			data, err := json.Marshal(historyPayload{Message: msgs[i], VerifiedAt: tail})
			if err != nil {
				return "", false, fmt.Errorf("sql history append: encode boundary: %w", err)
			}
			mark := fmt.Sprintf("UPDATE %s SET payload = %s WHERE id = %s", h.table, ph(1), ph(2))
			if _, err := tx.ExecContext(ctx, mark, data, id); err != nil {
				return "", false, fmt.Errorf("sql history append: mark boundary: %w", err)
			}
		}
	}
	if verify && len(msgs) > 0 {
		// 同表触发器还可能追加调用方未见的行。只核对本批新增区间；有额外行时回滚，写入器再走普通追加。
		q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE owner = %s AND conv_id = %s AND id > %s", h.table, ph(1), ph(2), ph(3))
		var count int
		if err := tx.QueryRowContext(ctx, q, key.Owner, key.ConvID, original.Int64).Scan(&count); err != nil {
			return "", false, fmt.Errorf("sql history append: verify batch: %w", err)
		}
		if count != len(msgs) {
			return "", false, nil
		}
	}
	if err := tx.Commit(); err != nil {
		return "", false, fmt.Errorf("sql history append: commit: %w", err)
	}
	return tail, true, nil
}

func historyCursor(id dbsql.NullInt64) agent_v2.HistoryCursor {
	if !id.Valid {
		return ""
	}
	return agent_v2.HistoryCursor(strconv.FormatInt(id.Int64, 10))
}

// Load 按写入顺序返回 q 圈定的那一段（语义见 agent_v2.HistoryStore）：过滤条件与 Before 在库里算，Limit > 0 时按
// id 倒序取 Limit+1 行——多取的那一行只用来判 More。会话不存在、没有满足条件的记录时返空页。每次都从库里解码出
// 新对象，调用方改返回值不影响存储。
func (h *HistoryStore) Load(ctx context.Context, key agent_v2.HistoryKey, q agent_v2.HistoryQuery) (agent_v2.HistoryPage, error) {
	ph := h.dialect.Placeholder
	var where strings.Builder
	args := []any{key.Owner, key.ConvID}
	where.WriteString("owner = " + ph(1) + " AND conv_id = " + ph(2))
	cond := func(col string, v any) {
		args = append(args, v)
		where.WriteString(" AND " + col + " = " + ph(len(args)))
	}
	if q.Before != "" {
		before, err := strconv.ParseInt(string(q.Before), 10, 64)
		if err != nil || before < 0 {
			return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: invalid cursor %q", q.Before)
		}
		args = append(args, before)
		where.WriteString(" AND id < " + ph(len(args)))
	}
	if q.After != "" {
		after, err := strconv.ParseInt(string(q.After), 10, 64)
		if err != nil || after < 0 {
			return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: invalid cursor %q", q.After)
		}
		args = append(args, after)
		where.WriteString(" AND id > " + ph(len(args)))
	}
	if q.MessageID != "" {
		cond("msg_id", q.MessageID)
	}
	if ids := q.MessageIDs(); len(ids) > 0 {
		params := make([]string, 0, len(ids))
		for _, id := range ids {
			args = append(args, id)
			params = append(params, ph(len(args)))
		}
		where.WriteString(" AND msg_id IN (" + strings.Join(params, ",") + ")")
	}
	if q.Task != "" {
		cond("task", q.Task)
	}
	if q.Agent != "" {
		cond("agent", q.Agent)
	}
	stmt := fmt.Sprintf(`SELECT id, payload FROM %s WHERE %s ORDER BY id`, h.table, where.String())
	// Limit = math.MaxInt 时多取一行会溢出成负数（PG / MySQL 拒绝负的 LIMIT），那么大的上限按不限处理。
	limited := q.Limit > 0 && q.Limit < math.MaxInt
	if limited {
		stmt += fmt.Sprintf(" DESC LIMIT %d", q.Limit+1)
	}
	rows, err := h.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: %w", err)
	}
	defer rows.Close()
	var (
		ids      []int64
		out      []*schema.Message
		verified []bool
	)
	for rows.Next() {
		var (
			id int64
			p  []byte
		)
		if err := rows.Scan(&id, &p); err != nil {
			return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: scan: %w", err)
		}
		rec := historyPayload{Message: new(schema.Message)}
		if err := json.Unmarshal(p, &rec); err != nil {
			return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: id %d 的消息解码失败: %w", id, err)
		}
		ids = append(ids, id)
		out = append(out, rec.Message)
		verified = append(verified, rec.VerifiedAt != "" && string(rec.VerifiedAt) == strconv.FormatInt(id, 10))
	}
	if err := rows.Err(); err != nil {
		return agent_v2.HistoryPage{}, fmt.Errorf("sql history load: %w", err)
	}
	more := false
	if limited {
		// 倒序读的：多出来的那一行是更早的一条，只说明还有更多
		if len(out) > q.Limit {
			more = true
			ids, out, verified = ids[:q.Limit], out[:q.Limit], verified[:q.Limit]
		}
		slices.Reverse(ids)
		slices.Reverse(out)
		slices.Reverse(verified)
	}
	if len(out) == 0 {
		return agent_v2.HistoryPage{}, nil
	}
	var boundaries map[int]agent_v2.HistoryCursor
	for i, ok := range verified {
		if ok {
			if boundaries == nil {
				boundaries = make(map[int]agent_v2.HistoryCursor)
			}
			boundaries[i] = agent_v2.HistoryCursor(strconv.FormatInt(ids[i], 10))
		}
	}
	return agent_v2.HistoryPage{Messages: out, VerifiedBoundaries: boundaries, Start: agent_v2.HistoryCursor(strconv.FormatInt(ids[0], 10)), More: more}, nil
}

// Setup 建表与索引，`IF NOT EXISTS`，重复调用幂等。
func (h *HistoryStore) Setup(ctx context.Context) error {
	ddl, err := h.historyMigrations()
	if err != nil {
		return err
	}
	for i, m := range ddl {
		if _, err := h.db.ExecContext(ctx, m); err != nil {
			return fmt.Errorf("sql history setup migration %d failed: %w", i+1, err)
		}
	}
	return nil
}

func (h *HistoryStore) lockTable() string {
	if name := h.table + "_writer"; len(name) <= 63 {
		return name
	}
	sum := sha256.Sum256([]byte(strings.ToLower(h.table)))
	return fmt.Sprintf("crew_history_writer_%x", sum[:16])
}

// historyMigrations 返各方言的建表语句。索引 (owner, conv_id, id) 让 Load 走索引、按 id 有序；三个过滤列不单独建
// 索引——同一场会话的行沿这个索引从新往旧扫、逐行比过滤列，命中凑够 Limit 就停。
func (h *HistoryStore) historyMigrations() ([]string, error) {
	t := h.table
	index := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_owner_conv_idx ON %s (owner, conv_id, id);`, t, t)
	lockDDL := func(keyType, suffix string) string {
		return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (owner %s NOT NULL, conv_id %s NOT NULL, PRIMARY KEY (owner, conv_id))%s",
			h.lockTable(), keyType, keyType, suffix)
	}
	switch h.dialect.(type) {
	case Postgres:
		// payload 用 BYTEA：存的是 JSON 字节，不在库里查字段；JSONB 会拒收 \u0000（见 Checkpointer 的建表注释）。
		return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id       BIGSERIAL PRIMARY KEY,
    owner    TEXT COLLATE "C" NOT NULL,
    conv_id  TEXT COLLATE "C" NOT NULL,
    payload  BYTEA NOT NULL,
    msg_id   TEXT COLLATE "C" NOT NULL DEFAULT '',
    task     TEXT COLLATE "C" NOT NULL DEFAULT '',
    agent    TEXT COLLATE "C" NOT NULL DEFAULT ''
);`, t), index, lockDDL(`TEXT COLLATE "C"`, "")}, nil
	case MySQL:
		// owner / conv_id 用 VARBINARY，理由同 Checkpointer 的身份列（utf8mb4_bin 是 PAD SPACE）；
		// 索引键 1020 + 1020 + 8 字节，在 InnoDB 3072 字节上限内。三个过滤列同样按字节比较，各 2048 字节（消息 ID、
		// RunID、编排位置都远短于此）。MySQL 没有 CREATE INDEX IF NOT EXISTS，索引随表一起建。
		return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id       BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    owner    VARBINARY(1020) NOT NULL,
    conv_id  VARBINARY(1020) NOT NULL,
    payload  LONGBLOB NOT NULL,
    msg_id   VARBINARY(2048) NOT NULL DEFAULT '',
    task     VARBINARY(2048) NOT NULL DEFAULT '',
    agent    VARBINARY(2048) NOT NULL DEFAULT '',
    INDEX %s_owner_conv_idx (owner, conv_id, id)
) ENGINE=InnoDB;`, t, t), lockDDL("VARBINARY(1020)", " ENGINE=InnoDB")}, nil
	case SQLite:
		// AUTOINCREMENT：删过的 id 不复用，行序永远等于写入序。
		return []string{fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    owner    TEXT NOT NULL,
    conv_id  TEXT NOT NULL,
    payload  BLOB NOT NULL,
    msg_id   TEXT NOT NULL DEFAULT '',
    task     TEXT NOT NULL DEFAULT '',
    agent    TEXT NOT NULL DEFAULT ''
);`, t), index, lockDDL("TEXT", "")}, nil
	default:
		return nil, errors.New("sql history Setup: 业务自定义 dialect 不支持 Setup, 业务自管 schema")
	}
}
