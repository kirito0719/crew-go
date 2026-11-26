package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"crew-go/compose_v2"
	"crew-go/model"
	"crew-go/observability"
	agentRedis "crew-go/redis"
	"crew-go/schema"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
)

type pauseModel struct {
	once    sync.Once
	started chan struct{}
	proceed chan struct{}
}

func (m *pauseModel) WithTools([]*schema.ToolInfo) (model.ChatModel, error) { return m, nil }

func (m *pauseModel) Generate(ctx context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	m.once.Do(func() { close(m.started) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.proceed:
		return schema.AssistantMessage("完成"), nil
	}
}

func (m *pauseModel) Stream(context.Context, []*schema.Message, ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	return nil, model.ErrStreamUnsupported
}

func TestPlanWebSharedSessionLifecycle(t *testing.T) {
	redisURL := os.Getenv("PLAN_WEB_TEST_REDIS_URL")
	options := &goredis.Options{}
	if redisURL == "" {
		options.Addr = miniredis.RunT(t).Addr()
	} else {
		var err error
		options, err = goredis.ParseURL(redisURL)
		if err != nil {
			t.Fatal("PLAN_WEB_TEST_REDIS_URL 无效")
		}
	}
	m := &pauseModel{started: make(chan struct{}), proceed: make(chan struct{})}
	opened := false
	t.Cleanup(func() {
		if !opened {
			close(m.proceed)
		}
	})
	newReplica := func() *httptest.Server {
		rdb := goredis.NewClient(options)
		t.Cleanup(func() { _ = rdb.Close() })
		app := &chatServer{
			model: m, obs: observability.New(), sessions: &redisSessions{client: rdb, ttl: time.Minute},
			cp:             agentRedis.NewCheckpointer(rdb, agentRedis.WithCheckpointerPrefix("crew-plan-web")),
			requestTimeout: 5 * time.Second, durable: true,
		}
		server := httptest.NewServer(app.routes())
		t.Cleanup(server.Close)
		return server
	}
	a, b := newReplica(), newReplica()
	owner := rand.Text()
	client := &http.Client{Timeout: 8 * time.Second}
	type response struct {
		code int
		body string
		err  error
	}
	request := func(method, url, who string) response {
		req, err := http.NewRequestWithContext(t.Context(), method, url, nil)
		if err != nil {
			return response{err: err}
		}
		req.AddCookie(&http.Cookie{Name: ownerCookie, Value: who})
		resp, err := client.Do(req)
		if err != nil {
			return response{err: err}
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return response{code: resp.StatusCode, body: string(body), err: err}
	}
	run := make(chan response, 1)
	go func() { run <- request(http.MethodGet, a.URL+"/api/chat?q=start", owner) }()
	select {
	case <-m.started:
	case resp := <-run:
		t.Fatalf("模型还没启动: %+v", resp)
	case <-time.After(5 * time.Second):
		t.Fatal("模型没有启动")
	}
	list := request(http.MethodGet, b.URL+"/api/sessions?status=running&limit=1", owner)
	var sessions []sessionRecord
	if list.err != nil || list.code != http.StatusOK || json.Unmarshal([]byte(list.body), &sessions) != nil || len(sessions) != 1 {
		t.Fatalf("另一个副本没有看到运行中的会话: %+v", list)
	}
	id := sessions[0].ID
	resumeURL := b.URL + "/api/chat?session_id=" + id
	sessionURL := b.URL + "/api/sessions/" + id
	t.Cleanup(func() {
		req, _ := http.NewRequest(http.MethodDelete, sessionURL, nil)
		req.AddCookie(&http.Cookie{Name: ownerCookie, Value: owner})
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	})
	if got := request(http.MethodGet, sessionURL, rand.Text()); got.code != http.StatusNotFound {
		t.Fatalf("其他会话身份看到了任务: %+v", got)
	}
	if got := request(http.MethodGet, resumeURL, owner); got.code != http.StatusConflict {
		t.Fatalf("并发续跑没有被拒: %+v", got)
	}
	if got := request(http.MethodDelete, sessionURL, owner); got.code != http.StatusConflict {
		t.Fatalf("并发删除没有被拒: %+v", got)
	}
	if got := request(http.MethodPost, b.URL+"/api/interrupt?session_id="+id, owner); got.code != http.StatusOK {
		t.Fatalf("跨副本暂停失败: %+v", got)
	}
	close(m.proceed)
	opened = true
	if got := <-run; got.err != nil || !strings.Contains(got.body, `"status":"interrupted"`) {
		t.Fatalf("没有挂起: %+v", got)
	}
	if got := request(http.MethodGet, resumeURL, owner); got.err != nil || !strings.Contains(got.body, `"status":"success"`) {
		t.Fatalf("跨副本续跑失败: %+v", got)
	}
	if got := request(http.MethodDelete, sessionURL, owner); got.code != http.StatusNoContent {
		t.Fatalf("删除会话失败: %+v", got)
	}
	if got := request(http.MethodGet, sessionURL, owner); got.code != http.StatusNotFound {
		t.Fatalf("删除后会话仍存在: %+v", got)
	}
	if got := request(http.MethodGet, a.URL+"/api/chat?q=temporary&ephemeral=true", owner); got.err != nil || !strings.Contains(got.body, `"status":"success"`) {
		t.Fatalf("临时会话未完成: %+v", got)
	}
	if got := request(http.MethodGet, b.URL+"/api/sessions", owner); got.code != http.StatusOK || strings.TrimSpace(got.body) != "[]" {
		t.Fatalf("完成的临时会话仍然可见: %+v", got)
	}
}

func TestPlanWebInMemorySessionExpiry(t *testing.T) {
	store := newMemorySessions(time.Minute)
	key := sessionKey{owner: rand.Text(), id: "demo"}
	if err := store.update(t.Context(), key, func(*sessionRecord) (*sessionRecord, error) {
		return &sessionRecord{ID: key.id, Status: statusSuccess, LastActivity: time.Now().Add(-time.Hour)}, nil
	}); err != nil {
		t.Fatal(err)
	}
	app := &chatServer{sessions: store, cp: compose_v2.NewInMemoryCheckpointer()}
	_, err := app.load(t.Context(), key)
	if !errors.Is(err, errSessionMissing) {
		t.Fatalf("过期的终态会话应已清理: %v", err)
	}
}

func TestPlanWebHTTPSProxySessionLifecycle(t *testing.T) {
	const ttl = time.Minute
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	mini := miniredis.RunT(t)
	redis := goredis.NewClient(&goredis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = redis.Close() })
	m := &pauseModel{started: make(chan struct{}), proceed: make(chan struct{})}
	proceed := sync.OnceFunc(func() { close(m.proceed) })
	defer proceed()
	app := &chatServer{model: m, obs: observability.New(),
		sessions: &redisSessions{client: redis, ttl: ttl},
		cp:       agentRedis.NewCheckpointer(redis, agentRedis.WithCheckpointerPrefix("crew-plan-web")),
		durable:  true, secureCookie: true,
	}
	origin := httptest.NewServer(app.routes())
	t.Cleanup(origin.Close)
	target, err := url.Parse(origin.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewTLSServer(httputil.NewSingleHostReverseProxy(target))
	t.Cleanup(proxy.Close)
	newBrowser := func() *http.Client {
		t.Helper()
		jar, err := cookiejar.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		return &http.Client{Transport: proxy.Client().Transport, Jar: jar}
	}
	type result struct {
		resp *http.Response
		body []byte
		err  error
	}
	request := func(browser *http.Client, method, path string) result {
		req, err := http.NewRequestWithContext(ctx, method, proxy.URL+path, nil)
		if err != nil {
			return result{err: err}
		}
		resp, err := browser.Do(req)
		if err != nil {
			return result{err: err}
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return result{resp: resp, body: body, err: err}
	}
	check := func(got result, status int) {
		t.Helper()
		if got.err != nil || got.resp.StatusCode != status || got.resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("代理响应应为 %d 且禁止缓存，响应=%v 正文=%s 错误=%v", status, got.resp, got.body, got.err)
		}
	}

	browser, visitor := newBrowser(), newBrowser()
	home := request(browser, http.MethodGet, "/")
	check(home, http.StatusOK)
	cookies := home.resp.Cookies()
	if len(cookies) != 1 || cookies[0].Name != ownerCookie || !validOwner(cookies[0].Value) ||
		!cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode || cookies[0].Path != "/" {
		t.Fatalf("HTTPS 代理下的会话 Cookie 不正确: %v", cookies)
	}
	check(request(visitor, http.MethodGet, "/"), http.StatusOK)
	stream := make(chan result, 1)
	go func() { stream <- request(browser, http.MethodGet, "/api/chat?q=start") }()
	select {
	case <-m.started:
	case stopped := <-stream:
		t.Fatalf("启动模型前连接已结束，正文=%s 错误=%v", stopped.body, stopped.err)
	case <-ctx.Done():
		t.Fatal("代理后的模型未启动")
	}

	list := request(browser, http.MethodGet, "/api/sessions?status=running")
	check(list, http.StatusOK)
	var rows []sessionRecord
	if err := json.Unmarshal(list.body, &rows); err != nil || len(rows) != 1 || rows[0].ID == "" {
		t.Fatalf("同一 Cookie 应能查到正在运行的会话: %s, %v", list.body, err)
	}
	id := rows[0].ID
	detail := "/api/sessions/" + id
	resume := "/api/chat?session_id=" + id
	check(request(visitor, http.MethodGet, detail), http.StatusNotFound)
	check(request(browser, http.MethodGet, detail), http.StatusOK)

	if got := request(browser, http.MethodPost, "/api/interrupt?session_id="+id); got.err != nil || got.resp.StatusCode != http.StatusOK {
		t.Fatalf("暂停失败，正文=%s 错误=%v", got.body, got.err)
	}
	proceed()
	paused := <-stream
	check(paused, http.StatusOK)
	if !strings.Contains(string(paused.body), `"status":"interrupted"`) {
		t.Fatalf("应通过代理看到挂起事件: %s", paused.body)
	}
	mini.FastForward(2 * ttl)
	check(request(browser, http.MethodGet, detail), http.StatusOK)
	resumed := request(browser, http.MethodGet, resume+"&action=continue")
	check(resumed, http.StatusOK)
	if !strings.Contains(string(resumed.body), `"status":"success"`) {
		t.Fatalf("超过终态 TTL 的挂起会话应仍能续跑: %s", resumed.body)
	}
	check(request(browser, http.MethodGet, resume), http.StatusGone)
	check(request(browser, http.MethodGet, detail), http.StatusOK)
	mini.FastForward(2 * ttl)
	check(request(browser, http.MethodGet, detail), http.StatusNotFound)
	expired := request(browser, http.MethodGet, "/api/sessions")
	check(expired, http.StatusOK)
	if strings.TrimSpace(string(expired.body)) != "[]" {
		t.Fatalf("终态过期后应从会话索引消失: %s", expired.body)
	}
}

type gatedSessionEdit struct {
	sessionStore
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *gatedSessionEdit) update(ctx context.Context, key sessionKey, edit func(*sessionRecord) (*sessionRecord, error)) error {
	return s.sessionStore.update(ctx, key, func(r *sessionRecord) (*sessionRecord, error) {
		s.once.Do(func() { close(s.reached) })
		<-s.release
		return edit(r)
	})
}

func TestPlanWebExpiredLeaseCannotCommitSession(t *testing.T) {
	mini := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: mini.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	key := sessionKey{owner: rand.Text(), id: "fenced-transaction"}
	cp := agentRedis.NewCheckpointer(client, agentRedis.WithCheckpointerPrefix("crew-plan-web"))
	store := &redisSessions{client: client}
	if err := store.update(t.Context(), key, func(*sessionRecord) (*sessionRecord, error) {
		return &sessionRecord{ID: key.id, Status: statusRunning}, nil
	}); err != nil {
		t.Fatal(err)
	}
	gate := &gatedSessionEdit{sessionStore: store, reached: make(chan struct{}), release: make(chan struct{})}
	a := &chatServer{cp: cp, sessions: gate}
	ctx, release, err := a.acquire(a.taskContext(t.Context(), key.owner), key.id, false)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	finished := make(chan error, 1)
	go func() { finished <- a.finish(ctx, key, statusInterrupted, "old", 0) }()
	select {
	case <-gate.reached:
	case err := <-finished:
		t.Fatalf("didn't reach watched metadata edit: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out reaching watched metadata edit")
	}
	defer func() {
		select {
		case <-gate.release:
		default:
			close(gate.release)
		}
	}()
	mini.FastForward(time.Minute)
	leases := cp.CheckpointTaskLeases()
	if ok, err := leases.TryAcquireTaskLease(t.Context(), key.id, "other-process-token", time.Minute); err != nil || !ok {
		t.Fatalf("could not acquire successor lease: %t %v", ok, err)
	}
	close(gate.release)
	finishErr := <-finished
	rec, readErr := store.get(t.Context(), key)
	if !errors.Is(finishErr, compose_v2.ErrTaskLeaseLost) || readErr != nil || rec == nil || rec.Status != statusRunning {
		t.Fatalf("transaction succeeded with expired token: err=%v record=%+v read err=%v", finishErr, rec, readErr)
	}
}
