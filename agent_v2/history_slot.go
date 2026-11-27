package agent_v2

// 会话历史的**归属身份**——「这条消息是谁说的」用什么来标。
//
// # 为什么不能用 agent 自己的名字
//
// `Agent.Name()` 是**标签**，框架从不校验它唯一：两个分支挂同名 agent（甚至同一个
// 实例）时归属就撞了，它们的产出在历史里合并成一条流，下一轮互相把对方的消息当自己的
// prior 读回来，静默无报错。
//
// 而编排注册 sub-agent 时给的那个名字（`AddBranch("br1", …)` / `AddStep` /
// `AddAgent` / `AddSubAgent`）**框架是强制唯一的**（重名当场 panic），它才是那个 agent
// 在这棵编排树里的位置。位置唯一，归属就唯一。
//
// # 形态：斜杠分隔的位置路径
//
//	""                       顶层（没有父编排）
//	"br1"                    ParallelAgent 的 br1 分支
//	"outer/inner/actor"      嵌套三层
//
// 路径由父编排在调 sub-agent 前 push 进 ctx，子 agent 记历史时读出来当归属。
//
// # 注册成员跨执行保持同一归属
//
// 这是跟 graphNS（cp 寻址用）的**关键区别**，也是这套设计能成立的前提：
//
//	graphNS:  agent_billing_hop2   round_3   plan_step_7
//	归属路径: agent_billing        body      plan
//
// graphNS 要区分「同一个位置的第 N 次执行」（不然 cp 互覆），所以带运行时序号；归属要
// 回答「谁说的」，而同一个 agent 在同一位置、第 1 轮和第 3 轮说的话**归属本来就该相同**
// ——Loop 的 body 跑十轮都是 body，Handoff 的 front 在第 0 跳和第 2 跳都是 front。
//
// 更要命的是跳数/轮次**跨 Run 会重置**（hops 存 ctx、每个 Run 从 0 起）：同一个 agent
// 在 Run 1 记成 `agent_billing_hop1`、Run 2 续接时变 `agent_billing_hop0`，按它归属就
// **读不回自己上一轮的历史**。临时成员只属于创建它的任务和步骤，使用独立编码的临时位置。
//
// # 代价：归属是编排结构的函数，改结构就是换身份
//
// 位置来自编排注册名和嵌套层次，所以**改分支名、改步骤名、加一层 wrapper、把 agent 从
// 顶层挪进编排（或反过来）**，都会让它跟旧历史里的归属对不上——旧消息还在，但按新归属
// 读不回来，模型退化成失忆。要跨 Run 续接同一场会话，编排树的注册名得保持稳定。
//
// 这比旧方案（用 `Agent.Name()`）的脆弱面**更大**：旧的只有改名一个触发点。但方向仍是
// 改进，因为失效模式换了档：
//
//	旧：静默**合并**——两个同名 agent 的历史混成一条，互相把对方的话当自己的读回来，
//	    污染模型输出，而且越聊越乱
//	新：静默**丢失**——读不到旧历史，模型从这轮起失忆，但不会读到错的东西
//
// 丢失比污染轻：前者是能力退化，后者是给模型喂假上下文。两者都不报错，这是这一层
// 「旁路」定位的固有代价（见 historyWriter godoc）。
//
// ⚠️ **失效时是彻底空，不是退化到 events**：默认视图（SelfOnly）下归属对不上的消息被
// 整条丢弃，而 `historyPrior` 仍报「读成功」（后端确实读到了，只是没有属于我的），于是
// 调用方的「读失败才回落 session events」那条路走不到。业务观察到的症状是**完全失忆**
// 而不是「历史少了几条」——排查时别按「后端出问题了」的方向找。
//
// # 同一个 agent 挂在两个位置时，历史是分开的
//
// 框架自身就能走到这个形状：Routing 的某分支和 fallback 用同一个 agent、Plan 的子 agent
// 同时兼任 executor、Sequential 把同一个 agent 挂两步。此时它在两个位置的历史**互不可见**。
//
// 这是「位置即身份」的直接推论，多数时候正是想要的——同一个 writer 挂 `draft` / `polish`
// 两步时，polish 不该把 draft 的产出当成自己上一轮说的话（单个 Run 内不受影响：编排会把
// 上一步产出当派生 input 喂进去）。但若业务**有意复用一个专家 agent** 跨多个位置、指望它
// 有一份连续记忆，那份记忆现在拿不到——没有报错，只是 prior 变空。
//
// # 顶层为空是有意的
//
// 顶层 agent 没有父编排、拿不到注册名，此时回落 `Agent.Name()`。顶层同一时刻只有一个
// agent 在跑，不存在"谁跟谁撞"的问题；而回落到名字还能让业务回看历史时读到有意义的
// 标识，比空串友好。

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"crew-go/compose_v2"
	"crew-go/schema"
)

// historySlotCtxKey 是归属路径的 ctx key（私有类型防碰撞）。
type historySlotCtxKey struct{}

type historySegment string

var historyNameEscaper = strings.NewReplacer("%", "%25", "/", "%2F")

// namedHistorySegment 编码外部名称，保留斜杠为层级分隔符，保留前缀 % 给框架生成的位置。
func namedHistorySegment(name string) historySegment {
	return historySegment(historyNameEscaper.Replace(name))
}

// temporaryHistorySegment 只标识当前任务的这一步，下一任务创建的临时成员拥有自己的历史。
func temporaryHistorySegment(taskID string, step int) historySegment {
	return historySegment(fmt.Sprintf("%%tmp-%x-%d", taskID, step))
}

// WithHistorySlot 声明「接下来这个 sub-agent 在编排树里的位置」。
//
// 编排在调 sub-agent 前 push，push 的是**本层的注册名**（`AddBranch` / `AddStep` /
// `AddAgent` 那个 name），框架自动拼上父路径。
//
//	subCtx := agent_v2.WithHistorySlot(ctx, br.Name)   // "br1"，父路径自动带
//
// 业务自写编排（GraphAgent 的 NodeFunc 里调 sub-agent）时同样该 push，否则子 agent 的
// 产出会回落到用 agent 名标归属——**同名就会合并**。`RunSubAgent` helper 已经代劳。
//
// name 为空时原样透传。名称中的 % 和 / 转义为 %25 和 %2F，框架生成的临时位置独立编码。
//
// 同时开一张**回执**：从这里往下实际产生了多少条历史记录（见 historyReceipt）。边界补记
// 据此判断「这一步的执行体到底记了没有」。
func WithHistorySlot(ctx context.Context, name string) context.Context {
	return pushHistorySegment(ctx, namedHistorySegment(name))
}

func pushHistorySegment(ctx context.Context, segment historySegment) context.Context {
	if segment == "" {
		return ctx
	}
	name := string(segment)
	parent := HistorySlot(ctx)
	if parent != "" {
		name = parent + "/" + name
	}
	return openHistoryReceipt(context.WithValue(ctx, historySlotCtxKey{}, name))
}

// historyReceipt 属于这一次成员调用。写入方把具体消息登记到本层及祖先层，兄弟调用各有自己的回执。
// trail 保存跨 Resume 稳定的调用路径，历史行据此识别先前已经完成的子调用。
type historyReceipt struct {
	parent   *historyReceipt
	n        atomic.Int64
	outputs  sync.Map                       // historyOutputKey → struct{}
	input    atomic.Pointer[schema.Message] // 本调用前言实际入队的输入，子调用各用自己的回执。
	trail    string
	restored sync.Map // 本次成员调用实际恢复过的 *compose_v2.Checkpoint。
	replayed sync.Map // 已实际重放的子调用回复。
}

// historyOutputKey 同时核对身份与正文。无 ID 的消息仅在同次调用内可按对象辨认。
type historyOutputKey struct {
	id        string
	anonymous *schema.Message
	payload   string
}

func historyOutputOf(m *schema.Message) historyOutputKey {
	if m == nil {
		return historyOutputKey{}
	}
	body := *m
	// 历史层盖章前后的消息具有同样的业务内容，业务自己修改的标注仍属于产出。
	body.Annotations = maps.Clone(m.Annotations)
	for _, k := range []string{HistoryAgentAnnotation, HistoryAgentDisplayAnnotation, HistoryTaskAnnotation,
		historyOriginAnnotation, historyScopeAnnotation, historyRecoveredInputAnnotation, hostedHistoryTaskAnnotation,
		CallerHistoryAnnotation, historyWriteReceipt, historyWriteFallback, historyCommitAnnotation} {
		delete(body.Annotations, k)
	}
	key := historyOutputKey{id: m.ID, payload: inputDigest(&body)}
	if m.ID == "" {
		key.anonymous = m
	}
	return key
}

const historyScopeAnnotation = "crew_history_scopes"

type detachedHistoryScopesKey struct{}

func (detachedHistoryScopesKey) ExecutionState() {}

func historyScopeTrail(ctx context.Context) string {
	if r := historyReceiptFrom(ctx); r != nil {
		return r.trail
	}
	trail, _ := ctx.Value(detachedHistoryScopesKey{}).(string)
	return trail
}

type historyReceiptCtxKey struct{}

// openHistoryReceipt 开一张新回执，父指向当前这张。
func openHistoryReceipt(ctx context.Context) context.Context {
	// namespace 区分同一位置的不同轮或调用，各维按原始字节加长度编码。
	task, ns := currentWriterTask(ctx), compose_v2.GetGraphNS(ctx)
	identity := identityString(task, HistorySlot(ctx), ns)
	trail := historyScopeTrail(ctx) + identityHash(identity) + "/"
	if ns != "" {
		trail += historyExecutionToken(task, ns)
	}
	parent := historyReceiptFrom(ctx)
	r := &historyReceipt{parent: parent, trail: trail}
	return context.WithValue(ctx, historyReceiptCtxKey{}, r)
}

// withHistoryRestoreSource 保留图实际接管的只读源快照。只有查到匹配的旧历史行时才解码消息。
func withHistoryRestoreSource(ctx context.Context, opts []compose_v2.RunOption) []compose_v2.RunOption {
	r := historyReceiptFrom(ctx)
	if r == nil || historyWriterFrom(ctx) == nil {
		return opts
	}
	return append(opts, compose_v2.WithRestoreSource(func(cp *compose_v2.Checkpoint) { r.restored.Store(cp, struct{}{}) }))
}

// retainedOutput 检查当前图从老存档恢复的消息以及本次重放的子调用结果。两者都须由持久历史印证。
func (r *historyReceipt) retainedOutput(key historyOutputKey) bool {
	if r == nil {
		return false
	}
	if _, ok := r.replayed.Load(key); ok {
		return true
	}
	found := false
	r.restored.Range(func(source, _ any) bool {
		found = checkpointHasHistoryOutput(source.(*compose_v2.Checkpoint), key)
		return !found
	})
	return found
}

func checkpointHasHistoryOutput(cp *compose_v2.Checkpoint, key historyOutputKey) bool {
	matches := func(m *schema.Message) bool { return m != nil && m.ID == key.id && historyOutputOf(m) == key }
	for _, v := range cp.Channels {
		typ, _ := compose_v2.LookupType(v.Type)
		switch typ {
		case reflect.TypeFor[*schema.Message]():
			var m *schema.Message
			if json.Unmarshal(v.Data, &m) == nil && matches(m) {
				return true
			}
		case reflect.TypeFor[[]*schema.Message]():
			var msgs []*schema.Message
			if json.Unmarshal(v.Data, &msgs) == nil && slices.ContainsFunc(msgs, matches) {
				return true
			}
		}
	}
	return false
}

func noteReplayedHistoryOutput(ctx context.Context, final *schema.Message) {
	r := historyReceiptFrom(ctx)
	if r == nil || final == nil || final.ID == "" || historyWriterFrom(ctx) == nil {
		return
	}
	key := historyOutputOf(final)
	for ; r != nil; r = r.parent {
		r.replayed.Store(key, struct{}{})
	}
}

// historyExecutionToken 让直接按 namespace 续子图的调用找回原执行，调用方无需重建父级的历史位置路径。
func historyExecutionToken(task, ns string) string {
	return "@" + identityHash(identityString(task, ns)) + "/"
}

func historyReceiptFrom(ctx context.Context) *historyReceipt {
	r, _ := ctx.Value(historyReceiptCtxKey{}).(*historyReceipt)
	return r
}

// registerHistoryOutputs 只登记本次实际写入或重放的消息。输入侧另用范围计数判断成员是否接管历史。
func registerHistoryOutputs(ctx context.Context, msgs []*schema.Message) {
	r := historyReceiptFrom(ctx)
	if r == nil {
		return
	}
	for cur := r; cur != nil; cur = cur.parent {
		cur.n.Add(1)
	}
	for _, m := range msgs {
		if m == nil || isDerivedInput(m) {
			continue
		}
		key := historyOutputOf(m)
		for cur := r; cur != nil; cur = cur.parent {
			cur.outputs.Store(key, struct{}{})
		}
	}
}

// producedHistoryHere 报「这次调用的范围内有没有产生过历史记录」。
//
// 边界补记据它判「输入归不归里面那个 agent 管」：里面记过东西 ⟹ 边界不插手。代价（已知取舍）：业务自研的外壳 agent
// 改写了输入再委派给框架的 BasicAgent 时，BasicAgent 记的是改写后的那条，边界见范围里有记录就不补——用户原话在整条
// 历史里一份都没有，下一轮模型看到的会话里用户从没说过这句话（外壳原样透传时正常一份）。这道范围判据不能放松：它同时
// 挡着「OnAgentStart 脱敏之后边界又把钩子之前的原文写进历史」。外壳要留原话，就在委派之前自己记一笔，或把改写放进
// BasicAgent 的钩子里做。
func producedHistoryHere(ctx context.Context) bool {
	r := historyReceiptFrom(ctx)
	return r != nil && r.n.Load() > 0
}

// HistorySlot 返回当前 agent 在编排树里的位置路径；顶层返空串。
func HistorySlot(ctx context.Context) string {
	if v, ok := ctx.Value(historySlotCtxKey{}).(string); ok {
		return v
	}
	return ""
}

// historyOwner 定这一层记历史时该盖什么归属。
//
// 有位置就用位置（编排强制唯一），没有就回落 agent 名（顶层场景，见本文件头部说明）。
func historyOwner(ctx context.Context, agentName string) string {
	if slot := HistorySlot(ctx); slot != "" {
		return slot
	}
	return string(namedHistorySegment(agentName))
}

// settleHistoryOnPause 在子调用以「可 Resume 的方式」退出时结算**输入**——只落输入，
// 产出这时还没有。
//
// # 为什么中断时就要落，不等成功
//
// 输入的持久点应在**跨进边界那一刻**，不在「执行完成那一刻」——完成与否只影响产出，
// 不影响输入（对照文档「结论七」：查了四家，没有一家把输入的持久化系在执行完成上）。
// 边界补记原来把输入的落笔系在「子调用成功」上，于是外部执行体（远程 agent / 业务
// 自研壳——它们不知道历史层的存在）中断时，输入只活在父编排的调用栈里：经父 Resume
// 那条路能自愈（step 重跑成功、补记跟上），但调用方**直接** Resume 子时输入永久缺席，
// 历史落成「有答无问」。cp 那一半没有这个洞（原始输入在父的 channel 里随 cp 走、父
// Resume 时恢复），缺的只是历史层这半。
//
// # 判据与成功路径的输入半边同一套（走 settleHistorySlot）
//
//   - BasicAgent 子：preamble 在本次调用里已记 ⟹ 范围回执 >0 ⟹ 跳过，不双记
//   - 外部执行体：什么都没记 ⟹ 落输入，owner = 父给的位置
//   - 父 Resume 重跑成功后的补记：跨 Run 守卫（inputAlreadyInHistorySlot，同一条流上
//     查落盘记录）认出这条 ⟹ 不再记
//   - 再次中断（多轮 HITL）：同上被跨 Run 守卫挡住
//   - Loop 第 2 轮起的输入是上一轮产出（assistant 角色）⟹ Role==user 过滤掉，不落垃圾
//
// # 只在「这个错会留下可 Resume 的存档」时做
//
// 判据是 `LeavesResumableCheckpoint`——框架对这个错的**处置意图**，不是错的类型（枚举
// IsInterrupt 会漏软取消 / CondFailRerunFrom 两档，GraphAgent 那轮踩过）。纯业务硬错
// 不落：cp 标 Failed、这一层不可 Resume，「报错的外部执行体不记」是既有语义。
func settleHistoryOnPause(ctx context.Context, agentName string, input *schema.Message, err error) *schema.Message {
	if err == nil || !compose_v2.LeavesResumableCheckpoint(err) {
		return nil
	}
	return settleHistoryAt(ctx, HistorySlot(ctx), agentName, gateExtentProduced, input, nil, true)
}

// settleHistorySlot 在编排调完 sub-agent 之后补记这一步——**只补执行体自己没记的**。
//
// # 为什么需要它
//
// 历史的写入点在 BasicAgent 的三个节点里。但编排收的是 `Agent` 接口，业务塞进来的可以是
// 任何实现：远程 agent、自研 agent、适配别的框架的壳——它们连历史层的存在都不知道，跑完
// 什么也不留。
//
// 后果不是「少一步」而是**整条断链**：`Sequential(远程A, 本地B)` 里，第一步的输入交给远程
// agent（没人记），第二步的输入是编排合成的派生输入（按判据跳过不记，因为「成分都能从历史
// 重算」——但那个前提此刻不成立了）。实测结果是历史里只剩本地 B 的一条产出，用户原话和远程
// 产出双双缺席，下一轮模型看到一个没有任何前因的结论。
//
// # 判据：输入和产出各用各的尺
//
// 两样东西问的不是同一个问题，用一把尺必然占其一失效模式：
//
//   - **输入**问「这一步归谁记」。执行体只要往历史里写过一条（`producedHistoryHere`，
//     见 historyReceipt），输入就是它的事，边界一律不插手。这条同时挡住一个安全问题：
//     `OnAgentStart` 是 [消费] 档，业务可以在那儿把消息整条换掉（脱敏是 CLAUDE.md 举的
//     例子），叶子记的是脱敏版，而边界握着的是**闭包里改写之前**那条、带原文——按范围
//     判就不会写进去。
//   - **产出**按本次调用回执里的消息身份和内容核对，跨续跑按实际输入或已完成后代的历史核对。
//     兄弟收到独立输入时，各自拥有自己返回的回复；成员改写输入的正文、工具数据或业务标注时，改写后的回复另记。
//
// 判据都不看 agent 类型——业务自研的 agent 只要自己接了历史层照样算「记过了」，不会被
// 当外部 agent 补一遍。
//
// 输入那条还有一道 `Role == user` 的过滤，跟 BasicAgent 自己记输入时同款
// （`basic_agent.go`）：编排把上一轮的 assistant 产出转手当下一轮的输入是常规形状，
// 那不是「用户说了句新的话」。
//
// ⚠️ **它现在是纵深防御，不是承重的那道**：消息的身份改成构造期铸造之后（见
// `rootInputID`），身份跨拷贝存活，上一轮产出转手当输入时按身份就认得出来，下面那道
// 认领已经接管了这个形状。实测撤掉这行**零测试失败**。留着是因为语义上它仍然对——
// assistant 消息不是「用户说了句新的话」——而且它挡的是「身份判据够不着」的残余情形
// （产出在 A 位置记的、转手到 B 位置当输入）。**改判据时别把它当哨兵**：它红不了。
//
// 输入那条**还要再问一遍身份，但问的是「在这条流里」**（`claimRecordHere`）：Loop 第 N
// 轮的输入就是第 N-1 轮的产出、同一条流 ⟹ 跳过；而 SelfOnly 扇出的兄弟分支共享同一条
// 输入身份、是不同的流 ⟹ 各记各的（每条分支流要各自完整，有问有答）。用全局判会变成
// 「谁先谁赢」，实测并发下份数随调度在 1~4 之间翻。
//
// 两处认领都是**查与登记一次完成**，不是「先查后写」——中间那个窗口正是随调度翻的来源。
//
// # 记什么
//
// 输入 + 最终产出，都盖这一步的位置。中间过程（工具往返、多轮）拿不到也不该编——外部
// agent 的内部过程本来就不属于这条会话，父这一层看到的就是「给了它什么、它返回了什么」。
//
// input 为 nil 或 final 为 nil 时各自跳过（有什么记什么）。
func settleHistorySlot(ctx context.Context, agentName string, input, final *schema.Message) {
	settleHistoryAt(ctx, HistorySlot(ctx), agentName, gateExtentProduced, input, final, true)
}

// settleHistoryTopLevel 让**顶层执行体自己**结算边界，位置取自己的名字。
//
// # 为什么需要它（而不是让顶层往 ctx 里推一个 slot）
//
// `settleHistorySlot` 的 `slot == "" → return` 那道守卫拦的是「顶层不归编排管」——那对
// 有静态子列表的 7 编排成立（叶子自己会记，wrapper 那层不需要记）。GraphAgent 不成立：
// 它的图可以全是 NodeFunc，一个会记的叶子都没有，而它自己的 InputWriter / OutputReader
// **就是**这条流的主路产出。
//
// 让它往 ctx 里推 `WithHistorySlot(ctx, name)` 也能过守卫，但**副作用是把自己的名字变成
// 所有后代归属的根段**：图里经 RunSubAgent 调起的子从 `worker` 变成 `<图名>/worker`。
// 实测那样有两个代价——(1) 顶层图改个名，它下面所有子的历史全部变孤儿（改动前改名零
// 影响）；(2) 同一张图，配置走 `WithHistoryCtx` 时归属带前缀、业务把 `WithHistory` 挂在
// 叶子上时归属是裸的，两种配法互不相认。而这两个代价换来的「跟嵌套时形状一致」其实只是
// 段数一致：嵌套时前缀取的是**父给的注册名**，顶层取的是**agent 自己的名字**，两个命名域。
//
// 所以位置只传给这一次结算（做认领的流键 + 过守卫），不进 ctx。归属仍按
// `historyOwner` 的既定回落取 agentName，后代归属逐字不变。
func settleHistoryTopLevel(ctx context.Context, agentName string, input, final *schema.Message) {
	settleHistoryAt(ctx, string(namedHistorySegment(agentName)), agentName, gateInputRecorded, input, final, true)
}

// deferRootInput 让顶层执行体（GraphAgent）这次 Run 的原始输入排在这次 Run 写进历史的第一批记录前面。
//
// 出口才结算的话，图里经 RunSubAgent 调起的子 agent 在执行中记下的问答排在用户那句话之前（探针实测）。也不能在入口
// 直接记：节点常把这条输入原样交给子 agent（转发），子 agent 的视图只看自己名下的记录，它要自己持有这一问才完整，入口
// 先记一份的话同一句话就记成两份。所以登记成「待补」，由写入器在下一批记录入队时决定（见 historyWriter.submit）：
// 这条输入已经记过（那一批就是子 agent 记它）⟹ 不补；否则先补它、再写那一批。整个 Run 一批都没有时出口照旧结算。
//
// 门槛跟出口那次结算的输入那条相同：user 角色、不是派生输入、这条流里跨 Run 没记过；这次 Run 里已经记过（OnAgentStart 钩子
// 里起的调用把它原样交给了会记历史的 agent）也不登记——那时记下它的调用已经跑完、已经入队。入口判过的这条，出口结算不再读
// 一次历史（rootVetted）。
//
// 份数由第一批是谁的决定：被转发的子 agent 记下这条输入之前已经有别的记录入队（委派在先、它自己的钩子先起了子调用、
// 并发扇出里委派的分支先写）时，补一份归顶层，转发的那个子 agent 再记自己的一份（每条流各自完整，同 SelfOnly 扇出各记
// 一份）；用户那句话都排在图执行写下的第一批之前（OnAgentStart 钩子里起的调用写的排在它前面）。待补只覆盖图执行那一段：图
// 跑完之后（含业务的输出读取器 panic、OnAgentEnd 钩子里起的调用）写的不再带出它（endRootInputWindow），记不记交给出口结算，
// 终局失败不记。
func deferRootInput(ctx context.Context, agentName string, input *schema.Message) {
	w := historyWriterFrom(ctx)
	owner := historyOwner(ctx, agentName)
	if w == nil || historySuppressed(ctx) || agentName == "" || input == nil ||
		input.Role != schema.RoleUser || isDerivedInput(input) || recordedInThisRun(ctx, input) ||
		inputAlreadyInHistorySlot(ctx, owner, input) {
		return
	}
	w.deferRoot(input, w.stamp(ctx, input, owner, agentName), owner)
}

// endRootInputWindow 在顶层 GraphAgent 的图跑完时调：还没补的原始输入收回，交给出口结算（见 deferRootInput）。
func endRootInputWindow(ctx context.Context) {
	if w := historyWriterFrom(ctx); w != nil {
		w.dropPendingRoot()
	}
}

// inputGate 决定输入那条问哪个问题——两个调用方的**前提不同**，用同一个问题会错一个。
type inputGate int

const (
	// gateExtentProduced：「这次调用范围内有没有产生记录」。
	//
	// 边界补记（`settleHistorySlot`）用它，前提是**执行体收到的就是这条输入**：编排把
	// 输入原样传给子，子只要往历史里写过一条，输入就是它的事。7 编排都满足这个前提。
	gateExtentProduced inputGate = iota

	// gateInputRecorded：「这条输入本 Run 记过没有」。
	//
	// 顶层自结算（`settleHistoryTopLevel`）用它，因为上面那个前提在它身上**不成立**：
	// 那里的「执行体」是整张图，而图底下的子 agent 记的是它自己的**派生输入**。
	// 「有人记过东西」≠「有人记过这条输入」——图一委派派生任务，用户原话就 0 条
	//（实测：委派形状原话 0，转发形状原话 1；中断→Resume 那条更要紧，中断出口结算本来
	// 就是专门为「Resume 拿不到原始 input」加的，被同一道闸绕过就白加了）。
	//
	// ⚠️ **不能把边界补记也换成这个**：实测会红 5 条（改写 hook 防原文泄漏 ×2 /
	// 扇出每分支各记一份 / 转交跨进程不重复 / 扇出中断 Resume 与基线一致）——那道闸对
	// 边界补记是承重的。
	gateInputRecorded
)

// settleHistoryAt 补写边界历史，返回本次实际入队的输入快照。
func settleHistoryAt(ctx context.Context, slot, agentName string, gate inputGate, input, final *schema.Message, recordInput bool) *schema.Message {
	w := historyWriterFrom(ctx)
	if w == nil || historySuppressed(ctx) || slot == "" {
		return nil
	}

	inputTakenCareOf := func() bool {
		if gate == gateInputRecorded {
			return recordedInThisRun(ctx, input)
		}
		return producedHistoryHere(ctx)
	}
	// 跨 Run 有没有在这条流里记过：顶层入口已经按同一条判过的（deferRootInput）不再读一次历史——这次 Run 里补没补、
	// 记没记都在写入器的登记表上，上面那条已经问过
	recordedBefore := func() bool {
		if gate == gateInputRecorded && w.rootVetted(input) {
			return false
		}
		return inputAlreadyInHistorySlot(ctx, slot, input)
	}

	// 认领放在**最后一个条件**：抢到就一定会记，不会登记了却没写。
	var msgs []*schema.Message
	queuedInput := recordInput && input != nil && input.Role == schema.RoleUser && !isDerivedInput(input) &&
		!inputTakenCareOf() && !recordedBefore() && w.claimRecordHere(slot, input)
	if queuedInput {
		msgs = append(msgs, input)
	}
	// 兄弟可能独立返回同一个对象，仅本次调用的回执、实际输入或真实子调用能证明透传。
	if final != nil && !isDerivedInput(final) {
		key := historyOutputOf(final)
		wroteInput := len(msgs) > 0 && historyOutputOf(input) == key
		if !wroteInput && !historyOutputRecorded(ctx, slot, input, key) {
			msgs = append(msgs, final)
		}
	}
	if len(msgs) == 0 {
		return nil
	}
	records := recordHistoryBatch(ctx, historyOwner(ctx, agentName), agentName, msgs, historyRecordOptions{})
	if queuedInput && len(records) != 0 {
		return records[0]
	}
	return nil
}

// inputAlreadyInAnyStream 报这条输入是不是已经在历史里了——**不限归属**。
//
// 只给**共享单流**的位置用（HandoffAgent 转交模式）：整场对话是一条流，原始输入在流里
// 任何归属下已在，就不该再落——「共享视图下同一条原始输入只记一份」是既有语义。
//
// ⚠️ **SelfOnly 扇出（Parallel 分支）不能用这个**：每条分支流要各自完整（有问有答），
// 兄弟分支先落的那份**不该**挡住我——那边走 per-slot 的 `inputAlreadyInHistorySlot`。
// 同一个「限不限归属」的开关，两类位置的正确答案相反（实测：Handoff 跨进程 Resume 用
// per-slot 查会把输入落成两份、扇出用 any-stream 查会让后到的分支流缺问）。
//
// 只在**当前任务**的记录里找（按消息 ID 与任务章查，见 [findHistory]）：要挡的是同一个任务里转交的下一跳、
// 续跑重跑到这一步时再记一份，那些记录都是本任务写的。
func inputAlreadyInAnyStream(ctx context.Context, input *schema.Message) bool {
	if input == nil {
		return false
	}
	nonce := rootInputID(input)
	if nonce == "" {
		return false
	}
	hit, _ := findHistory(ctx, nil, HistoryQuery{MessageID: nonce}, true, anyHistoryRecord)
	return hit
}

// historyOutputRecorded 先查本调用的回执，再查持久历史。跨边界的回复只认相同身份和内容，
// 且必须来自实际输入或本次调用的后代。旧行没有执行路径时，核对当前调用真正重放或恢复的消息。
func historyOutputRecorded(ctx context.Context, slot string, input *schema.Message, key historyOutputKey) bool {
	r := historyReceiptFrom(ctx)
	if r != nil {
		if _, ok := r.outputs.Load(key); ok {
			return true
		}
	}
	if key.id == "" {
		return false
	}
	forwarded := input != nil && input.ID == key.id && historyOutputOf(input) == key
	task := currentWriterTask(ctx)
	retained := sync.OnceValue(func() bool { return r.retainedOutput(key) })
	hit, _ := findHistory(ctx, nil, HistoryQuery{MessageID: key.id}, false, func(m *schema.Message) bool {
		if !historyRecordOfTask(m, task) {
			return false
		}
		owner, _ := m.Annotations[HistoryAgentAnnotation].(string)
		producer, _ := m.Annotations[HistoryTaskAnnotation].(string)
		trail, _ := m.Annotations[historyScopeAnnotation].(string)
		other := owner != slot || producer != task
		descendant := false
		if trail != "" && r != nil {
			_, replayed := r.replayed.Load(key)
			descendant = strings.HasPrefix(trail, r.trail) && (other || trail != r.trail || replayed)
			if ns := compose_v2.GetGraphNS(ctx); !descendant && HistorySlot(ctx) == "" && ns != "" {
				descendant = strings.Contains(trail, historyExecutionToken(task, ns)) && other
			}
		} else if other {
			inSubtree := HistorySlot(ctx) == "" || (producer == task && strings.HasPrefix(owner, slot+"/"))
			descendant = inSubtree && retained()
		}
		// 本位置若已写下原始用户输入，出口回传原话也只保留这一份。
		return ((forwarded && (other || input.Role == schema.RoleUser)) || descendant) && historyOutputOf(m) == key
	})
	if hit {
		for cur := r; cur != nil; cur = cur.parent {
			cur.outputs.Store(key, struct{}{})
		}
	}
	return hit
}

// historyConfigProvider 让编排能看到 sub-agent 的历史配置。
//
// 编排收的是 `Agent` 接口，本来看不到配置——但**写入器必须由编排这一层拥有**，否则
// 编排调用「不是 BasicAgent 的执行体」时 ctx 里还没有写入器（它只在 BasicAgent.Run
// 里建、且活在它自己的子 ctx 里），边界补记无从落笔。
type historyConfigProvider interface {
	historyCfgForOrchestration() *historyConfig
}

// historySubsProvider 让借配置的那一趟能穿过嵌套的编排：编排交出自己的直接子执行体（见 history_subs.go）。
type historySubsProvider interface {
	historySubs() []Agent
}

// borrowHistoryConfig 从 subs 的整棵静态子树里借历史配置：BasicAgent 交出自己的，编排往下走自己的子执行体（嵌套的
// 编排也一样，见 walkHistorySubs）。整棵树都没配时返回 (nil, nil)；有两处不同时返回冲突的那两处。
//
// 往下走是为了让最外层的编排拥有写入器：只看直接子时，`Sequential[Sequential[chat]]` 的外层借不到配置、入口不开写入器，
// 内层在外层的节点里开——这场对话唯一的写入器成了嵌套写入器，调用方提供的前文与历史层就分不清谁说了算（见
// caller_history.go）。GraphAgent 没有静态子列表，走不进去；它的配置只能来自 WithHistoryCtx。
func borrowHistoryConfig(subs []Agent) (*historyConfig, *historyConflict) {
	var chosen *historyConfig
	var chosenBy string
	var conflict *historyConflict
	exhausted := walkHistorySubs(subs, func(sub Agent) bool {
		hp, ok := sub.(historyConfigProvider)
		if !ok {
			return true
		}
		hc := hp.historyCfgForOrchestration()
		switch {
		case hc == nil:
		case chosen == nil:
			chosen, chosenBy = hc, sub.Name()
		case !sameHistoryConfig(chosen, hc):
			conflict = &historyConflict{names: [2]string{chosenBy, sub.Name()}, cfgs: [2]*historyConfig{chosen, hc}}
			return false
		}
		return true
	})
	if exhausted && historyBorrowExhaustedWarned.CompareAndSwap(false, true) {
		slog.Warn("agent_v2: 借历史配置时子执行体超过上限还没走完——组合里多半有不可比较的值类型包装成的环，"+
			"更深处配的历史层借不到（本条每个进程只记一次）", "limit", historyBorrowMaxVisits)
	}
	if conflict != nil {
		return nil, conflict
	}
	return chosen, nil
}

// ConfiguredHistoryStore 从 Agent 的静态定义查找历史后端，不读取请求上下文。
// 未配置或多个成员的历史配置不一致时返回 nil。
func ConfiguredHistoryStore(ag Agent) HistoryStore {
	hc, _ := borrowHistoryConfig([]Agent{ag})
	if hc == nil || isFrameworkNil(hc.store) {
		return nil
	}
	return hc.store
}

// historyBorrowMaxVisits 是借配置这一趟最多看多少个子执行体。嵌套编排的规模远到不了这个数；到了多半是不可比较的值
// 类型包装成了环（认不出走过，分叉的环按层数算是指数级）。
const historyBorrowMaxVisits = 1024

var historyBorrowExhaustedWarned atomic.Bool

// walkHistorySubs 深度优先把 subs 的整棵静态子树逐个交给 visit（visit 返回 false 时停下）：编排往下走它的子执行体
// （historySubsProvider）。走过的不再走——同一个实例出现在几处、组合成了环（转交候选里有自己）：按接口值本身认，指针按
// 地址、内嵌编排指针的值类型包装按值。曾经只认指针，值类型包装成环时无限递归、整个进程栈溢出（不配历史层也一样，每个
// 编排的 Run 入口都要走这一趟）。不可比较的值认不出走过，靠总数上限停下。返回是不是因为看满上限停下的。
func walkHistorySubs(subs []Agent, visit func(Agent) bool) (exhausted bool) {
	visited := make(map[any]bool)
	budget := historyBorrowMaxVisits
	var walk func(subs []Agent) bool
	walk = func(subs []Agent) bool {
		for _, sub := range subs {
			if IsNilAgent(sub) {
				continue
			}
			if reflect.ValueOf(sub).Comparable() {
				if visited[sub] {
					continue
				}
				visited[sub] = true
			}
			if budget == 0 {
				exhausted = true
				return false
			}
			budget--
			if !visit(sub) {
				return false
			}
			if sp, ok := sub.(historySubsProvider); ok && !walk(sp.historySubs()) {
				return false
			}
		}
		return true
	}
	walk(subs)
	return exhausted
}

// historyConflict 是借配置时两处不一样的历史配置：哪两个 agent、各自的配置。
type historyConflict struct {
	names [2]string
	cfgs  [2]*historyConfig
}

// Error 说清哪两个 agent、差在哪：owner 不同、owner 函数不是同一个、store 不是同一个。
func (c *historyConflict) Error() string {
	a, b := c.cfgs[0], c.cfgs[1]
	var diff string
	switch {
	case a.owner != b.owner:
		diff = fmt.Sprintf("owner %q vs %q", a.owner, b.owner)
	case !sameOwnerFunc(a, b):
		diff = "WithHistoryOwnerFunc 不是同一个函数（按函数值比：返回值相同的两个函数也算不同）"
	default:
		diff = "不是同一个 HistoryStore"
	}
	return fmt.Sprintf("sub-agents %q and %q declare conflicting history config (%s)", c.names[0], c.names[1], diff)
}

// startHistoryForOrchestration 让编排在自己这一层建历史写入器。
//
// 配置从 sub-agent 借（整棵静态子树，见 [borrowHistoryConfig]）：业务是在 BasicAgent 上配 `WithHistory` 的，编排自己
// 没有这个 option。「一场会话一条流」本来就意味着同一编排下的子 agent 共用一个写入器。
//
// # 配置不一致时 fail-fast，不静默挑一个
//
// 原来取的是「第一个配了的」，理由写作「谁的配置不重要」。**那句话在 owner 和 store
// 两个维度上不成立**：子 agent 配了不同的 store 或不同的 owner 时，整场对话落到哪里
// 取决于挑中了谁。而 Handoff / Plan 的子是 `map[string]Agent`，`for range` 顺序每次
// 都不同——实测连跑 6 次，同一套编排把对话写进了 A/A/B/B/A/A 两个不同租户的 store。
// 跨 Run 的后果更糟：Run 1 写租户 A、Run 2 写租户 B、Run 3 读到哪半看运气，而落进 B
// 的内容本身是 A 的对话。
//
// 所以配置只要有一处不同就直接报错。常见情形（所有子共用同一个 store 和 owner）不受
// 影响；`historySubs()` 那边也已经把 map 的迭代顺序定死，报错信息跨次稳定。
//
// 幂等：ctx 里已有写入器（更外层编排建过）或这条分支被摘除时直接返回。都没配历史时
// 返回原 ctx + no-op，零开销。
//
// **写入器的生命周期因此变成整个编排**而不是某一步：出口 closeAndJoin 排空所有子
// agent 的写入，这也是「一场会话一条流」该有的形状。
func startHistoryForOrchestration(ctx context.Context, subs []Agent, name string) (context.Context, func(), error) {
	noop := func() {}
	if historyWriterFrom(ctx) != nil || historySuppressed(ctx) {
		return ctx, noop, nil
	}
	// 运行入口声明优先——它更靠近这次运行，也是唯一能覆盖「编排里一个 BasicAgent 都
	// 没有」那种形状的声明方式。见 WithHistoryCtx。
	if hc := historyCfgFromCtx(ctx); hc != nil {
		return startHistory(ctx, hc, name)
	}

	chosen, conflict := borrowHistoryConfig(subs)
	if conflict != nil {
		// 报出冲突的是哪两个 agent、差在哪——不然业务在一棵嵌套的编排树里找不到
		return ctx, noop, fmt.Errorf("agent %q: %s — 一场会话只能有一条流，框架无法替业务选租户；"+
			"请让同一编排下（含嵌套的编排）的子 agent 用同一个 WithHistory 配置（同一个 store 实例、同一个 owner 或同一个 owner 函数）",
			name, conflict.Error())
	}
	return startHistory(ctx, chosen, name)
}

// sameHistoryConfig 判两个子的历史配置是不是同一套。
//
// 只要有一处不同就算冲突——落错租户是静默的数据事故，宁可误报也不漏报。
func sameHistoryConfig(a, b *historyConfig) bool {
	if a.owner != b.owner || !sameOwnerFunc(a, b) {
		return false
	}
	return sameHistoryStore(a.store, b.store)
}

// sameOwnerFunc 判两份配置的 owner 函数是不是同一个（按函数值比）。
func sameOwnerFunc(a, b *historyConfig) bool {
	if (a.ownerFunc == nil) != (b.ownerFunc == nil) {
		return false
	}
	return a.ownerFunc == nil || reflect.ValueOf(a.ownerFunc).Pointer() == reflect.ValueOf(b.ownerFunc).Pointer()
}

// sameHistoryStore 判两个 store 是不是同一个。
//
// 不能直接用 `==`：`HistoryStore` 是接口，业务的实现若是不可比较的值类型（含 map /
// slice 字段的 struct 按值实现接口），`==` 会 panic。按种类分别处理，比不了的按类型
// 相同当作同一个——那一档宁可漏报也不误伤。
func sameHistoryStore(a, b HistoryStore) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	ra, rb := reflect.ValueOf(a), reflect.ValueOf(b)
	if ra.Type() != rb.Type() {
		return false
	}
	switch ra.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Chan, reflect.Map, reflect.Func:
		return ra.Pointer() == rb.Pointer()
	}
	return true
}

// inputAlreadyInHistorySlot 报这条输入是不是已经在**指定那条流**的历史里了。
//
// 转交场景下同一句原始输入会透传给每一跳，各跳都调边界补记——不查一下就会记 N 份。
// 判据跟 BasicAgent 那边同源：输入自带的身份（见 rootInputID），扫历史
// 找有没有同 nonce 的。没有 nonce 的输入（非转交场景）直接放行——那种情况下每个位置
// 收到的本来就是不同的输入。
// ⚠️ **slot 必须由调用方传进来，不能自己从 ctx 取**：`settleHistoryAt` 的 slot 是显式参数，
// 顶层自结算那条路上它等于 agentName 而 `HistorySlot(ctx)` 恒是空串。自己取 ctx 就变成
// 「拿空串去比对 owner=agentName 的记录」——恒不命中，整道守卫在顶层路径上是死代码
// （实测后果：同一个 sid 上 Run→中断→再 Run，即便调用方传的是**同一个 message 指针**，
// 用户那句话也会被记两遍）。
func inputAlreadyInHistorySlot(ctx context.Context, slot string, input *schema.Message) bool {
	nonce := rootInputID(input)
	if nonce == "" {
		return false
	}
	// 按流限定：跨 Run 续接时前一个 Run 在**这条流**上的记录能挡住重记，而兄弟分支
	// 刚写进去的那份挡不住我——每条分支流各自要完整（有问有答）。只查当前任务的记录：
	// 要挡的「同一个 sid 上 Run → 中断 → 再 Run」是同一个任务。归属在 match 里比——slot 为空时
	// 比的是「没有归属」，不能当成「不按归属过滤」。
	hit, _ := findHistory(ctx, nil, HistoryQuery{MessageID: nonce}, true, func(m *schema.Message) bool {
		return priorHasRootInputIDInSlot([]*schema.Message{m}, nonce, slot)
	})
	return hit
}

// resolveHistoryConfigForAbort 解析这次调用该往哪个历史里写——ctx 声明优先，
// 否则从 sub-agent 借（同 [startHistoryForOrchestration] 的判据，只是不建写入器）。
//
// 配置不一致时**不报错，返 nil**：这条路是 Abort 的收尾补记，不该在业务放弃任务的时候
// 再抛一个配置错。真正的 fail-fast 留在 `startHistoryForOrchestration`——正常跑一次就会撞上。
func resolveHistoryConfigForAbort(ctx context.Context, subs []Agent) *historyConfig {
	if hc := historyCfgFromCtx(ctx); hc != nil {
		return hc
	}
	chosen, _ := borrowHistoryConfig(subs) // 冲突时 chosen 为 nil：这条路上不报错，理由见上
	return chosen
}

// CloseAbandonedTaskHistory 为原历史分区里该任务尚无结果的工具调用补齐放弃结果。
// 调用方须持有该任务的放弃许可，且已删除根存档。重试会跳过已有结果的调用。
func CloseAbandonedTaskHistory(ctx context.Context, destination TaskHistoryDestination, task string) error {
	return closeTaskHistory(ctx, destination, task, "任务已被放弃，此工具调用没有结果")
}

// CloseTerminalTaskHistory 为已终结任务补写缺失的工具结果。调用方须持有任务许可并完成清档。
// 补写只表明没有记录到工具结果，工具可能已执行。重试跳过已有结果的调用。
func CloseTerminalTaskHistory(ctx context.Context, destination TaskHistoryDestination, task string) error {
	return closeTaskHistory(ctx, destination, task, "任务已结束，未记录此工具调用的结果")
}

func closeTaskHistory(ctx context.Context, destination TaskHistoryDestination, task, missingResult string) error {
	store, key := destination.Store, destination.Key
	if task == "" || isFrameworkNil(store) || key.Owner == "" || key.ConvID == "" {
		return nil
	}
	src := historySource{store: store, key: key, task: task}
	var msgs []*schema.Message // 新 → 旧
	err := src.scanBackward(ctx, HistoryQuery{Task: task}, historyPageMin, true, func(m *schema.Message, _ HistoryCursor) bool {
		msgs = append(msgs, m)
		return false
	}, nil)
	if err != nil {
		return err
	}

	slices.Reverse(msgs) // 历史扫描从新到旧，配对需要追加顺序。
	var closers []*schema.Message
	for _, call := range danglingToolCalls(msgs) {
		if call.ID == "" {
			continue
		}
		m := call.declaration
		owner, _ := m.Annotations[HistoryAgentAnnotation].(string)
		display, _ := m.Annotations[HistoryAgentDisplayAnnotation].(string)
		closer := schema.ToolResultMessage(call.ID, missingResult)
		closers = append(closers, stampHistoryAgent(closer, owner, display, task))
	}
	if len(closers) == 0 {
		return nil
	}
	if err := compose_v2.CheckCheckpointTaskLease(ctx); err != nil {
		return err
	}
	return store.Append(ctx, key, closers)
}

// beginRunActivity 先校验本次调用的断点，再登记本次执行和要放弃的任务。
func beginRunActivity(ctx context.Context, cfg runConfig, session *RunSession, ag Agent) (context.Context, *runActivity, error) {
	if err := cfg.breakpoints.validate(ag); err != nil {
		return ctx, nil, err
	}
	tasks, agentName := cfg.abandoned, ag.Name()
	ids := make([]string, 0, len(tasks))
	var settled []string
	for _, task := range tasks {
		id := task.id
		if id == "" || id == session.RunID {
			return ctx, nil, rejectCall(ctx, "agent %q: WithAbandonedTasks 给了 %q——放弃的是这次 Run 之前的任务，不能是空的，"+
				"也不能是这次 Run 自己的任务（RunID %q）", agentName, id, session.RunID)
		}
		if detachedFromRunning(ctx, id) {
			return ctx, nil, rejectCall(ctx, "agent %q: WithAbandonedTasks 给了 %q——它是脱离出来之前所在的任务，此刻正在执行，不能放弃",
				agentName, id)
		}
		if task.settled {
			settled = append(settled, id)
		} else {
			ids = append(ids, id)
		}
	}
	_, nested := enclosingTask(ctx)
	admittedID, _ := ctx.Value(rootRunAdmissionKey{}).(string)
	// 透传的自研入口可能已经声明新开，但尚未有内建入口取得存档准入。
	freshRoot := !nested || (compose_v2.IsFreshRunCtx(ctx) && !compose_v2.WithinNodeScope(ctx) && admittedID != session.RunID)
	begin := compose_v2.BeginTaskActivity
	if freshRoot {
		begin = compose_v2.BeginFreshTaskActivity
	}
	ctx, task, err := begin(ctx, session.RunID, ids, settled...)
	if err != nil {
		return ctx, nil, fmt.Errorf("agent %q: %w", agentName, err)
	}
	activity := &runActivity{TaskActivity: task}
	if !freshRoot {
		receive := func(result json.RawMessage) error {
			var events []*AgentEvent
			if err := json.Unmarshal(result, &events); err != nil {
				return err
			}
			noteReplayedHistoryOutput(ctx, FinalMessageByAgent(events, agentName))
			activity.replay = &events
			return nil
		}
		ctx, err = compose_v2.EnterNestedAgent(ctx, session.RunID, reflect.TypeOf(ag).String(), HistorySlot(ctx), historyScopeTrail(ctx), receive)
		if err != nil {
			task.Release()
			return ctx, nil, err
		}
	}
	if freshRoot {
		activity.runID = session.RunID
		activity.initial = compose_v2.GetCheckpointer(ctx)
		activity.initialOwner = compose_v2.GetOwner(ctx)
		ctx = context.WithValue(ctx, rootRunAdmissionKey{}, session.RunID)
	}
	return ctx, activity, nil
}

// abandonEntryTimeout 是入口删档和补写历史的共享预算。存储调用超时后记日志并继续新 Run。
var abandonEntryTimeout = 10 * time.Second

// abandonTasksAtEntry 用入场时已取得许可的旧任务分区清档。删档成功后补齐当前历史写入器中的旧工具结果。
// 补写完成后释放放弃许可，调用方再记录本次输入并触发钩子。
func abandonTasksAtEntry(ctx context.Context, activity *compose_v2.TaskActivity, tasks []abandonedTask, agentName string) {
	defer activity.FinishAbandon()
	if len(tasks) == 0 {
		return
	}
	sctx, cancel := context.WithTimeout(ctx, abandonEntryTimeout)
	defer cancel()
	w := historyWriterFrom(ctx)
	const ns = ""
	session := GetSession(ctx)
	for _, task := range tasks {
		id := task.id
		if task.settled {
			clearTaskApprovalRules(session, id)
			continue
		}
		cper, admitted := activity.AbandonedCheckpoint(id)
		if !admitted {
			slog.WarnContext(ctx, "agent_v2: 放弃任务的存档分区未确认",
				"agent", agentName, "task", id, "ns", ns)
			continue
		}
		recorded, _ := taskApprovalRoot(session, id)
		if recorded != nil {
			same, err := compose_v2.ConfirmCheckpointPartition(sctx, id, cper, recorded)
			if err != nil || !same {
				slog.WarnContext(ctx, "agent_v2: 放弃任务的存档分区未确认",
					"agent", agentName, "task", id, "ns", ns, "err", err)
				continue
			}
		}
		if cper != nil {
			start := time.Now()
			n, err := cper.DeleteSubtree(sctx, id, ns)
			compose_v2.EmitCpCleanupHelper(ctx, id, ns, "abandoned", n, time.Since(start).Milliseconds(), err)
			if err != nil {
				slog.WarnContext(ctx, "agent_v2: 放弃任务时删存档失败（存档可能还在、任务还能续），悬空的工具调用不补",
					"agent", agentName, "task", id, "ns", ns, "err", err)
				continue
			}
		}
		if w != nil {
			if err := CloseAbandonedTaskHistory(sctx, TaskHistoryDestination{Key: w.key, Store: w.store}, id); err != nil {
				slog.WarnContext(ctx, "agent_v2: 放弃任务的历史收尾失败", "agent", agentName, "task", id, "err", err)
				if cper != nil && recorded == nil {
					rememberTaskApprovalRoot(session, id, cper, ns)
				}
				continue
			}
		}
		clearTaskApprovalRules(session, id)
	}
}
