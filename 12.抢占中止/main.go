// 声明当前文件属于 main 包，作为程序入口
package main

// 导入依赖包
import (
	// 导入内存会话存储包
	"myloop/mem"
	// 导入 checkpoint 存储包
	adkstore "myloop/store"
	// 导入字节缓冲区包
	"bytes"
	// 导入上下文包，用于控制请求生命周期
	"context"
	// 导入 JSON 编解码包
	"encoding/json"
	// 导入错误处理包
	"errors"
	// 导入格式化 I/O 包
	"fmt"
	// 导入 I/O 基础接口包
	"io"
	// 导入日志包
	"log"
	// 导入操作系统接口包
	"os"
	// 导入文件路径操作包
	"path/filepath"
	// 导入排序包
	"sort"
	// 导入字符串操作包
	"strings"
	// 导入同步原语包（Mutex、Map 等）
	"sync"
	// 导入时间包
	"time"

	// 导入 A2UI 流式渲染包
	"myloop/a2ui"

	// 导入通用工具包（审批结果等）
	commontool "github.com/cloudwego/eino-examples/adk/common/tool"
	// 导入图工具包，用于将工作流封装为工具
	"github.com/cloudwego/eino-examples/adk/common/tool/graphtool"
	// 导入批量处理节点包
	"github.com/cloudwego/eino-examples/compose/batch/batch"
	// 导入本地后端包（文件系统、Shell 能力）
	localbk "github.com/cloudwego/eino-ext/adk/backend/local"
	// 导入 CozeLoop 回调包（可观测性）
	clc "github.com/cloudwego/eino-ext/callbacks/cozeloop"
	// 导入 Eino ADK 核心包
	"github.com/cloudwego/eino/adk"
	// 导入 Deep Agent 预构建包
	"github.com/cloudwego/eino/adk/prebuilt/deep"
	// 导入全局回调处理包
	"github.com/cloudwego/eino/callbacks"
	// 导入模型组件接口包
	"github.com/cloudwego/eino/components/model"
	// 导入工具组件接口包
	"github.com/cloudwego/eino/components/tool"
	// 导入编排组件包（Workflow、Graph 等）
	"github.com/cloudwego/eino/compose"
	// 导入消息模式包
	"github.com/cloudwego/eino/schema"
	// 导入 Hertz Web 框架的应用包
	"github.com/cloudwego/hertz/pkg/app"
	// 导入 Hertz HTTP 服务器包
	hserver "github.com/cloudwego/hertz/pkg/app/server"
	// 导入 Hertz HTTP 状态码常量包
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	// 导入 CozeLoop 客户端包
	"github.com/coze-dev/cozeloop-go"
	// 导入 UUID 生成包
	"github.com/google/uuid"
	// 导入 SSE（Server-Sent Events）包
	"github.com/hertz-contrib/sse"

	// 导入 DeepSeek 模型实现包
	"github.com/cloudwego/eino-ext/components/model/deepseek"
)

// init 函数在包初始化时自动执行
func init() {
	// 注册 ChatItem 类型的序列化名称，用于 checkpoint 持久化
	schema.RegisterName[ChatItem]("chatwitheino_chat_item")
	// 注册 ApprovalResult 类型的序列化名称，用于 checkpoint 持久化
	schema.RegisterName[commontool.ApprovalResult]("chatwitheino_approval_result")
}

// ChatItem 是 TurnLoop 的输入项类型，每条用户消息或审批决定都作为一个 ChatItem 推入循环
type ChatItem struct {
	// 用户消息文本（审批项时为空）
	Query string
	// 审批结果，非 nil 表示此项携带审批决定
	ApprovalResult *commontool.ApprovalResult
	// 此审批项要解决的 interrupt ID
	InterruptID string
}

// errInterrupted 是 OnAgentEvents 在 Agent 被中断等待审批时返回的错误，TurnLoop 将其作为退出原因
var errInterrupted = errors.New("agent interrupted for approval")

// iterEnvelope 将事件迭代器从 OnAgentEvents 传递给 HTTP handler
type iterEnvelope struct {
	// Agent 产出的事件异步迭代器
	events *adk.AsyncIterator[*adk.TypedAgentEvent[*schema.Message]]
	// 当前会话的历史消息快照
	history []*schema.Message
	// 用于 HTTP handler 将消费结果回传给 OnAgentEvents 的 channel
	done chan iterResult
}

// iterResult 携带 HTTP handler 的消费结果，回传给 OnAgentEvents
type iterResult struct {
	// 最后一条助手回复的文本内容
	lastContent string
	// 中间消息列表（工具调用 + 工具结果），用于持久化到会话
	intermediates []*schema.Message
	// 中断 ID（如果 Agent 被暂停等待审批，则非空）
	interruptID string
	// A2UI 组件索引计数器
	msgIdx int
	// 流式处理过程中发生的错误
	err error
}

// sessionTurnState 持有每个会话的 TurnLoop 和事件桥接 channel
type sessionTurnState struct {
	// 互斥锁，保护并发访问
	mu sync.Mutex
	// 该会话的 TurnLoop 实例
	loop *adk.TurnLoop[*ChatItem, *schema.Message]
	// OnAgentEvents 向 HTTP handler 发送事件迭代器的 channel
	iterReady chan iterEnvelope
	// HTTP handler 向 OnAgentEvents 回传消费结果的 channel
	iterDone chan iterResult
	// 关闭此 channel 通知旧的 handler 退出（抢占时）
	handlerDone chan struct{}
}

// CozeLoop 工作空间 ID
const cozeLoopWorkspaceId = "7480846041387237385"

// 会话数据目录
const sessionsDir = "data/sessions"

// main 函数，程序入口
func main() {

	// 创建根上下文
	ctx := context.Background()

	// 从环境变量获取 CozeLoop API Token
	cozeLoopApiToken := os.Getenv("COZELOOP_API_TOKEN")
	// 如果配置了 CozeLoop Token，则启用可观测性追踪
	if cozeLoopApiToken != "" {
		// 创建 CozeLoop 客户端
		client, err := cozeloop.NewClient(
			// 使用 API Token 认证
			cozeloop.WithAPIToken(cozeLoopApiToken),
			// 指定工作空间 ID
			cozeloop.WithWorkspaceID(cozeLoopWorkspaceId),
		)
		// 如果创建客户端失败，终止程序
		if err != nil {
			log.Fatalf("创建 CozeLoop 客户端失败: %v", err)
		}
		// 函数结束时延迟关闭客户端（等待 5 秒确保数据上报完成）
		defer func() {
			// 等待 5 秒，确保所有追踪数据上报完成
			time.Sleep(5 * time.Second)
			// 关闭客户端
			client.Close(ctx)
		}()
		// 将 CozeLoop 注册为全局回调处理器
		callbacks.AppendGlobalHandlers(clc.NewLoopHandler(client))
		// 记录 CozeLoop 已启用
		log.Println("CozeLoop tracing enabled")
	} else {
		// 记录 CozeLoop 未启用
		log.Println("CozeLoop tracing disabled (set COZELOOP_API_TOKEN to enable)")
	}

	// 从环境变量获取 DeepSeek API Key
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	// 如果未设置 API Key，终止程序
	if apiKey == "" {
		log.Fatalf("请设置环境变量 DEEPSEEK_API_KEY")
	}
	// 创建 DeepSeek 聊天模型实例
	chatModel, err := deepseek.NewChatModel(ctx, &deepseek.ChatModelConfig{
		// 设置 API Key
		APIKey: apiKey,
		// 设置 API 基础 URL
		BaseURL: "https://api.deepseek.com",
		// 设置模型名称为 deepseek-v4-flash
		Model: "deepseek-v4-flash",
	})
	// 如果创建模型失败，终止程序
	if err != nil {
		log.Fatalf("创建模型失败: %v", err)
	}
	// 创建文档问答工作流工具
	workflowTool, err := createWorkflowTool(ctx, chatModel)
	// 如果创建工作流失败，终止程序
	if err != nil {
		log.Fatalf("创建流程失败: %v", err)
	}

	// 创建中间件列表
	handlers := []adk.ChatModelAgentMiddleware{
		// 注册审批中间件（工具调用前需人工审批）
		newApprovalMiddleware[*schema.Message](),
		// 注册安全工具中间件（捕获工具错误，防止管道崩溃）
		newSafeToolMiddleware[*schema.Message](),
	}
	// 创建本地后端（提供文件系统和 Shell 操作能力）
	backend, err := localbk.NewBackend(ctx, &localbk.Config{})
	// 如果创建后端失败，终止程序
	if err != nil {
		log.Fatalf("创建文件系统失败: %v", err)
	}
	// ========== 2. 创建智能体 ==========
	// 使用 deep.New 创建深度 Agent（支持工具调用的多轮循环 Agent）
	agent, err := deep.New(ctx, &deep.Config{
		// Agent 名称
		Name: "调用工具智能体",
		// Agent 描述
		Description: "调用工具智能体",
		// 配置使用的聊天模型
		ChatModel: chatModel,
		// 配置后端（提供文件系统操作能力）
		Backend: backend,
		// 配置流式 Shell（提供命令执行能力）
		StreamingShell: backend,
		// 配置工具列表
		ToolsConfig: adk.ToolsConfig{
			// 工具节点配置
			ToolsNodeConfig: compose.ToolsNodeConfig{
				// 注册工作流工具
				Tools: []tool.BaseTool{workflowTool},
			},
		},
		// 配置模型重试策略
		ModelRetryConfig: &adk.ModelRetryConfig{
			// 最大重试次数
			MaxRetries: 5,
			// 判断是否可重试的错误（429/限流相关错误）
			IsRetryAble: func(_ context.Context, err error) bool {
				// 检查是否包含 429 状态码
				return strings.Contains(err.Error(), "429") ||
					// 检查是否包含 Too Many Requests
					strings.Contains(err.Error(), "Too Many Requests") ||
					// 检查是否包含 qpm limit（每分钟请求限制）
					strings.Contains(err.Error(), "qpm limit")
			},
		},
		// 最大迭代次数（Agent 循环调用工具的上限）
		MaxIteration: 50,
		// 注册中间件处理器
		Handlers: handlers,
	})

	// 如果创建 Agent 失败，终止程序
	if err != nil {
		log.Fatalf("创建智能体失败: %v", err)
	}

	// 创建内存 checkpoint 存储（用于审批中断状态的保存与恢复）
	checkpointStore := adkstore.NewInMemoryStore()

	// 创建会话存储（基于 JSONL 文件持久化）
	store, err := mem.NewStore(sessionsDir)

	// 从环境变量获取工作目录
	workspaceDir := os.Getenv("WORKSPACE_DIR")
	// 如果未设置，使用默认值
	if workspaceDir == "" {
		workspaceDir = "./data/workspace"
	}

	// 从环境变量获取项目根目录
	projectRoot := os.Getenv("PROJECT_ROOT")
	// 如果未设置，使用当前工作目录
	if projectRoot == "" {
		// 获取当前工作目录
		if cwd, err := os.Getwd(); err == nil {
			projectRoot = cwd
		}
	}
	// 将项目根目录转为绝对路径
	if abs, err := filepath.Abs(projectRoot); err == nil {
		projectRoot = abs
	}
	// 记录项目根目录
	log.Printf("project root: %s", projectRoot)

	// 从环境变量获取示例目录
	examplesDir := os.Getenv("EXAMPLES_DIR")
	// 如果未设置，尝试使用项目根目录下的 examples 子目录
	if examplesDir == "" {
		// 拼接候选路径
		candidate := filepath.Join(projectRoot, "examples")
		// 检查候选路径是否存在且为目录
		if fi, err := os.Stat(candidate); err == nil && fi.IsDir() {
			examplesDir = candidate
		} else {
			// 否则使用项目根目录本身
			examplesDir = projectRoot
		}
	}
	// 将示例目录转为绝对路径
	if abs, err := filepath.Abs(examplesDir); err == nil {
		examplesDir = abs
	}
	// 记录示例目录
	log.Printf("examples dir: %s", examplesDir)

	// 设置 HTTP 服务端口
	port := "8085"

	// 创建服务器实例
	srv := &server{
		// Agent 实例
		agent: agent,
		// 会话存储
		store: store,
		// checkpoint 存储
		checkpointStore: checkpointStore,
		// 工作目录
		workspaceDir: workspaceDir,
		// 项目根目录
		projectRoot: projectRoot,
		// 示例目录
		examplesDir: examplesDir,
	}

	// 创建 Hertz HTTP 服务器
	h := hserver.Default(hserver.WithHostPorts(":" + port))

	// 注册根路由，返回前端页面
	h.GET("/", func(_ context.Context, c *app.RequestContext) {
		// 读取 index.html 文件
		data, err := os.ReadFile("static/index.html")
		// 如果读取失败，返回 404
		if err != nil {
			c.JSON(consts.StatusNotFound, map[string]string{"error": "index.html not found"})
			return
		}
		// 返回 HTML 页面内容
		c.Data(consts.StatusOK, "text/html; charset=utf-8", data)
	})

	// 注册创建会话路由
	h.POST("/sessions", func(_ context.Context, c *app.RequestContext) {
		// 生成新的 UUID 作为会话 ID
		id := uuid.New().String()
		// 在存储中创建新会话
		if _, err := store.GetOrCreate(id); err != nil {
			// 创建失败，返回 500 错误
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		// 返回新创建的会话 ID
		c.JSON(consts.StatusOK, map[string]string{"id": id})
	})

	// 注册获取会话列表路由
	h.GET("/sessions", func(_ context.Context, c *app.RequestContext) {
		// 获取所有会话元数据
		metas, err := store.List()
		// 如果获取失败，返回 500 错误
		if err != nil {
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		// 如果没有会话，返回空数组
		if metas == nil {
			metas = []mem.SessionMeta{}
		}
		// 返回会话列表
		c.JSON(consts.StatusOK, metas)
	})

	// 注册删除会话路由
	h.DELETE("/sessions/:id", func(_ context.Context, c *app.RequestContext) {
		// 获取路径参数中的会话 ID
		id := c.Param("id")
		// 停止该会话正在运行的 TurnLoop
		ts := srv.getTurnState(id)
		// 加锁保护
		ts.mu.Lock()
		// 如果存在 TurnLoop
		if ts.loop != nil {
			// 立即停止 TurnLoop
			ts.loop.Stop(adk.WithImmediate())
			// 清空 loop 引用
			ts.loop = nil
		}
		// 解锁
		ts.mu.Unlock()
		// 从存储中删除会话
		if err := store.Delete(id); err != nil {
			// 删除失败，返回 500 错误
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		// 从 turnStates 中删除会话状态
		srv.turnStates.Delete(id)
		// 返回 204 无内容
		c.Status(consts.StatusNoContent)
	})

	// 注册聊天路由（支持抢占）
	h.POST("/sessions/:id/chat", func(ctx context.Context, c *app.RequestContext) {
		// 调用聊天处理函数
		srv.handleChat(ctx, c)
	})

	// 注册渲染会话历史路由
	h.GET("/sessions/:id/render", func(_ context.Context, c *app.RequestContext) {
		// 调用渲染处理函数
		srv.handleRender(c)
	})

	// 注册审批路由
	h.POST("/sessions/:id/approve", func(ctx context.Context, c *app.RequestContext) {
		// 调用审批处理函数
		srv.handleApprove(ctx, c)
	})

	// 注册中止路由
	h.POST("/sessions/:id/abort", func(ctx context.Context, c *app.RequestContext) {
		// 调用中止处理函数
		srv.handleAbort(ctx, c)
	})

	// 注册文件上传路由
	h.POST("/sessions/:id/docs", func(_ context.Context, c *app.RequestContext) {
		// 调用上传处理函数
		srv.handleUpload(c)
	})

	// 记录服务启动信息
	log.Printf("starting server on http://localhost:%s", port)
	// 启动 HTTP 服务器（阻塞）
	h.Spin()

}

// chatRequest 聊天请求体结构
type chatRequest struct {
	// 用户发送的消息内容
	Message string `json:"message"`
}

// approveRequest 审批请求体结构
type approveRequest struct {
	// 是否批准
	Approved bool `json:"approved"`
	// 拒绝原因（可选）
	Reason string `json:"reason,omitempty"`
	// 是否始终允许该工具
	AlwaysAllow bool `json:"always_allow"`
	// 工具名称
	ToolName string `json:"tool_name"`
}

// handleChat 处理聊天消息，创建或复用 TurnLoop，支持抢占正在进行的轮次
func (s *server) handleChat(ctx context.Context, c *app.RequestContext) {
	// 获取路径参数中的会话 ID
	id := c.Param("id")

	// 读取请求体
	body, _ := c.Body()
	// 声明请求结构体
	var req chatRequest
	// 解析 JSON 请求体
	if err := json.Unmarshal(body, &req); err != nil || req.Message == "" {
		// 解析失败或消息为空，返回 400 错误
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "message is required"})
		return
	}

	// 记录聊天请求日志
	log.Printf("[chat] session=%s msg=%q", id, req.Message)

	// 获取或创建会话
	sess, err := s.store.GetOrCreate(id)
	// 如果获取失败，返回 500 错误
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// 创建 ChatItem（用户消息载体）
	item := &ChatItem{Query: req.Message}
	// 获取该会话的 TurnLoop 状态
	ts := s.getTurnState(id)

	// 声明本地 iterReady channel 引用
	var localIterReady chan iterEnvelope
	// 声明本地 handlerDone channel 引用
	var localHandlerDone chan struct{}

	// 加锁保护会话状态
	ts.mu.Lock()
	// 如果该会话已有 TurnLoop 在运行
	if ts.loop != nil {
		// 获取当前 loop 引用
		loop := ts.loop
		// 记录抢占日志
		log.Printf("[chat] session=%s preempting current turn", id)

		// 通知旧的 handler 退出
		if ts.handlerDone != nil {
			// 关闭 handlerDone channel，旧 handler 收到信号后退出
			close(ts.handlerDone)
		}
		// 创建新的事件桥接 channel
		ts.iterReady = make(chan iterEnvelope, 1)
		// 创建新的结果回传 channel
		ts.iterDone = make(chan iterResult, 1)
		// 创建新的 handler 完成信号 channel
		ts.handlerDone = make(chan struct{})
		// 保存本地引用
		localIterReady = ts.iterReady
		// 保存本地引用
		localHandlerDone = ts.handlerDone
		// 解锁
		ts.mu.Unlock()

		// 向 TurnLoop 推入新项并触发预占
		// 优先等待当前工具调用完成（优雅取消），如果 5 秒内未完成则升级为立即取消
		ok, _ := loop.Push(item, adk.WithPreemptTimeout[*ChatItem, *schema.Message](adk.AfterToolCalls, 5*time.Second))
		// 如果 Push 失败（loop 已停止）
		if !ok {
			// 记录日志
			log.Printf("[chat] session=%s loop was dead, creating new loop", id)
			// 加锁
			ts.mu.Lock()
			// 清除可能残留的 checkpoint（旧 loop 可能在 shutdown 时重新保存了 checkpoint）
			// 确保新 loop 以全新状态启动，不误入 resume 模式
			if deleter, ok := s.checkpointStore.(adk.CheckPointDeleter); ok {
				deleter.Delete(context.Background(), id)
			}
			// 创建新的 TurnLoop
			loop = s.newLoop(sess, id, false)
			// 更新 loop 引用
			ts.loop = loop
			// 创建新的事件桥接 channel
			ts.iterReady = make(chan iterEnvelope, 1)
			// 创建新的结果回传 channel
			ts.iterDone = make(chan iterResult, 1)
			// 创建新的 handler 完成信号 channel
			ts.handlerDone = make(chan struct{})
			// 保存本地引用
			localIterReady = ts.iterReady
			// 保存本地引用
			localHandlerDone = ts.handlerDone
			// 解锁
			ts.mu.Unlock()
			// 推入用户消息
			loop.Push(item)
			// 启动 TurnLoop（在后台 goroutine 中运行）
			loop.Run(context.Background())
			// 启动 loop 清理协程
			s.startLoopCleanup(ts, loop, id)
		}
	} else {
		// 没有 TurnLoop — 创建一个新的
		// 清除可能残留的 checkpoint（旧 loop 可能在 shutdown 时重新保存了 checkpoint）
		// 确保新 loop 以全新状态启动，不误入 resume 模式
		if deleter, ok := s.checkpointStore.(adk.CheckPointDeleter); ok {
			deleter.Delete(context.Background(), id)
		}
		// 创建新的 TurnLoop
		loop := s.newLoop(sess, id, false)
		// 设置 loop 引用
		ts.loop = loop
		// 创建事件桥接 channel
		ts.iterReady = make(chan iterEnvelope, 1)
		// 创建结果回传 channel
		ts.iterDone = make(chan iterResult, 1)
		// 创建 handler 完成信号 channel
		ts.handlerDone = make(chan struct{})
		// 保存本地引用
		localIterReady = ts.iterReady
		// 保存本地引用
		localHandlerDone = ts.handlerDone
		// 解锁
		ts.mu.Unlock()
		// 推入用户消息
		loop.Push(item)
		// 启动 TurnLoop
		loop.Run(context.Background())
		// 启动 loop 清理协程
		s.startLoopCleanup(ts, loop, id)
	}

	// 创建 SSE 流
	stream := sse.NewStream(c)
	// 确保请求结束时刷新缓冲区
	defer func() { _ = c.Flush() }()

	// 创建心跳停止信号 channel
	kaStop := make(chan struct{})
	// 启动心跳 goroutine，防止长时间无数据导致连接超时
	go func() {
		// 创建 5 秒间隔的定时器
		ticker := time.NewTicker(5 * time.Second)
		// 确保 goroutine 退出时停止定时器
		defer ticker.Stop()
		// 循环发送心跳
		for {
			select {
			// 收到停止信号，退出
			case <-kaStop:
				return
			// 定时器触发，发送空心跳
			case <-ticker.C:
				// 发送空的 SSE 事件作为心跳
				_ = stream.Publish(&sse.Event{Data: []byte{}})
			}
		}
	}()

	// 等待 OnAgentEvents 发送事件迭代器
	var envelope iterEnvelope
	select {
	// 成功收到事件迭代器
	case envelope = <-localIterReady:
	// 被更新的抢占取代，当前 handler 退出
	case <-localHandlerDone:
		// 停止心跳
		close(kaStop)
		// 记录日志
		log.Printf("[chat] session=%s handler superseded by newer preempt", id)
		// 通知前端当前轮次已被抢占
		_ = stream.Publish(&sse.Event{Data: []byte(`{"event":"preempted"}`)})
		return
	// 超时（60 秒内 Agent 未启动）
	case <-time.After(60 * time.Second):
		// 停止心跳
		close(kaStop)
		// 发送超时错误
		_ = stream.Publish(&sse.Event{Data: []byte(`{"error":"agent did not start in time"}`)})
		return
	}

	// 将 Agent 事件流转换为 A2UI 格式写入 SSE
	lastContent, intermediates, interruptID, finalMsgIdx, streamErr := a2ui.StreamToWriter(
		// 使用 sseLineWriter 包装 SSE 流
		&sseLineWriter{stream: stream}, id, envelope.history, envelope.events,
	)
	// 停止心跳
	close(kaStop)

	// 将结果回传给发送此 envelope 的 OnAgentEvents
	envelope.done <- iterResult{
		// 最后一条助手回复内容
		lastContent: lastContent,
		// 中间消息列表
		intermediates: intermediates,
		// 中断 ID
		interruptID: interruptID,
		// 最终消息索引
		msgIdx: finalMsgIdx,
		// 流式错误
		err: streamErr,
	}

	// 根据结果记录日志
	if streamErr != nil {
		// 记录流式错误
		log.Printf("[chat] session=%s stream error: %v", id, streamErr)
	} else if interruptID != "" {
		// 记录中断信息
		log.Printf("[chat] session=%s interrupted: id=%s", id, interruptID)
	} else {
		// 记录完成信息
		log.Printf("[chat] session=%s done, response=%d chars", id, len(lastContent))
	}
}

// handleRender 渲染会话历史，用于切换会话时加载已有消息
func (s *server) handleRender(c *app.RequestContext) {
	// 获取路径参数中的会话 ID
	id := c.Param("id")
	// 获取或创建会话
	sess, err := s.store.GetOrCreate(id)
	// 如果获取失败，返回 500 错误
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// 创建字节缓冲区
	var buf bytes.Buffer
	// 将历史消息渲染为 A2UI 格式
	if err := a2ui.RenderHistory(&buf, id, sess.GetMessages()); err != nil {
		// 渲染失败，返回 500 错误
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// 返回 NDJSON 格式的渲染数据
	c.Data(consts.StatusOK, "application/x-ndjson", buf.Bytes())
}

// handleApprove 处理审批请求，创建新的 TurnLoop 从 checkpoint 恢复执行
func (s *server) handleApprove(ctx context.Context, c *app.RequestContext) {
	// 获取路径参数中的会话 ID
	id := c.Param("id")

	// 获取或创建会话
	sess, err := s.store.GetOrCreate(id)
	// 如果获取失败，返回 500 错误
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// 获取待处理的中断 ID
	interruptID := sess.GetPendingInterruptID()
	// 如果没有待处理的中断，返回 400 错误
	if interruptID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "no pending interrupt for this session"})
		return
	}

	// 读取请求体
	body, _ := c.Body()
	// 声明审批请求结构体
	var req approveRequest
	// 解析 JSON 请求体
	if err := json.Unmarshal(body, &req); err != nil {
		// 解析失败，返回 400 错误
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	// 声明拒绝原因指针
	var reason *string
	// 如果提供了拒绝原因
	if req.Reason != "" {
		reason = &req.Reason
	}
	// 构建审批结果
	result := &commontool.ApprovalResult{Approved: req.Approved, DisapproveReason: reason}

	// 清空待处理的中断 ID（防止重复审批）
	sess.SetPendingInterruptID("")

	// 记录审批日志
	log.Printf("[approve] session=%s interruptID=%s approved=%v", id, interruptID, req.Approved)

	// 创建新的 TurnLoop 用于从 checkpoint 恢复
	ts := s.getTurnState(id)
	// 加锁保护
	ts.mu.Lock()
	// 停止旧的 TurnLoop
	if ts.loop != nil {
		// 立即停止
		ts.loop.Stop(adk.WithImmediate())
	}
	// 通知旧的 handler 退出
	if ts.handlerDone != nil {
		close(ts.handlerDone)
	}
	// 创建新的 TurnLoop（启用 checkpoint 恢复）
	loop := s.newLoop(sess, id, true)
	// 更新 loop 引用
	ts.loop = loop
	// 创建事件桥接 channel
	ts.iterReady = make(chan iterEnvelope, 1)
	// 创建结果回传 channel
	ts.iterDone = make(chan iterResult, 1)
	// 创建 handler 完成信号 channel
	ts.handlerDone = make(chan struct{})
	// 保存本地引用
	localIterReady := ts.iterReady
	// 保存本地引用
	localHandlerDone := ts.handlerDone
	// 解锁
	ts.mu.Unlock()

	// 推入审批项到 TurnLoop
	loop.Push(&ChatItem{
		// 审批结果
		ApprovalResult: result,
		// 要解决的中断 ID
		InterruptID: interruptID,
	})
	// 启动 TurnLoop
	loop.Run(context.Background())
	// 启动 loop 清理协程
	s.startLoopCleanup(ts, loop, id)

	// 创建 SSE 流
	stream := sse.NewStream(c)
	// 确保请求结束时刷新缓冲区
	defer func() { _ = c.Flush() }()

	// 创建心跳停止信号 channel
	kaStop := make(chan struct{})
	// 启动心跳 goroutine
	go func() {
		// 创建 5 秒间隔的定时器
		ticker := time.NewTicker(5 * time.Second)
		// 确保 goroutine 退出时停止定时器
		defer ticker.Stop()
		// 循环发送心跳
		for {
			select {
			// 收到停止信号，退出
			case <-kaStop:
				return
			// 定时器触发，发送空心跳
			case <-ticker.C:
				_ = stream.Publish(&sse.Event{Data: []byte{}})
			}
		}
	}()

	// 等待 OnAgentEvents 发送事件迭代器
	var envelope iterEnvelope
	select {
	// 成功收到事件迭代器
	case envelope = <-localIterReady:
	// 被更新的请求取代
	case <-localHandlerDone:
		// 停止心跳
		close(kaStop)
		// 记录日志
		log.Printf("[approve] session=%s handler superseded by newer request", id)
		// 通知前端已被取代
		_ = stream.Publish(&sse.Event{Data: []byte(`{"event":"preempted"}`)})
		return
	// 超时
	case <-time.After(60 * time.Second):
		// 停止心跳
		close(kaStop)
		// 发送超时错误
		_ = stream.Publish(&sse.Event{Data: []byte(`{"error":"agent did not start in time"}`)})
		return
	}

	// 将 Agent 事件流转换为 A2UI 格式写入 SSE
	lastContent, intermediates, newInterruptID, finalMsgIdx, streamErr := a2ui.StreamToWriter(
		// 使用 sseLineWriter 包装 SSE 流
		&sseLineWriter{stream: stream}, id, envelope.history, envelope.events,
	)
	// 停止心跳
	close(kaStop)

	// 将结果回传给 OnAgentEvents
	envelope.done <- iterResult{
		// 最后一条助手回复内容
		lastContent: lastContent,
		// 中间消息列表
		intermediates: intermediates,
		// 新的中断 ID（如果再次被中断）
		interruptID: newInterruptID,
		// 最终消息索引
		msgIdx: finalMsgIdx,
		// 流式错误
		err: streamErr,
	}

	// 根据结果记录日志
	if newInterruptID != "" {
		// 记录再次中断信息
		log.Printf("[approve] session=%s re-interrupted: id=%s", id, newInterruptID)
	} else if streamErr != nil {
		// 记录流式错误
		log.Printf("[approve] session=%s stream error: %v", id, streamErr)
	} else {
		// 记录完成信息
		log.Printf("[approve] session=%s done, response=%d chars", id, len(lastContent))
	}
}

// handleAbort 处理中止请求，立即停止会话的 TurnLoop 并清除 checkpoint
func (s *server) handleAbort(_ context.Context, c *app.RequestContext) {
	// 获取路径参数中的会话 ID
	id := c.Param("id")
	// 获取该会话的 TurnLoop 状态
	ts := s.getTurnState(id)
	// 加锁保护
	ts.mu.Lock()
	// 如果存在 TurnLoop
	if ts.loop != nil {
		// 立即停止 TurnLoop（取消当前轮次的 context）
		ts.loop.Stop(adk.WithImmediate())
		// 清空 loop 引用
		ts.loop = nil
	}
	// 解锁
	ts.mu.Unlock()

	// 清除 checkpoint，防止新 loop 启动时误触 resume 模式
	// 如果不清除，新 loop 发现旧 checkpoint 会进入 genResume 路径，
	// 但推入的是普通聊天项（无审批结果），导致 "no items for resume" 错误，
	// OnAgentEvents 永远不会被调用，handleChat 永远挂起
	if deleter, ok := s.checkpointStore.(adk.CheckPointDeleter); ok {
		deleter.Delete(context.Background(), id)
	}

	// 返回中止成功状态
	c.JSON(consts.StatusOK, map[string]string{"status": "aborted"})
}

// handleUpload 处理文件上传请求
func (s *server) handleUpload(c *app.RequestContext) {
	// 获取路径参数中的会话 ID
	id := c.Param("id")

	// 构建该会话的工作目录绝对路径
	absWorkDir, err := filepath.Abs(filepath.Join(s.workspaceDir, id))
	// 如果路径构建失败，返回 500 错误
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	// 创建工作目录（如果不存在）
	if err := os.MkdirAll(absWorkDir, 0o755); err != nil {
		// 创建失败，返回 500 错误
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// 从表单中获取上传的文件
	fileHeader, err := c.FormFile("file")
	// 如果没有文件字段，返回 400 错误
	if err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "file field is required"})
		return
	}

	// 构建保存路径
	dst := filepath.Join(absWorkDir, filepath.Base(fileHeader.Filename))
	// 保存上传的文件
	if err := c.SaveUploadedFile(fileHeader, dst); err != nil {
		// 保存失败，返回 500 错误
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// 返回上传成功信息
	c.JSON(consts.StatusOK, map[string]string{
		// 文件名
		"name": fileHeader.Filename,
		// 保存路径
		"path": dst,
	})
}

// sseLineWriter 将写入数据按行分割并通过 SSE 发送
type sseLineWriter struct {
	// SSE 流实例
	stream *sse.Stream
	// 缓冲区，用于累积未完整行的数据
	buf []byte
}

// Write 实现 io.Writer 接口，按行分割数据并通过 SSE 发送
func (w *sseLineWriter) Write(p []byte) (int, error) {
	// 将新数据追加到缓冲区
	w.buf = append(w.buf, p...)
	// 循环处理缓冲区中的完整行
	for {
		// 初始化换行符位置
		idx := -1
		// 查找第一个换行符
		for i, b := range w.buf {
			// 找到换行符
			if b == '\n' {
				idx = i
				break
			}
		}
		// 如果没有完整行，退出循环
		if idx < 0 {
			break
		}
		// 提取完整行
		line := w.buf[:idx]
		// 从缓冲区中移除已处理的行
		w.buf = w.buf[idx+1:]
		// 跳过空行
		if len(line) == 0 {
			continue
		}
		// 通过 SSE 发送该行数据
		if err := w.stream.Publish(&sse.Event{Data: line}); err != nil {
			return 0, err
		}
	}
	// 返回写入的字节数
	return len(p), nil
}

// getTurnState 获取或创建会话的 TurnLoop 状态
func (s *server) getTurnState(sessionID string) *sessionTurnState {
	// 使用 sync.Map 的 LoadOrStore 原子操作获取或创建状态
	val, _ := s.turnStates.LoadOrStore(sessionID, &sessionTurnState{})
	// 类型断言返回
	return val.(*sessionTurnState)
}

// startLoopCleanup 启动一个 goroutine 等待 TurnLoop 退出并清理状态
func (s *server) startLoopCleanup(ts *sessionTurnState, loop *adk.TurnLoop[*ChatItem, *schema.Message], sessionID string) {
	// 启动清理协程
	go func() {
		// 等待 TurnLoop 完全退出
		result := loop.Wait()
		// 加锁保护
		ts.mu.Lock()
		// 如果当前 loop 仍然是这个（没有被替换）
		if ts.loop == loop {
			// 清空 loop 引用
			ts.loop = nil
		}
		// 解锁
		ts.mu.Unlock()

		// 检查退出原因
		if result.ExitReason != nil {
			// 记录错误退出日志
			log.Printf("[loop] session=%s exited with error: %v", sessionID, result.ExitReason)
		} else {
			// 记录正常退出日志
			log.Printf("[loop] session=%s exited cleanly", sessionID)
		}

		// 如果 loop 不是因 errInterrupted 退出，则清除 checkpoint
		// errInterrupted 意味着 Agent 被暂停等待审批，checkpoint 需要保留供 resume 使用
		// 其他退出原因（agent canceled、工具错误等）意味着 checkpoint 已过期，
		// 不清除会导致下次新建 loop 时误入 resume 模式，GenResume 收到普通聊天项而报错
		if result.ExitReason != nil && !errors.Is(result.ExitReason, errInterrupted) {
			if deleter, ok := s.checkpointStore.(adk.CheckPointDeleter); ok {
				deleter.Delete(context.Background(), sessionID)
				log.Printf("[loop] session=%s checkpoint cleared (exit not from interrupt)", sessionID)
			}
		}
	}()
}

// newLoop 创建一个新的 TurnLoop 实例
func (s *server) newLoop(sess *mem.Session, sessionID string, isResume bool) *adk.TurnLoop[*ChatItem, *schema.Message] {
	// 配置 TurnLoop
	cfg := adk.TurnLoopConfig[*ChatItem, *schema.Message]{
		// GenInput：每轮开始时调用，从队列中的 items 构建 Agent 输入
		GenInput: func(ctx context.Context, loop *adk.TurnLoop[*ChatItem, *schema.Message], items []*ChatItem) (*adk.GenInputResult[*ChatItem, *schema.Message], error) {
			// 遍历队列中的 items，持久化用户消息
			for _, item := range items {
				// 如果是用户消息（非审批项）
				if item.Query != "" {
					// 创建用户消息
					userMsg := schema.UserMessage(item.Query)
					// 持久化到会话存储
					if err := sess.Append(userMsg); err != nil {
						// 持久化失败，仅记录警告
						log.Printf("warn: failed to persist user message: %v", err)
					}
				}
			}
			// 获取完整的会话历史消息
			history := sess.GetMessages()
			// 构建 Agent 运行时的输入消息（包含系统提示 + 历史）
			runMessages := s.genRunMessages(sessionID, history)
			// 返回 GenInputResult
			return &adk.GenInputResult[*ChatItem, *schema.Message]{
				// 构建 Agent 输入
				Input: &adk.TypedAgentInput[*schema.Message]{
					// 消息列表
					Messages: runMessages,
					// 启用流式输出
					EnableStreaming: true,
				},
			}, nil
		},
		// PrepareAgent：每轮调用，返回本轮使用的 Agent
		PrepareAgent: func(ctx context.Context, loop *adk.TurnLoop[*ChatItem, *schema.Message], consumed []*ChatItem) (adk.TypedAgent[*schema.Message], error) {
			// 返回预创建的 Agent 实例
			return s.agent, nil
		},
		// OnAgentEvents：接收 Agent 的事件流，负责将事件传递给 HTTP handler
		OnAgentEvents: func(ctx context.Context, tc *adk.TurnContext[*ChatItem, *schema.Message], events *adk.AsyncIterator[*adk.TypedAgentEvent[*schema.Message]]) error {
			// 获取会话的 TurnLoop 状态
			ts := s.getTurnState(sessionID)
			// 获取当前会话历史
			history := sess.GetMessages()
			// 创建结果回传 channel
			done := make(chan iterResult, 1)
			// 将事件迭代器发送给 HTTP handler
			ts.iterReady <- iterEnvelope{
				// 事件迭代器
				events: events,
				// 历史消息
				history: history,
				// 结果回传 channel
				done: done,
			}
			// 等待 HTTP handler 消费完事件并返回结果
			result := <-done
			// 持久化中间消息（工具调用 + 工具结果）
			for _, msg := range result.intermediates {
				// 逐条追加到会话存储
				if appendErr := sess.Append(msg); appendErr != nil {
					// 持久化失败，仅记录警告
					log.Printf("warn: failed to persist intermediate: %v", appendErr)
				}
			}
			// 检查是否有中断（审批请求）
			if result.interruptID != "" {
				// 保存中断 ID 到会话
				sess.SetPendingInterruptID(result.interruptID)
				// 返回中断错误，TurnLoop 将以此作为退出原因
				return errInterrupted
			}
			// 正常完成，返回 nil
			return nil
		},
		// GenResume：从 checkpoint 恢复时调用，从新 Push 的 items 中提取审批结果
		GenResume: func(ctx context.Context, loop *adk.TurnLoop[*ChatItem, *schema.Message], canceledItems, unhandledItems, newItems []*ChatItem) (*adk.GenResumeResult[*ChatItem, *schema.Message], error) {
			// newItems 包含审批恢复时 Push 的 item
			// 如果没有新项，返回错误
			if len(newItems) == 0 {
				return nil, fmt.Errorf("no items for resume")
			}
			// 获取第一个新项（审批项）
			item := newItems[0]
			// 如果该项没有审批结果，返回错误
			if item.ApprovalResult == nil {
				return nil, fmt.Errorf("item has no approval result")
			}
			// 返回恢复结果
			return &adk.GenResumeResult[*ChatItem, *schema.Message]{
				// 构建恢复参数
				ResumeParams: &adk.ResumeParams{
					// 以 interruptID 为 key，审批结果为 value
					Targets: map[string]any{
						item.InterruptID: item.ApprovalResult,
					},
				},
			}, nil
		},
		// checkpoint 存储
		Store: s.checkpointStore,
		// checkpoint ID（使用会话 ID）
		CheckpointID: sessionID,
	}
	// 创建并返回 TurnLoop 实例
	return adk.NewTurnLoop(cfg)
}

// genRunMessages 构建 Agent 运行时的输入消息列表（系统提示 + 过滤后的历史消息）
func (s *server) genRunMessages(sessionID string, history []*schema.Message) []*schema.Message {
	// 初始化提示行列表
	var lines []string
	// 添加上下文标记
	lines = append(lines, "[Context]")
	// 添加重要规则提示
	lines = append(lines,
		// 规则说明
		"IMPORTANT RULES:",
		// 规则 1：必须使用工具查找真实代码
		"  1. Always use filesystem tools to look up real code before answering. Do not guess or make up information.",
		// 规则 2：使用工具后必须写文本回复
		"  2. After using tools (even if they return no results), you MUST write a text response to the user summarizing what you found.",
		// 规则 3：不能仅以工具调用结束
		"  3. Never end your turn without a text response — tool calls alone are not sufficient.",
		// 规则 4：构建/测试代码时使用执行工具
		"  4. When asked to build or test code, use the execute tool to run the command.",
		// 构建说明
		"     Each Go example has its own go.mod. To build an example, run:",
		// 构建命令示例
		"       cd <example-dir> && go build ./...",
		// 不要假设构建成功
		"     NEVER assume a build succeeded without actually running it.",
		// 规则 5：声称编译通过前必须运行构建工具
		"  5. When writing or editing a file and then claiming it compiles, you MUST run the build tool to verify.",
	)

	// 如果配置了项目根目录
	if s.projectRoot != "" {
		// 添加项目根目录信息
		lines = append(lines,
			// 项目根目录路径
			fmt.Sprintf("Project root: %s", s.projectRoot),
			// 重要提示：使用文件系统工具时传入项目根目录
			"  IMPORTANT: Always pass the project root as the path argument when using filesystem tools.",
			// grep 使用示例
			fmt.Sprintf("  - grep(pattern=\"...\", path=\"%s\")", s.projectRoot),
			// glob 使用示例
			fmt.Sprintf("  - glob(pattern=\"%s/**/*.go\")", s.projectRoot),
			// read_file 使用示例
			fmt.Sprintf("  - read_file(file_path=\"%s/some/file.go\")", s.projectRoot),
			// 说明 grep 和 glob 会递归搜索
			"  grep and glob recurse into ALL subdirectories under the given path.",
			// 列出项目根目录的顶级子目录
			"  Top-level subdirectories of the project root:",
		)
		// 读取项目根目录内容
		if entries, err := os.ReadDir(s.projectRoot); err == nil {
			// 遍历目录条目
			for _, e := range entries {
				// 只列出目录
				if e.IsDir() {
					// 添加子目录路径
					lines = append(lines, "    - "+filepath.Join(s.projectRoot, e.Name())+"/")
				}
			}
		}
		// 提示使用工具读取真实源码
		lines = append(lines, "  Use these tools to read actual source code before answering questions about the codebase.")
	}

	// 如果配置了示例目录且与项目根目录不同
	if s.examplesDir != "" && s.examplesDir != s.projectRoot {
		// 添加示例目录信息
		lines = append(lines,
			// 示例目录路径
			fmt.Sprintf("eino-examples directory: %s", s.examplesDir),
			// 提示在示例目录中搜索
			"  When the user asks about examples or sample code, search here specifically:",
			// grep 使用示例
			fmt.Sprintf("  - grep(pattern=\"...\", path=\"%s\")", s.examplesDir),
			// glob 使用示例
			fmt.Sprintf("  - glob(pattern=\"%s/**/*.go\")", s.examplesDir),
		)
	}

	// 获取该会话的工作目录绝对路径
	absWorkDir, err := filepath.Abs(filepath.Join(s.workspaceDir, sessionID))
	// 如果路径构建成功
	if err == nil {
		// 读取工作目录内容
		entries, _ := os.ReadDir(absWorkDir)
		// 初始化上传文件列表
		var uploadedFiles []string
		// 遍历目录条目
		for _, e := range entries {
			// 只收集文件（非目录）
			if !e.IsDir() {
				// 添加文件路径
				uploadedFiles = append(uploadedFiles, filepath.Join(absWorkDir, e.Name()))
			}
		}
		// 如果有上传的文件
		if len(uploadedFiles) > 0 {
			// 添加会话工作目录信息
			lines = append(lines,
				// 工作目录路径
				fmt.Sprintf("Session workspace: %s", absWorkDir),
				// 上传文件列表标题
				"  Uploaded files:",
			)
			// 遍历上传文件
			for _, f := range uploadedFiles {
				// 添加文件路径
				lines = append(lines, "    - "+f)
			}
		}
	}

	// 将所有提示行拼接为一个字符串
	ctx := strings.Join(lines, "\n")
	// 创建运行消息列表（容量为历史消息数 + 1）
	runMessages := make([]*schema.Message, 0, len(history)+1)

	// 添加系统提示作为第一条用户消息
	runMessages = append(runMessages, schema.UserMessage(ctx))
	// 过滤掉孤儿 tool_call 消息（有 tool_calls 但没有匹配的 Tool 结果），
	// 防止发送给模型 API 时出现 invalid_request_error。
	runMessages = append(runMessages, filterOrphanedToolCalls(history)...)
	// 返回构建好的消息列表
	return runMessages
}

// filterOrphanedToolCalls 过滤掉有 tool_calls 但没有匹配 tool 结果的助手消息
// 防止发送给模型 API 时出现无效消息序列导致请求被拒绝
// 孤儿 tool_call 消息发生在 Agent 被中断（如等待审批）时，
// 中断的中间消息已持久化到会话，但后续的恢复输出（工具结果 + 最终回复）尚未持久化
func filterOrphanedToolCalls(history []*schema.Message) []*schema.Message {
	// 初始化结果列表
	var result []*schema.Message
	// 初始化遍历索引
	i := 0
	// 遍历历史消息
	for i < len(history) {
		// 获取当前消息
		msg := history[i]
		// 如果是助手消息且包含 tool_calls
		if msg.Role == schema.Assistant && len(msg.ToolCalls) > 0 {
			// 统计紧随其后的连续 tool 结果消息数量
			toolResultCount := 0
			// 从下一条消息开始检查
			j := i + 1
			// 连续统计 tool 角色消息
			for j < len(history) && history[j].Role == schema.Tool {
				// 计数器加一
				toolResultCount++
				// 移动到下一条
				j++
			}
			// 如果 tool 结果数量少于 tool_calls 数量（孤儿消息）
			if toolResultCount < len(msg.ToolCalls) {
				// 跳过此助手消息
				i++
				continue
			}
		}
		// 将有效消息加入结果
		result = append(result, msg)
		// 移动到下一条
		i++
	}
	// 返回过滤后的消息列表
	return result
}

// server 服务器结构体，持有所有依赖
type server struct {
	// Agent 实例
	agent adk.TypedAgent[*schema.Message]
	// 会话存储
	store *mem.Store
	// checkpoint 存储（用于中断状态保存与恢复）
	checkpointStore adk.CheckPointStore
	// 工作目录（上传文件保存位置）
	workspaceDir string
	// 项目根目录（Agent 可探索的代码库根目录）
	projectRoot string
	// 示例目录（eino-examples 仓库根目录）
	examplesDir string
	// 会话 ID 到 TurnLoop 状态的映射（并发安全）
	turnStates sync.Map
}

// Input 定义工作流工具的输入参数
type Input struct {
	// 上传文档文件的绝对路径
	FilePath string `json:"file_path" jsonschema:"description=Absolute path to the uploaded document file"`
	// 要从文档中回答的问题
	Question string `json:"question"  jsonschema:"description=The question to answer from the document"`
}

// Output 定义工作流工具的输出结果
type Output struct {
	// 综合回答
	Answer string `json:"answer"`
	// 用于生成回答的关键摘录
	Sources []string `json:"sources"`
}

// ScoreTask 是输入到内部 BatchNode 工作流的每个块的输入
type ScoreTask struct {
	// 块的文本内容
	Text string
	// 用户问题
	Question string
}

// ScoredChunk 是由内部 BatchNode 工作流产生的每个块的评分结果
type ScoredChunk struct {
	// 原始文本块
	Text string
	// 相关性评分（0-10）
	Score int
	// 最相关的句子或短语
	Excerpt string
}

// SynthIn 综合回答节点的输入
type SynthIn struct {
	// 评分最高的 TopK 块
	TopK []ScoredChunk
	// 用户问题
	Question string
}

// createWorkflowTool 构建文档问答工作流工具
func createWorkflowTool(_ context.Context, cm model.BaseModel[*schema.Message]) (tool.BaseTool, error) {
	// 创建主流程工作流
	var fullWorkflow = compose.NewWorkflow[Input, Output]()
	// 构建加载文件节点（读取上传的文档文件）
	fullWorkflow.AddLambdaNode("load", compose.InvokableLambda(
		// in 是 START 节点的输入
		func(ctx context.Context, in Input) ([]*schema.Document, error) {
			// 读取文件内容
			data, err := os.ReadFile(in.FilePath)
			// 如果读取失败，返回错误
			if err != nil {
				return nil, fmt.Errorf("read %q: %w", in.FilePath, err)
			}
			// 将文件内容封装为 Document
			return []*schema.Document{{Content: string(data)}}, nil
		},
		// 设置输入来源为 START 节点
	)).AddInput(compose.START)

	// 构建分块节点（将文档分成每个 800 字符的片段）
	fullWorkflow.AddLambdaNode("chunk", compose.InvokableLambda(
		// docs 是 load 节点的输出
		func(ctx context.Context, docs []*schema.Document) ([]*schema.Document, error) {
			// 初始化输出列表
			var out []*schema.Document
			// 遍历每个文档
			for _, d := range docs {
				// 将文档内容拆分为 800 字符的块
				out = append(out, splitIntoChunks(d.Content, 800)...)
			}
			// 返回所有块
			return out, nil
		},
		// 设置输入来源为 load 节点
	)).AddInput("load")

	// 构建评分工作流
	scoreWorkflow := newScoreWorkFlow(cm)
	// 创建批量处理节点
	scorer := batch.NewBatchNode(&batch.NodeConfig[ScoreTask, ScoredChunk]{
		// 节点名称
		Name: "ChunkScorer",
		// 传入评分工作流作为内部任务
		InnerTask: scoreWorkflow,
		// 最大并发数（5 表示最多 5 个块并行评分）
		MaxConcurrency: 5,
	})

	// 构建主流程批量处理节点（使用 scorer）
	fullWorkflow.AddLambdaNode("score", compose.InvokableLambda(
		// 批量评分处理函数
		func(ctx context.Context, in map[string]any) ([]ScoredChunk, error) {
			// 从输入中获取文档块列表
			chunks := in["Chunks"].([]*schema.Document)
			// 从输入中获取用户问题
			question := in["Question"].(string)
			// 将文档块转换为评分任务
			tasks := make([]ScoreTask, len(chunks))
			// 遍历每个块
			for i, c := range chunks {
				// 构建评分任务
				tasks[i] = ScoreTask{Text: c.Content, Question: question}
			}
			// 调用批量处理节点并行执行评分
			return scorer.Invoke(ctx, tasks)
		},
		// 从 chunk 节点获取 Chunks 字段
	)).
		AddInputWithOptions("chunk",
			// 映射 Chunks 字段
			[]*compose.FieldMapping{compose.ToField("Chunks")},
			// 不建立直接依赖（允许并行执行）
			compose.WithNoDirectDependency()).
		// 从 START 节点获取 Question 字段
		AddInputWithOptions(compose.START,
			// 映射 Question 字段
			[]*compose.FieldMapping{compose.MapFields("Question", "Question")},
			// 不建立直接依赖
			compose.WithNoDirectDependency())
	// 按分数降序排序，保留分数最高的前 3 个块
	fullWorkflow.AddLambdaNode("filter", compose.InvokableLambda(
		// scored 是 score 节点的输出
		func(ctx context.Context, scored []ScoredChunk) ([]ScoredChunk, error) {
			// 按分数降序排序
			sort.Slice(scored, func(i, j int) bool {
				return scored[i].Score > scored[j].Score
			})
			// 最多保留 3 个
			const maxK = 3
			// 初始化顶部列表
			var top []ScoredChunk
			// 遍历排序后的结果
			for _, c := range scored {
				// 如果分数低于 3，停止
				if c.Score < 3 {
					break
				}
				// 加入顶部列表
				top = append(top, c)
				// 达到上限则停止
				if len(top) == maxK {
					break
				}
			}
			// 返回过滤后的结果
			return top, nil
		},
		// 设置输入来源为 score 节点
	)).AddInput("score")

	// 配置响应回答节点
	fullWorkflow.AddLambdaNode("answer", compose.InvokableLambda(
		// 综合回答处理函数
		func(ctx context.Context, in map[string]any) (Output, error) {
			// 获取 TopK 块
			topK := in["TopK"].([]ScoredChunk)
			// 获取用户问题
			question := in["Question"].(string)
			// 如果没有相关内容
			if len(topK) == 0 {
				// 返回未找到相关内容的提示
				return Output{
					Answer: fmt.Sprintf("No relevant content found in the document for: %q", question),
				}, nil
			}
			// 调用综合回答函数
			return synthesize(ctx, cm, SynthIn{TopK: topK, Question: question})
		},
		// 从 filter 节点获取 TopK 字段
	)).
		AddInputWithOptions("filter",
			// 映射 TopK 字段
			[]*compose.FieldMapping{compose.ToField("TopK")}, compose.WithNoDirectDependency()).
		// 从 START 节点获取 Question 字段
		AddInputWithOptions(compose.START,
			// 映射 Question 字段
			[]*compose.FieldMapping{compose.MapFields("Question", "Question")}, compose.WithNoDirectDependency())

	// END 节点接收 answer 节点的输出
	fullWorkflow.End().
		// 设置输入来源
		AddInput("answer")

	// 将工作流封装为可调用的图工具
	return graphtool.NewInvokableGraphTool[Input, Output](
		// 工作流实例
		fullWorkflow,
		// 工具名称
		"answer_from_document",
		// 工具描述
		"在用户上传的大型文档中搜索与问题相关的内容，并从最相关的段落中综合出带引用的答案。如果用户上传过文档，一定要使用该工具处理，不要使用read_file。",
	)
}

// newScoreWorkFlow 创建评分子工作流
func newScoreWorkFlow(cm model.BaseModel[*schema.Message]) *compose.Workflow[ScoreTask, ScoredChunk] {
	// 创建评分工作流
	scoreWorkflow := compose.NewWorkflow[ScoreTask, ScoredChunk]()
	// 添加评分节点
	scoreWorkflow.AddLambdaNode("score_chunk", compose.InvokableLambda(
		// 评分处理函数
		func(ctx context.Context, t ScoreTask) (ScoredChunk, error) {
			// 构建评分提示词
			prompt := fmt.Sprintf(`Rate how relevant the following text chunk is to the question.
Question: %s
Chunk:
%s
Reply with JSON only — no explanation, no markdown fences:
{"score": <0-10>, "excerpt": "<most relevant sentence or phrase, empty string if score is 0>"}
Score guide: 0=completely irrelevant, 3=tangentially related, 7=clearly relevant, 10=directly answers the question.`,
				// 插入用户问题
				t.Question,
				// 插入文本块内容
				t.Text)
			// 创建用户消息
			userMessage := schema.UserMessage(prompt)
			// 构建消息列表
			messages := []*schema.Message{userMessage}

			// 调用模型生成评分
			resp, err := cm.Generate(ctx, messages)
			// 如果调用失败，返回 0 分
			if err != nil {
				return ScoredChunk{Text: t.Text, Score: 0}, nil
			}
			// 提取模型回复文本
			content := strings.TrimSpace(messageText(resp))
			// 去除可选的 markdown 代码块包裹
			content = strings.TrimPrefix(content, "```json")
			// 去除开头的 ```
			content = strings.TrimPrefix(content, "```")
			// 去除结尾的 ```
			content = strings.TrimSuffix(content, "```")
			// 去除首尾空白
			content = strings.TrimSpace(content)
			// 定义 JSON 解析结构体
			var sr struct {
				// 评分
				Score int `json:"score"`
				// 摘录
				Excerpt string `json:"excerpt"`
			}
			// 解析 JSON 响应
			if err := json.Unmarshal([]byte(content), &sr); err != nil {
				// 解析失败，返回 0 分
				return ScoredChunk{Text: t.Text, Score: 0}, nil
			}
			// 返回评分结果
			return ScoredChunk{Text: t.Text, Score: sr.Score, Excerpt: sr.Excerpt}, nil
		},
		// 设置输入来源为 START 节点
	)).AddInput(compose.START)
	// 设置 END 节点接收 score_chunk 的输出
	scoreWorkflow.End().AddInput("score_chunk")
	// 返回评分工作流
	return scoreWorkflow
}

// messageText 提取消息中的文本内容
func messageText(msg *schema.Message) string {
	// 如果消息为空，返回空字符串
	if msg == nil {
		return ""
	}
	// 如果有 Content 字段
	if msg.Content != "" {
		// 直接返回 Content
		return msg.Content
	}
	// 初始化文本片段列表
	var parts []string
	// 遍历用户输入多内容部分
	for _, part := range msg.UserInputMultiContent {
		// 如果是文本类型且不为空
		if part.Type == schema.ChatMessagePartTypeText && part.Text != "" {
			// 添加到片段列表
			parts = append(parts, part.Text)
		}
	}
	// 遍历助手生成多内容部分
	for _, part := range msg.AssistantGenMultiContent {
		// 如果是文本类型且不为空
		if part.Type == schema.ChatMessagePartTypeText && part.Text != "" {
			// 添加到片段列表
			parts = append(parts, part.Text)
		}
	}
	// 用换行符拼接所有片段
	return strings.Join(parts, "\n")
}

// splitIntoChunks 将文本拆分为指定大小的块
func splitIntoChunks(text string, chunkSize int) []*schema.Document {
	// 初始化块列表
	var chunks []*schema.Document
	// 创建字符串构建器
	var buf strings.Builder

	// flush 函数将缓冲区内容作为一个块输出
	flush := func() {
		// 去除首尾空白
		s := strings.TrimSpace(buf.String())
		// 如果不为空
		if s != "" {
			// 添加为新块
			chunks = append(chunks, &schema.Document{Content: s})
		}
		// 重置缓冲区
		buf.Reset()
	}

	// 按双换行符（段落）分割文本
	for _, para := range strings.Split(text, "\n\n") {
		// 去除段落首尾空白
		para = strings.TrimSpace(para)
		// 如果段落为空，跳过
		if para == "" {
			continue
		}
		// 如果加入段落后超过块大小且缓冲区不为空
		if buf.Len()+len(para)+2 > chunkSize && buf.Len() > 0 {
			// 先输出缓冲区内容
			flush()
		}
		// 如果段落本身超过块大小，按行拆分
		if len(para) > chunkSize {
			// 按换行符拆分为行
			for _, line := range strings.Split(para, "\n") {
				// 去除行首尾空白
				line = strings.TrimSpace(line)
				// 如果行为空，跳过
				if line == "" {
					continue
				}
				// 如果加入行后超过块大小且缓冲区不为空
				if buf.Len()+len(line)+1 > chunkSize && buf.Len() > 0 {
					// 先输出缓冲区内容
					flush()
				}
				// 如果缓冲区不为空，添加换行符
				if buf.Len() > 0 {
					buf.WriteByte('\n')
				}
				// 写入行内容
				buf.WriteString(line)
			}
		} else {
			// 如果缓冲区不为空，添加段落分隔符
			if buf.Len() > 0 {
				buf.WriteString("\n\n")
			}
			// 写入段落内容
			buf.WriteString(para)
		}
	}
	// 输出缓冲区剩余内容
	flush()
	// 返回所有块
	return chunks
}

// synthesize 使用模型根据最相关的文档片段综合回答
func synthesize(ctx context.Context, cm model.BaseModel[*schema.Message], in SynthIn) (Output, error) {
	// 创建字符串构建器
	var sb strings.Builder
	// 写入指令提示
	sb.WriteString("Answer the following question using only the provided document excerpts.\n\n")
	// 写入问题标记
	sb.WriteString("Question: ")
	// 写入用户问题
	sb.WriteString(in.Question)
	// 写入文档片段标记
	sb.WriteString("\n\nDocument excerpts:\n")

	// 初始化来源列表
	sources := make([]string, len(in.TopK))
	// 遍历 TopK 块
	for i, c := range in.TopK {
		// 获取摘录
		excerpt := c.Excerpt
		// 如果摘录为空，使用原始文本
		if excerpt == "" {
			excerpt = c.Text
		}
		// 保存来源
		sources[i] = excerpt
		// 写入带编号的摘录
		fmt.Fprintf(&sb, "[%d] %s\n\n", i+1, excerpt)
	}
	// 写入回答要求
	sb.WriteString("Provide a clear, concise answer. Cite excerpt numbers like [1] when referencing sources.")

	// 构建消息列表
	messages := []*schema.Message{
		// 创建用户消息（包含提示词）
		schema.UserMessage(sb.String()),
	}
	// 调用模型生成回答
	resp, err := cm.Generate(ctx, messages)
	// 如果生成失败，返回错误
	if err != nil {
		return Output{}, fmt.Errorf("synthesize: %w", err)
	}
	// 返回回答结果和来源
	return Output{Answer: messageText(resp), Sources: sources}, nil
}

// newSafeToolMiddleware 创建安全工具中间件（捕获工具错误，防止管道崩溃）
func newSafeToolMiddleware[M adk.MessageType]() adk.TypedChatModelAgentMiddleware[M] {
	// 返回安全工具中间件实例
	return &safeToolMiddleware[M]{
		// 嵌入基础中间件
		TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[M]{},
	}
}

// safeToolMiddleware 安全工具中间件结构体
type safeToolMiddleware[M adk.MessageType] struct {
	// 嵌入类型化的基础聊天模型 Agent 中间件
	*adk.TypedBaseChatModelAgentMiddleware[M]
}

// WrapInvokableToolCall 包装可调用工具调用，捕获错误
func (m *safeToolMiddleware[M]) WrapInvokableToolCall(
	_ context.Context,
	// 原始工具调用端点
	endpoint adk.InvokableToolCallEndpoint,
	_ *adk.ToolContext,
) (adk.InvokableToolCallEndpoint, error) {
	// 返回包装后的工具调用函数
	return func(ctx context.Context, args string, opts ...tool.Option) (string, error) {
		// 调用原始工具
		result, err := endpoint(ctx, args, opts...)
		// 如果有错误
		if err != nil {
			// 如果是中断重运行错误，直接传播
			if _, ok := compose.IsInterruptRerunError(err); ok {
				return "", err
			}
			// 将错误格式化为文本返回（不传播错误，防止管道崩溃）
			return fmt.Sprintf("[tool error] %v", err), nil
		}
		// 返回正常结果
		return result, nil
	}, nil
}

// WrapStreamableToolCall 包装可流式工具调用，捕获错误
func (m *safeToolMiddleware[M]) WrapStreamableToolCall(
	_ context.Context,
	// 原始可流式工具调用端点
	endpoint adk.StreamableToolCallEndpoint,
	_ *adk.ToolContext,
) (adk.StreamableToolCallEndpoint, error) {
	// 返回包装后的可流式工具调用函数
	return func(ctx context.Context, args string, opts ...tool.Option) (*schema.StreamReader[string], error) {
		// 调用原始工具
		sr, err := endpoint(ctx, args, opts...)
		// 如果有错误
		if err != nil {
			// 如果是中断重运行错误，直接传播
			if _, ok := compose.IsInterruptRerunError(err); ok {
				return nil, err
			}
			// 将错误作为单块流返回
			return SingleChunkReader(fmt.Sprintf("[tool error] %v", err)), nil
		}
		// 用安全包装器包装流读取器
		return safeWrapReader(sr), nil
	}, nil
}

// SingleChunkReader 创建一个只发送一个字符串然后 EOF 的流读取器
func SingleChunkReader(msg string) *schema.StreamReader[string] {
	// 创建管道（缓冲区大小为 1）
	r, w := schema.Pipe[string](1)
	// 发送消息
	_ = w.Send(msg, nil)
	// 关闭写入端
	w.Close()
	// 返回读取端
	return r
}

// safeWrapReader 代理流读取器，在流错误时将错误作为最终块发送
// 而不是传播错误，使模型看到完整的（带错误标注的）工具结果
func safeWrapReader(sr *schema.StreamReader[string]) *schema.StreamReader[string] {
	// 创建管道（缓冲区大小为 64）
	r, w := schema.Pipe[string](64)
	// 启动 goroutine 代理数据
	go func() {
		// 确保 goroutine 退出时关闭写入端
		defer w.Close()
		// 循环读取和转发
		for {
			// 从源流读取块
			chunk, err := sr.Recv()
			// 如果是 EOF，退出
			if errors.Is(err, io.EOF) {
				return
			}
			// 如果有错误
			if err != nil {
				// 将错误作为最终块发送
				_ = w.Send(fmt.Sprintf("\n[tool error] %v", err), nil)
				return
			}
			// 转发正常块
			_ = w.Send(chunk, nil)
		}
	}()
	// 返回读取端
	return r
}

// approvalMiddleware 审批中间件结构体
type approvalMiddleware[M adk.MessageType] struct {
	// 嵌入类型化的基础聊天模型 Agent 中间件
	*adk.TypedBaseChatModelAgentMiddleware[M]
}

// newApprovalMiddleware 创建审批中间件
func newApprovalMiddleware[M adk.MessageType]() adk.TypedChatModelAgentMiddleware[M] {
	// 返回审批中间件实例
	return &approvalMiddleware[M]{
		// 嵌入基础中间件
		TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[M]{},
	}
}

// WrapInvokableToolCall 包装可调用工具调用，添加审批流程
func (m *approvalMiddleware[M]) WrapInvokableToolCall(
	_ context.Context,
	// 原始工具调用端点
	endpoint adk.InvokableToolCallEndpoint,
	// 工具上下文
	tCtx *adk.ToolContext,
) (adk.InvokableToolCallEndpoint, error) {
	// 只对 answer_from_document 工具启用审批
	if tCtx.Name != "answer_from_document" {
		// 其他工具直接返回原始端点
		return endpoint, nil
	}
	// 返回包装后的工具调用函数
	return func(ctx context.Context, args string, opts ...tool.Option) (string, error) {
		// 检查是否处于中断恢复状态
		wasInterrupted, _, storedArgs := tool.GetInterruptState[string](ctx)
		// 如果未被中断（首次调用）
		if !wasInterrupted {
			// 触发有状态中断，暂停执行等待人工审批
			return "", tool.StatefulInterrupt(ctx, &commontool.ApprovalInfo{
				// 工具名称
				ToolName: tCtx.Name,
				// 调用参数（JSON 格式）
				ArgumentsInJSON: args,
			}, args)
		}

		// 检查是否有审批结果
		isTarget, hasData, data := tool.GetResumeContext[*commontool.ApprovalResult](ctx)
		// 如果是目标中断且有审批数据
		if isTarget && hasData {
			// 如果批准
			if data.Approved {
				// 使用保存的参数执行原始工具
				return endpoint(ctx, storedArgs, opts...)
			}
			// 如果有拒绝原因
			if data.DisapproveReason != nil {
				// 返回拒绝信息
				return fmt.Sprintf("tool '%s' disapproved: %s", tCtx.Name, *data.DisapproveReason), nil
			}
			// 返回通用拒绝信息
			return fmt.Sprintf("tool '%s' disapproved", tCtx.Name), nil
		}

		// 检查是否为任意类型的恢复上下文
		isTarget, _, _ = tool.GetResumeContext[any](ctx)
		// 如果不是目标中断
		if !isTarget {
			// 重新触发中断
			return "", tool.StatefulInterrupt(ctx, &commontool.ApprovalInfo{
				// 工具名称
				ToolName: tCtx.Name,
				// 保存的参数
				ArgumentsInJSON: storedArgs,
			}, storedArgs)
		}

		// 使用保存的参数执行原始工具
		return endpoint(ctx, storedArgs, opts...)
	}, nil
}
