package main

import (
	"a2ui/mem"
	adkstore "a2ui/store"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"a2ui/a2ui"

	commontool "github.com/cloudwego/eino-examples/adk/common/tool"
	"github.com/cloudwego/eino-examples/adk/common/tool/graphtool"
	"github.com/cloudwego/eino-examples/compose/batch/batch"
	localbk "github.com/cloudwego/eino-ext/adk/backend/local"
	clc "github.com/cloudwego/eino-ext/callbacks/cozeloop"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/prebuilt/deep"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/cloudwego/hertz/pkg/app"
	hserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/coze-dev/cozeloop-go"
	"github.com/google/uuid"
	"github.com/hertz-contrib/sse"

	"github.com/cloudwego/eino-ext/components/model/deepseek"
)

const cozeLoopWorkspaceId = "7480846041387237385"

const sessionsDir = "data/sessions"

func main() {

	ctx := context.Background()

	cozeLoopApiToken := os.Getenv("COZELOOP_API_TOKEN")
	if cozeLoopApiToken != "" {
		// 创建 CozeLoop 客户端
		client, err := cozeloop.NewClient(
			cozeloop.WithAPIToken(cozeLoopApiToken),
			cozeloop.WithWorkspaceID(cozeLoopWorkspaceId),
		)
		if err != nil {
			log.Fatalf("创建 CozeLoop 客户端失败: %v", err)
		}
		//函数结束关闭
		defer func() {
			time.Sleep(5 * time.Second)
			client.Close(ctx)
		}()
		// 注册为全局 Callback
		callbacks.AppendGlobalHandlers(clc.NewLoopHandler(client))
		log.Println("CozeLoop tracing enabled")
	} else {
		log.Println("CozeLoop tracing disabled (set COZELOOP_API_TOKEN to enable)")
	}

	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		log.Fatalf("请设置环境变量 DEEPSEEK_API_KEY")
	}
	chatModel, err := deepseek.NewChatModel(ctx, &deepseek.ChatModelConfig{
		APIKey:  apiKey,
		BaseURL: "https://api.deepseek.com",
		Model:   "deepseek-v4-flash",
	})
	if err != nil {
		log.Fatalf("创建模型失败: %v", err)
	}
	workflowTool, err := createWorkflowTool(ctx, chatModel)
	if err != nil {
		log.Fatalf("创建流程失败: %v", err)
	}
	//backend, err := localbk.NewBackend(ctx, &localbk.Config{})

	handlers := []adk.ChatModelAgentMiddleware{
		//注册审批中间件
		newApprovalMiddleware[*schema.Message](),
		//注册安全工具中间件
		newSafeToolMiddleware[*schema.Message](),
	}
	backend, err := localbk.NewBackend(ctx, &localbk.Config{})
	if err != nil {
		log.Fatalf("创建文件系统失败: %v", err)
	}
	// ========== 2. 创建智能体 ==========
	agent, err := deep.New(ctx, &deep.Config{
		Name:           "调用工具智能体",
		Description:    "调用工具智能体",
		ChatModel:      chatModel,
		Backend:        backend, // 提供文件系统操作能力
		StreamingShell: backend, // 提供命令执行能力
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{workflowTool},
			},
		},
		ModelRetryConfig: &adk.ModelRetryConfig{
			MaxRetries: 5,
			IsRetryAble: func(_ context.Context, err error) bool {
				return strings.Contains(err.Error(), "429") ||
					strings.Contains(err.Error(), "Too Many Requests") ||
					strings.Contains(err.Error(), "qpm limit")
			},
		},
		MaxIteration: 50,
		Handlers:     handlers,
	})

	if err != nil {
		log.Fatalf("创建智能体失败: %v", err)
	}

	//启动多轮对话 开启流式输出
	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
		CheckPointStore: adkstore.NewInMemoryStore(),
	})

	store, err := mem.NewStore(sessionsDir)

	workspaceDir := os.Getenv("WORKSPACE_DIR")
	if workspaceDir == "" {
		workspaceDir = "./data/workspace"
	}

	projectRoot := os.Getenv("PROJECT_ROOT")
	if projectRoot == "" {
		if cwd, err := os.Getwd(); err == nil {
			projectRoot = cwd
		}
	}
	if abs, err := filepath.Abs(projectRoot); err == nil {
		projectRoot = abs
	}
	log.Printf("project root: %s", projectRoot)

	examplesDir := os.Getenv("EXAMPLES_DIR")
	if examplesDir == "" {
		candidate := filepath.Join(projectRoot, "examples")
		if fi, err := os.Stat(candidate); err == nil && fi.IsDir() {
			examplesDir = candidate
		} else {
			examplesDir = projectRoot
		}
	}
	if abs, err := filepath.Abs(examplesDir); err == nil {
		examplesDir = abs
	}
	log.Printf("examples dir: %s", examplesDir)

	//服务启动端口
	port := "8085"

	srv := &server{
		runner:       runner,
		store:        store,
		workspaceDir: workspaceDir,
		projectRoot:  projectRoot,
		examplesDir:  examplesDir,
	}

	//启动http服务
	h := hserver.Default(hserver.WithHostPorts(":" + port))

	h.GET("/", func(_ context.Context, c *app.RequestContext) {
		data, err := os.ReadFile("static/index.html")
		if err != nil {
			c.JSON(consts.StatusNotFound, map[string]string{"error": "index.html not found"})
			return
		}
		c.Data(consts.StatusOK, "text/html; charset=utf-8", data)
	})

	h.POST("/sessions", func(_ context.Context, c *app.RequestContext) {
		id := uuid.New().String()
		if _, err := store.GetOrCreate(id); err != nil {
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		c.JSON(consts.StatusOK, map[string]string{"id": id})
	})

	h.GET("/sessions", func(_ context.Context, c *app.RequestContext) {
		metas, err := store.List()
		if err != nil {
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		if metas == nil {
			metas = []mem.SessionMeta{}
		}
		c.JSON(consts.StatusOK, metas)
	})

	h.DELETE("/sessions/:id", func(_ context.Context, c *app.RequestContext) {
		id := c.Param("id")
		if err := store.Delete(id); err != nil {
			c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		c.Status(consts.StatusNoContent)
	})

	h.POST("/sessions/:id/chat", func(ctx context.Context, c *app.RequestContext) {
		srv.handleChat(ctx, c)
	})

	h.GET("/sessions/:id/render", func(_ context.Context, c *app.RequestContext) {
		srv.handleRender(c)
	})

	h.POST("/sessions/:id/approve", func(ctx context.Context, c *app.RequestContext) {
		srv.handleApprove(ctx, c)
	})

	h.POST("/sessions/:id/abort", func(_ context.Context, c *app.RequestContext) {
		// No-op: abort requires TurnLoop (introduced in the next chapter).
		c.JSON(consts.StatusOK, map[string]string{"status": "not supported without TurnLoop"})
	})

	h.POST("/sessions/:id/docs", func(_ context.Context, c *app.RequestContext) {
		srv.handleUpload(c)
	})

	log.Printf("starting server on http://localhost:%s", port)
	h.Spin()

}

type chatRequest struct {
	Message string `json:"message"`
}

type approveRequest struct {
	Approved    bool   `json:"approved"`
	Reason      string `json:"reason,omitempty"`
	AlwaysAllow bool   `json:"always_allow"`
	ToolName    string `json:"tool_name"`
}

func (s *server) handleChat(ctx context.Context, c *app.RequestContext) {
	id := c.Param("id")

	body, _ := c.Body()
	var req chatRequest
	if err := json.Unmarshal(body, &req); err != nil || req.Message == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "message is required"})
		return
	}

	log.Printf("[chat] session=%s msg=%q", id, req.Message)

	sess, err := s.store.GetOrCreate(id)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	userMsg := schema.UserMessage(req.Message)
	if err := sess.Append(userMsg); err != nil {
		log.Printf("warn: failed to persist user message: %v", err)
	}

	history := sess.GetMessages()
	runMessages := s.buildRunMessages(id, history)
	events := s.runner.Run(ctx, runMessages, adk.WithCheckPointID(id))

	stream := sse.NewStream(c)
	defer func() { _ = c.Flush() }()

	lastContent, intermediates, interruptID, _, streamErr := a2ui.StreamToWriter(
		&sseLineWriter{stream: stream}, id, history, events,
	)

	for _, msg := range intermediates {
		if appendErr := sess.Append(msg); appendErr != nil {
			log.Printf("warn: failed to persist intermediate: %v", appendErr)
		}
	}

	if interruptID != "" {
		sess.SetPendingInterruptID(interruptID)
		log.Printf("[chat] session=%s interrupted: id=%s", id, interruptID)
	} else if streamErr != nil {
		log.Printf("[chat] session=%s error: %v", id, streamErr)
	} else {
		log.Printf("[chat] session=%s done, response=%d chars", id, len(lastContent))
	}
}

func (s *server) handleRender(c *app.RequestContext) {
	id := c.Param("id")
	sess, err := s.store.GetOrCreate(id)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	var buf bytes.Buffer
	if err := a2ui.RenderHistory(&buf, id, sess.GetMessages()); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	c.Data(consts.StatusOK, "application/x-ndjson", buf.Bytes())
}

func (s *server) handleApprove(ctx context.Context, c *app.RequestContext) {
	id := c.Param("id")

	sess, err := s.store.GetOrCreate(id)
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	interruptID := sess.GetPendingInterruptID()
	if interruptID == "" {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "no pending interrupt for this session"})
		return
	}

	body, _ := c.Body()
	var req approveRequest
	if err := json.Unmarshal(body, &req); err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	var reason *string
	if req.Reason != "" {
		reason = &req.Reason
	}
	result := &commontool.ApprovalResult{Approved: req.Approved, DisapproveReason: reason}

	sess.SetPendingInterruptID("")

	log.Printf("[approve] session=%s interruptID=%s approved=%v", id, interruptID, req.Approved)

	// Resume via Runner with the approval decision.
	events, err2 := s.runner.ResumeWithParams(ctx, id, &adk.ResumeParams{
		Targets: map[string]any{interruptID: result},
	})
	if err2 != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err2.Error()})
		return
	}

	stream := sse.NewStream(c)
	defer func() { _ = c.Flush() }()

	msgIdx := sess.GetMsgIdx()
	lastContent, intermediates, newInterruptID, finalMsgIdx, streamErr := a2ui.StreamContinue(
		&sseLineWriter{stream: stream}, id, msgIdx, events,
	)

	// 持久化审批过程的中间消息（工具结果 + 后续助理响应），
	// 确保 session 消息序列保持完整，避免出现"孤儿"的 tool_call。
	for _, msg := range intermediates {
		if appendErr := sess.Append(msg); appendErr != nil {
			log.Printf("[approve] warn: failed to persist intermediate: %v", appendErr)
		}
	}

	if newInterruptID != "" {
		sess.SetPendingInterruptID(newInterruptID)
		sess.SetMsgIdx(finalMsgIdx)
		log.Printf("[approve] session=%s re-interrupted: id=%s", id, newInterruptID)
	} else if streamErr != nil {
		log.Printf("[approve] session=%s stream error: %v", id, streamErr)
	} else {
		log.Printf("[approve] session=%s done, response=%d chars", id, len(lastContent))
	}
}

func (s *server) handleUpload(c *app.RequestContext) {
	id := c.Param("id")

	absWorkDir, err := filepath.Abs(filepath.Join(s.workspaceDir, id))
	if err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := os.MkdirAll(absWorkDir, 0o755); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(consts.StatusBadRequest, map[string]string{"error": "file field is required"})
		return
	}

	dst := filepath.Join(absWorkDir, filepath.Base(fileHeader.Filename))
	if err := c.SaveUploadedFile(fileHeader, dst); err != nil {
		c.JSON(consts.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	c.JSON(consts.StatusOK, map[string]string{
		"name": fileHeader.Filename,
		"path": dst,
	})
}

type sseLineWriter struct {
	stream *sse.Stream
	buf    []byte
}

func (w *sseLineWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		idx := -1
		for i, b := range w.buf {
			if b == '\n' {
				idx = i
				break
			}
		}
		if idx < 0 {
			break
		}
		line := w.buf[:idx]
		w.buf = w.buf[idx+1:]
		if len(line) == 0 {
			continue
		}
		if err := w.stream.Publish(&sse.Event{Data: line}); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (s *server) buildRunMessages(sessionID string, history []*schema.Message) []*schema.Message {
	var lines []string
	lines = append(lines, "[Context]")
	lines = append(lines,
		"IMPORTANT RULES:",
		"  1. Always use filesystem tools to look up real code before answering. Do not guess or make up information.",
		"  2. After using tools (even if they return no results), you MUST write a text response to the user summarizing what you found.",
		"  3. Never end your turn without a text response — tool calls alone are not sufficient.",
		"  4. When asked to build or test code, use the execute tool to run the command.",
		"     Each Go example has its own go.mod. To build an example, run:",
		"       cd <example-dir> && go build ./...",
		"     NEVER assume a build succeeded without actually running it.",
		"  5. When writing or editing a file and then claiming it compiles, you MUST run the build tool to verify.",
	)

	if s.projectRoot != "" {
		lines = append(lines,
			fmt.Sprintf("Project root: %s", s.projectRoot),
			"  IMPORTANT: Always pass the project root as the path argument when using filesystem tools.",
			fmt.Sprintf("  - grep(pattern=\"...\", path=\"%s\")", s.projectRoot),
			fmt.Sprintf("  - glob(pattern=\"%s/**/*.go\")", s.projectRoot),
			fmt.Sprintf("  - read_file(file_path=\"%s/some/file.go\")", s.projectRoot),
			"  grep and glob recurse into ALL subdirectories under the given path.",
			"  Top-level subdirectories of the project root:",
		)
		if entries, err := os.ReadDir(s.projectRoot); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					lines = append(lines, "    - "+filepath.Join(s.projectRoot, e.Name())+"/")
				}
			}
		}
		lines = append(lines, "  Use these tools to read actual source code before answering questions about the codebase.")
	}

	if s.examplesDir != "" && s.examplesDir != s.projectRoot {
		lines = append(lines,
			fmt.Sprintf("eino-examples directory: %s", s.examplesDir),
			"  When the user asks about examples or sample code, search here specifically:",
			fmt.Sprintf("  - grep(pattern=\"...\", path=\"%s\")", s.examplesDir),
			fmt.Sprintf("  - glob(pattern=\"%s/**/*.go\")", s.examplesDir),
		)
	}

	absWorkDir, err := filepath.Abs(filepath.Join(s.workspaceDir, sessionID))
	if err == nil {
		entries, _ := os.ReadDir(absWorkDir)
		var uploadedFiles []string
		for _, e := range entries {
			if !e.IsDir() {
				uploadedFiles = append(uploadedFiles, filepath.Join(absWorkDir, e.Name()))
			}
		}
		if len(uploadedFiles) > 0 {
			lines = append(lines,
				fmt.Sprintf("Session workspace: %s", absWorkDir),
				"  Uploaded files:",
			)
			for _, f := range uploadedFiles {
				lines = append(lines, "    - "+f)
			}
		}
	}

	ctx := strings.Join(lines, "\n")
	runMessages := make([]*schema.Message, 0, len(history)+1)

	runMessages = append(runMessages, schema.UserMessage(ctx))
	// 过滤掉孤儿 tool_call 消息（有 tool_calls 但没有匹配的 Tool 结果），
	// 防止发送给模型 API 时出现 invalid_request_error。
	runMessages = append(runMessages, filterOrphanedToolCalls(history)...)
	return runMessages
}

// filterOrphanedToolCalls removes assistant messages with tool_calls that are
// NOT followed by matching tool result messages. This prevents sending the model
// API an invalid message sequence where an assistant's tool_calls have no
// corresponding tool results — which would cause the API to reject the request.
//
// A message sequence like:
//
//	UserMsg → AssistantMsg(tool_calls=[...]) → UserMsg2
//
// is invalid. The correct sequence is:
//
//	UserMsg → AssistantMsg(tool_calls=[...]) → ToolMsg(...) → AssistantMsg(...)
//
// Orphaned tool_call messages occur when the agent was interrupted (e.g. for
// human approval) and the interruption's intermediate messages were persisted
// to session but the subsequent resume output (tool results + final response)
// was not.
func filterOrphanedToolCalls(history []*schema.Message) []*schema.Message {
	var result []*schema.Message
	i := 0
	for i < len(history) {
		msg := history[i]
		if msg.Role == schema.Assistant && len(msg.ToolCalls) > 0 {
			// Count immediate consecutive tool result messages following this assistant message.
			toolResultCount := 0
			j := i + 1
			for j < len(history) && history[j].Role == schema.Tool {
				toolResultCount++
				j++
			}
			if toolResultCount < len(msg.ToolCalls) {
				// Orphaned: not enough tool results, skip this assistant message.
				i++
				continue
			}
		}
		result = append(result, msg)
		i++
	}
	return result
}

type server struct {
	runner       *adk.Runner
	store        *mem.Store
	workspaceDir string
	projectRoot  string
	examplesDir  string
}

/**
 * 定义工作流tool的输入
 */
type Input struct {
	FilePath string `json:"file_path" jsonschema:"description=Absolute path to the uploaded document file"`
	Question string `json:"question"  jsonschema:"description=The question to answer from the document"`
}

/**
 * 定义工作流tool的输出
 */
type Output struct {
	Answer  string   `json:"answer"`
	Sources []string `json:"sources"` // key excerpts used to produce the answer
}

// scoreTask 是输入到内部 BatchNode 工作流的每个块的输入。
type ScoreTask struct {
	Text     string // 块的文本内容
	Question string // 用户问题
}

// scoredChunk 是由内部 BatchNode 工作流产生的每个块的评分结果。
type ScoredChunk struct {
	Text    string // 原始文本块¬
	Score   int    // 0-10 相关性评分
	Excerpt string // 最相关的句子或短语
}

type SynthIn struct {
	TopK     []ScoredChunk
	Question string
}

/**
 * 构建工作流tool
 */
func createWorkflowTool(_ context.Context, cm model.BaseModel[*schema.Message]) (tool.BaseTool, error) {
	//主流程工作流
	var fullWorkflow = compose.NewWorkflow[Input, Output]()
	//构建加载文件节点
	fullWorkflow.AddLambdaNode("load", compose.InvokableLambda(
		//in是START节点的输入
		func(ctx context.Context, in Input) ([]*schema.Document, error) {
			data, err := os.ReadFile(in.FilePath)
			if err != nil {
				return nil, fmt.Errorf("read %q: %w", in.FilePath, err)
			}
			return []*schema.Document{{Content: string(data)}}, nil
		},
	)).AddInput(compose.START)

	//构建分块节点 将文档分成每个800字符的片段 连接到load
	fullWorkflow.AddLambdaNode("chunk", compose.InvokableLambda(
		//docs 是load节点的输出
		func(ctx context.Context, docs []*schema.Document) ([]*schema.Document, error) {
			var out []*schema.Document
			for _, d := range docs {
				out = append(out, splitIntoChunks(d.Content, 800)...)
			}
			return out, nil
		},
	)).AddInput("load")

	//构建评分工作流
	scoreWorkflow := newScoreWorkFlow(cm)
	//批量处理节点
	scorer := batch.NewBatchNode(&batch.NodeConfig[ScoreTask, ScoredChunk]{
		Name: "ChunkScorer",
		//传入工作流节点
		InnerTask: scoreWorkflow,
		//最大并发 0代表顺序执行(默认)
		MaxConcurrency: 5,
	})

	//构建主流程批量处理节点  使用scorer
	fullWorkflow.AddLambdaNode("score", compose.InvokableLambda(
		func(ctx context.Context, in map[string]any) ([]ScoredChunk, error) {
			//将从in获取的Chunks 拆分为ScoreTask前片
			chunks := in["Chunks"].([]*schema.Document)
			question := in["Question"].(string)
			tasks := make([]ScoreTask, len(chunks))
			for i, c := range chunks {
				tasks[i] = ScoreTask{Text: c.Content, Question: question}
			}
			//调用批量处理节点并行执行任务
			return scorer.Invoke(ctx, tasks)
		},
	)).
		AddInputWithOptions("chunk",
			[]*compose.FieldMapping{compose.ToField("Chunks")},
			compose.WithNoDirectDependency()).
		AddInputWithOptions(compose.START,
			[]*compose.FieldMapping{compose.MapFields("Question", "Question")},
			compose.WithNoDirectDependency())
	//按分数降序排序 保留三个分数最高的
	fullWorkflow.AddLambdaNode("filter", compose.InvokableLambda(
		//scored score节点的输出
		func(ctx context.Context, scored []ScoredChunk) ([]ScoredChunk, error) {
			sort.Slice(scored, func(i, j int) bool {
				return scored[i].Score > scored[j].Score
			})
			const maxK = 3
			var top []ScoredChunk
			for _, c := range scored {
				if c.Score < 3 {
					break
				}
				top = append(top, c)
				if len(top) == maxK {
					break
				}
			}
			return top, nil
		},
	)).AddInput("score")

	//配置响应回答
	fullWorkflow.AddLambdaNode("answer", compose.InvokableLambda(
		func(ctx context.Context, in map[string]any) (Output, error) {
			topK := in["TopK"].([]ScoredChunk)
			question := in["Question"].(string)
			if len(topK) == 0 {
				return Output{
					Answer: fmt.Sprintf("No relevant content found in the document for: %q", question),
				}, nil
			}
			return synthesize(ctx, cm, SynthIn{TopK: topK, Question: question})
		},
	)).
		AddInputWithOptions("filter",
			[]*compose.FieldMapping{compose.ToField("TopK")}, compose.WithNoDirectDependency()).
		AddInputWithOptions(compose.START,
			[]*compose.FieldMapping{compose.MapFields("Question", "Question")}, compose.WithNoDirectDependency())

	// END receives output from answer.
	fullWorkflow.End().
		AddInput("answer")

	//构建工具封装工作流
	return graphtool.NewInvokableGraphTool[Input, Output](
		fullWorkflow,
		"answer_from_document",
		"在用户上传的大型文档中搜索与问题相关的内容，并从最相关的段落中综合出带引用的答案。如果用户上传过文档，一定要使用该工具处理，不要使用read_file。",
	)
}

func newScoreWorkFlow(cm model.BaseModel[*schema.Message]) *compose.Workflow[ScoreTask, ScoredChunk] {
	scoreWorkflow := compose.NewWorkflow[ScoreTask, ScoredChunk]()
	//给工作流添加score_chunk 节点 前面接入系统的START节点
	scoreWorkflow.AddLambdaNode("score_chunk", compose.InvokableLambda(
		func(ctx context.Context, t ScoreTask) (ScoredChunk, error) {
			prompt := fmt.Sprintf(`Rate how relevant the following text chunk is to the question.
Question: %s
Chunk:
%s
Reply with JSON only — no explanation, no markdown fences:
{"score": <0-10>, "excerpt": "<most relevant sentence or phrase, empty string if score is 0>"}
Score guide: 0=completely irrelevant, 3=tangentially related, 7=clearly relevant, 10=directly answers the question.`,
				t.Question, t.Text)
			//封装消息
			userMessage := schema.UserMessage(prompt)
			messages := []*schema.Message{userMessage}

			resp, err := cm.Generate(ctx, messages)
			if err != nil {
				return ScoredChunk{Text: t.Text, Score: 0}, nil
			}
			content := strings.TrimSpace(messageText(resp))
			// strip optional markdown code block wrapper
			content = strings.TrimPrefix(content, "```json")
			content = strings.TrimPrefix(content, "```")
			content = strings.TrimSuffix(content, "```")
			content = strings.TrimSpace(content)
			var sr struct {
				Score   int    `json:"score"`
				Excerpt string `json:"excerpt"`
			}
			//解析为json  sr
			if err := json.Unmarshal([]byte(content), &sr); err != nil {
				return ScoredChunk{Text: t.Text, Score: 0}, nil
			}
			return ScoredChunk{Text: t.Text, Score: sr.Score, Excerpt: sr.Excerpt}, nil
		},
	)).AddInput(compose.START)
	//从scoreWorkflow工作流寻找End节点 有就直接返回并且设置连接score_chunk
	//没有End节点则创建End节点并设置连接score_chunk
	scoreWorkflow.End().AddInput("score_chunk")
	return scoreWorkflow
}

func messageText(msg *schema.Message) string {
	if msg == nil {
		return ""
	}
	if msg.Content != "" {
		return msg.Content
	}
	var parts []string
	for _, part := range msg.UserInputMultiContent {
		if part.Type == schema.ChatMessagePartTypeText && part.Text != "" {
			parts = append(parts, part.Text)
		}
	}
	for _, part := range msg.AssistantGenMultiContent {
		if part.Type == schema.ChatMessagePartTypeText && part.Text != "" {
			parts = append(parts, part.Text)
		}
	}
	return strings.Join(parts, "\n")
}

/**
 * 将文本拆分为块
 */
func splitIntoChunks(text string, chunkSize int) []*schema.Document {
	var chunks []*schema.Document
	var buf strings.Builder

	flush := func() {
		s := strings.TrimSpace(buf.String())
		if s != "" {
			chunks = append(chunks, &schema.Document{Content: s})
		}
		buf.Reset()
	}

	for _, para := range strings.Split(text, "\n\n") {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}
		if buf.Len()+len(para)+2 > chunkSize && buf.Len() > 0 {
			flush()
		}
		// paragraph itself exceeds chunkSize: split by line
		if len(para) > chunkSize {
			for _, line := range strings.Split(para, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				if buf.Len()+len(line)+1 > chunkSize && buf.Len() > 0 {
					flush()
				}
				if buf.Len() > 0 {
					buf.WriteByte('\n')
				}
				buf.WriteString(line)
			}
		} else {
			if buf.Len() > 0 {
				buf.WriteString("\n\n")
			}
			buf.WriteString(para)
		}
	}
	flush()
	return chunks
}

func synthesize(ctx context.Context, cm model.BaseModel[*schema.Message], in SynthIn) (Output, error) {
	var sb strings.Builder
	sb.WriteString("Answer the following question using only the provided document excerpts.\n\n")
	sb.WriteString("Question: ")
	sb.WriteString(in.Question)
	sb.WriteString("\n\nDocument excerpts:\n")

	sources := make([]string, len(in.TopK))
	for i, c := range in.TopK {
		excerpt := c.Excerpt
		if excerpt == "" {
			excerpt = c.Text
		}
		sources[i] = excerpt
		fmt.Fprintf(&sb, "[%d] %s\n\n", i+1, excerpt)
	}
	sb.WriteString("Provide a clear, concise answer. Cite excerpt numbers like [1] when referencing sources.")

	messages := []*schema.Message{
		schema.UserMessage(sb.String()),
	}
	resp, err := cm.Generate(ctx, messages)
	if err != nil {
		return Output{}, fmt.Errorf("synthesize: %w", err)
	}
	return Output{Answer: messageText(resp), Sources: sources}, nil
}

func newSafeToolMiddleware[M adk.MessageType]() adk.TypedChatModelAgentMiddleware[M] {
	return &safeToolMiddleware[M]{
		TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[M]{},
	}
}

type safeToolMiddleware[M adk.MessageType] struct {
	*adk.TypedBaseChatModelAgentMiddleware[M]
}

func (m *safeToolMiddleware[M]) WrapInvokableToolCall(
	_ context.Context,
	endpoint adk.InvokableToolCallEndpoint,
	_ *adk.ToolContext,
) (adk.InvokableToolCallEndpoint, error) {
	return func(ctx context.Context, args string, opts ...tool.Option) (string, error) {
		result, err := endpoint(ctx, args, opts...)
		if err != nil {
			if _, ok := compose.IsInterruptRerunError(err); ok {
				return "", err
			}
			return fmt.Sprintf("[tool error] %v", err), nil
		}
		return result, nil
	}, nil
}

func (m *safeToolMiddleware[M]) WrapStreamableToolCall(
	_ context.Context,
	endpoint adk.StreamableToolCallEndpoint,
	_ *adk.ToolContext,
) (adk.StreamableToolCallEndpoint, error) {
	return func(ctx context.Context, args string, opts ...tool.Option) (*schema.StreamReader[string], error) {
		sr, err := endpoint(ctx, args, opts...)
		if err != nil {
			if _, ok := compose.IsInterruptRerunError(err); ok {
				return nil, err
			}
			return SingleChunkReader(fmt.Sprintf("[tool error] %v", err)), nil
		}
		return safeWrapReader(sr), nil
	}, nil
}

// SingleChunkReader returns a StreamReader that emits one string then EOF.
func SingleChunkReader(msg string) *schema.StreamReader[string] {
	r, w := schema.Pipe[string](1)
	_ = w.Send(msg, nil)
	w.Close()
	return r
}

// safeWrapReader proxies chunks from sr; on a stream error it emits the error
// as a final chunk instead of propagating it, so the model sees a complete
// (if error-annotated) tool result rather than a pipeline failure.
func safeWrapReader(sr *schema.StreamReader[string]) *schema.StreamReader[string] {
	r, w := schema.Pipe[string](64)
	go func() {
		defer w.Close()
		for {
			chunk, err := sr.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				_ = w.Send(fmt.Sprintf("\n[tool error] %v", err), nil)
				return
			}
			_ = w.Send(chunk, nil)
		}
	}()
	return r
}

type approvalMiddleware[M adk.MessageType] struct {
	*adk.TypedBaseChatModelAgentMiddleware[M]
}

func newApprovalMiddleware[M adk.MessageType]() adk.TypedChatModelAgentMiddleware[M] {
	return &approvalMiddleware[M]{
		TypedBaseChatModelAgentMiddleware: &adk.TypedBaseChatModelAgentMiddleware[M]{},
	}
}

func (m *approvalMiddleware[M]) WrapInvokableToolCall(
	_ context.Context,
	endpoint adk.InvokableToolCallEndpoint,
	tCtx *adk.ToolContext,
) (adk.InvokableToolCallEndpoint, error) {
	if tCtx.Name != "answer_from_document" {
		return endpoint, nil
	}
	return func(ctx context.Context, args string, opts ...tool.Option) (string, error) {
		wasInterrupted, _, storedArgs := tool.GetInterruptState[string](ctx)
		if !wasInterrupted {
			return "", tool.StatefulInterrupt(ctx, &commontool.ApprovalInfo{
				ToolName:        tCtx.Name,
				ArgumentsInJSON: args,
			}, args)
		}

		isTarget, hasData, data := tool.GetResumeContext[*commontool.ApprovalResult](ctx)
		if isTarget && hasData {
			if data.Approved {
				return endpoint(ctx, storedArgs, opts...)
			}
			if data.DisapproveReason != nil {
				return fmt.Sprintf("tool '%s' disapproved: %s", tCtx.Name, *data.DisapproveReason), nil
			}
			return fmt.Sprintf("tool '%s' disapproved", tCtx.Name), nil
		}

		isTarget, _, _ = tool.GetResumeContext[any](ctx)
		if !isTarget {
			return "", tool.StatefulInterrupt(ctx, &commontool.ApprovalInfo{
				ToolName:        tCtx.Name,
				ArgumentsInJSON: storedArgs,
			}, storedArgs)
		}

		return endpoint(ctx, storedArgs, opts...)
	}, nil
}
