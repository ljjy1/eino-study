package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cloudwego/eino-examples/adk/common/tool/graphtool"
	"github.com/cloudwego/eino-examples/compose/batch/batch"
	clc "github.com/cloudwego/eino-ext/callbacks/cozeloop"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"github.com/coze-dev/cozeloop-go"

	"github.com/cloudwego/eino-ext/components/model/deepseek"
)

const cozeLoopWorkspaceId = "7480846041387237385"

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
	tools := []tool.BaseTool{
		workflowTool,
	}
	// ========== 2. 创建智能体 ==========

	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:        "tool_search_agent",
		Description: "An agent that can dynamically search and use tools from a large tool library",
		Instruction: `You are a helpful assistant.`,
		Model:       chatModel,
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: tools,
			},
		},
	})

	if err != nil {
		log.Fatalf("创建智能体失败: %v", err)
	}
	//启动多轮对话 开启流式输出
	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
	})

	//创建切片存储聊天记录 长度0 容量16
	history := make([]*schema.Message, 0, 16)

	//获取控制台输入
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("（输入 exit / quit / 再见 / 退出 结束对话）")
	fmt.Println(strings.Repeat("─", 40))

	for {
		fmt.Fprint(os.Stdout, "你> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue // 空行跳过，不退出
		}

		// 检查退出条件
		switch strings.ToLower(line) {
		case "exit", "quit", "再见", "退出", "bye":
			return
		}

		// 将用户消息添加到历史记录
		history = append(history, schema.UserMessage(line))

		// 调用 Runner 进行推理（流式输出）
		fmt.Fprint(os.Stdout, "智能体> ")
		iter := runner.Run(ctx, history)

		var fullResponse string
	eventLoop:
		for {
			event, ok := iter.Next()
			if !ok {
				break // 事件流结束
			}
			if event.Err != nil {
				log.Printf("推理出错: %v", event.Err)
				break
			}
			if event.Output != nil && event.Output.MessageOutput != nil {
				mv := event.Output.MessageOutput

				// 🐛 BUG 修复：过滤掉工具结果事件（Role=Tool）
				// ReAct 循环中，工具结果被发射为独立的事件，内容是原始 JSON。
				// 旧的代码将所有非流式消息都当成助手输出处理，导致：
				// 1. 原始 JSON 被打印到终端，污染用户界面
				// 2. JSON 覆盖 fullResponse，后续流式文本追加在后面
				// 3. 被污染的 fullResponse 存入 history，下一轮模型看到 JSON 开头的"助手消息"后被搞糊涂
				// 4. 模型可能生成 days=0 的 get_forecast 调用 → make([]DayForecast, 0) → 空切片
				if mv.Role != "" && mv.Role != schema.Assistant {
					// 流式事件需要排空，防止 goroutine 泄漏
					if mv.IsStreaming && mv.MessageStream != nil {
						for {
							_, err := mv.MessageStream.Recv()
							if err != nil {
								break
							}
						}
					}
					continue eventLoop
				}

				if mv.IsStreaming && mv.MessageStream != nil {
					// 流式模式：逐 chunk 消费
					for {
						chunk, err := mv.MessageStream.Recv()
						if err != nil {
							break // io.EOF，流结束
						}
						if chunk != nil && chunk.Content != "" {
							fmt.Print(chunk.Content)
							fullResponse += chunk.Content
						}
					}
				} else if mv.Message != nil {
					// 非流式模式：直接输出完整消息（只有纯文本回复才会走这里）
					content := mv.Message.Content
					if content != "" {
						fmt.Print(content)
						fullResponse = content
					}
				}
			}
		}
		fmt.Println() // 输出换行，准备下一轮输入

		// 将 AI 回复加入历史记录，后续对话会携带完整上下文
		if fullResponse != "" {
			history = append(history, schema.AssistantMessage(fullResponse, nil))
		}
	}

	// 检查 scanner 是否因错误退出
	if err := scanner.Err(); err != nil {
		log.Printf("读取输入出错: %v", err)
	}

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
	Text    string // 原始文本块
	Score   int    // 0-10 相关性评分
	Excerpt string // 最相关的句子或短语
}

type ScoreIn struct {
	//拆分的文档分块
	Chunks []*schema.Document
	//用户问题
	Question string
}

type SynthIn struct {
	TopK     []ScoredChunk
	Question string
}

/**
 * 构建工作流tool
 */
func createWorkflowTool(ctx context.Context, cm model.BaseModel[*schema.Message]) (tool.BaseTool, error) {
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
		//in 自定义封装的参数 通过chunk节点和START节点组合来的参数
		func(ctx context.Context, in ScoreIn) ([]ScoredChunk, error) {
			//将从in获取的Chunks 拆分为ScoreTask前片
			tasks := make([]ScoreTask, len(in.Chunks))
			for i, c := range in.Chunks {
				tasks[i] = ScoreTask{Text: c.Content, Question: in.Question}
			}
			//调用批量处理节点并行执行任务
			return scorer.Invoke(ctx, tasks)
		},
	)).
		AddInputWithOptions("chunk",
			[]*compose.FieldMapping{compose.ToField("Chunks")}).
		AddInputWithOptions(compose.START,
			[]*compose.FieldMapping{compose.MapFields("Question", "Question")})

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
		func(ctx context.Context, in SynthIn) (Output, error) {
			if len(in.TopK) == 0 {
				return Output{
					Answer: fmt.Sprintf("No relevant content found in the document for: %q", in.Question),
				}, nil
			}
			return synthesize(ctx, cm, in)
		},
	)).
		AddInputWithOptions("filter",
			[]*compose.FieldMapping{compose.ToField("TopK")}).
		AddInputWithOptions(compose.START,
			[]*compose.FieldMapping{compose.MapFields("Question", "Question")})

	// END receives output from answer.
	fullWorkflow.End().
		AddInput("answer")

	//构建工具封装工作流
	return graphtool.NewInvokableGraphTool[Input, Output](
		fullWorkflow,
		"answer_from_document",
		"在用户上传的大型文档中搜索与问题相关的内容，并从最相关的段落中综合出带引用的答案。如果用户上传了文档，一定要使用该工具处理，不要使用read_file。",
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
