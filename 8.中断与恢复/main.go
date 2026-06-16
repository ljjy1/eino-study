package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	clc "github.com/cloudwego/eino-ext/callbacks/cozeloop"
	"github.com/cloudwego/eino-ext/components/model/deepseek"
	"github.com/cloudwego/eino/callbacks"
	"github.com/coze-dev/cozeloop-go"

	commontool "github.com/cloudwego/eino-examples/adk/common/tool"
)

// memoryStore 内存中的 CheckPointStore，用于中断/恢复的状态保存
type memoryStore struct {
	m map[string][]byte
}

func (s *memoryStore) Get(_ context.Context, checkPointID string) ([]byte, bool, error) {
	data, ok := s.m[checkPointID]
	return data, ok, nil
}

func (s *memoryStore) Set(_ context.Context, checkPointID string, checkPoint []byte) error {
	s.m[checkPointID] = checkPoint
	return nil
}

type GetWeatherInput struct {
	Location string `json:"location" jsonschema:"description=The city and state, e.g. San Francisco, CA"`
	Unit     string `json:"unit,omitempty" jsonschema:"enum=celsius,enum=fahrenheit,description=The unit of temperature"`
}

type GetWeatherOutput struct {
	Temperature float64 `json:"temperature"`
	Unit        string  `json:"unit"`
	Condition   string  `json:"condition"`
}

type GetForecastInput struct {
	Location string `json:"location" jsonschema:"description=The city and state"`
	Days     int    `json:"days" jsonschema:"description=Number of days to forecast (1-10)"`
}

type DayForecast struct {
	Day         string  `json:"day"`
	Temperature float64 `json:"temperature"`
	Condition   string  `json:"condition"`
}
type GetForecastOutput struct {
	Forecasts []DayForecast `json:"forecasts"`
}

type GetStockPriceInput struct {
	Ticker         string `json:"ticker" jsonschema:"description=Stock ticker symbol (e.g., AAPL, GOOGL)"`
	IncludeHistory bool   `json:"include_history,omitempty" jsonschema:"description=Include historical data"`
}

type GetStockPriceOutput struct {
	Ticker string  `json:"ticker"`
	Price  float64 `json:"price"`
	Change float64 `json:"change"`
}

type ConvertCurrencyInput struct {
	Amount       float64 `json:"amount" jsonschema:"description=Amount to convert"`
	FromCurrency string  `json:"from_currency" jsonschema:"description=Source currency code (e.g., USD)"`
	ToCurrency   string  `json:"to_currency" jsonschema:"description=Target currency code (e.g., EUR)"`
}

type ConvertCurrencyOutput struct {
	OriginalAmount  float64 `json:"original_amount"`
	ConvertedAmount float64 `json:"converted_amount"`
	ExchangeRate    float64 `json:"exchange_rate"`
}

const cozeLoopWorkspaceId = "7480846041387237385"

func main() {
	ctx := context.Background()

	cozeLoopApiToken := os.Getenv("COZELOOP_API_TOKEN")
	if cozeLoopApiToken != "" {
		// 创建 CozeLoop 客户端
		client, err := cozeloop.NewClient(
			cozeloop.WithAPIToken(cozeLoopApiToken),
			cozeloop.WithWorkspaceID(cozeLoopWorkspaceId),
			//设置超大跟踪报告 避免长字段截断警告
			cozeloop.WithUltraLargeTraceReport(true),
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
	weatherTool := createWeatherTools()
	financeTools := createFinanceTools()
	allTools := append(weatherTool, financeTools...)

	// 创建审批中间件（保存引用，方便后续动态设置"总是同意"）
	approvalMw := newApprovalMiddleware()

	// ========== 2. 创建智能体 ==========
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:        "tool_search_agent",
		Description: "An agent that can dynamically search and use tools from a large tool library",
		Instruction: `You are a helpful assistant.`,
		Model:       chatModel,
		Handlers: []adk.ChatModelAgentMiddleware{
			approvalMw,
		},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: allTools,
			},
		},
	})

	if err != nil {
		log.Fatalf("创建智能体失败: %v", err)
	}
	// 创建内存 CheckPointStore，使中断/恢复机制生效
	cpStore := &memoryStore{m: make(map[string][]byte)}

	//启动多轮对话 开启流式输出
	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
		CheckPointStore: cpStore, // 必须设置 CheckPointStore，否则中断状态无法保存
	})

	//创建切片存储聊天记录 长度0 容量16
	history := make([]*schema.Message, 0, 16)

	//获取控制台输入
	scanner := bufio.NewScanner(os.Stdin)
	var cpIDSeq int // 用于生成唯一 checkpoint ID

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

		// 本轮对话的唯一 checkpoint ID，中断/恢复共用
		cpIDSeq++
		checkPointID := fmt.Sprintf("cp-%d", cpIDSeq)

		// 调用 Runner 进行推理（流式输出），传入 checkpoint ID 使中断恢复可用
		fmt.Fprint(os.Stdout, "智能体> ")
		iter := runner.Run(ctx, history, adk.WithCheckPointID(checkPointID))

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

			// ===== 检测中断事件（工具审批等需要用户确认的场景） =====
			if event.Action != nil && event.Action.Interrupted != nil {
				fullResponse = "" // 中断时没有模型输出，清空

				// 从中断上下文中找到根因（root cause），获取工具名称和参数
				rootCauseID, approvalInfo := extractApprovalInfo(event.Action.Interrupted)
				if rootCauseID == "" {
					log.Printf("无法定位中断根因，跳过审批")
					break
				}

				fmt.Println()
				fmt.Println(strings.Repeat("═", 40))
				fmt.Println("  ⚠ 需要您的审批")
				if approvalInfo != nil {
					fmt.Printf("  工具: %s\n", approvalInfo.ToolName)
					fmt.Printf("  参数: %s\n", approvalInfo.ArgumentsInJSON)
				}
				fmt.Println(strings.Repeat("─", 40))
				fmt.Print("  是否同意执行？[Y=同意 N=拒绝 A=总是同意(此工具不再审批)]: ")

				if !scanner.Scan() {
					break
				}
				userInput := strings.TrimSpace(scanner.Text())

				var approvalResult *commontool.ApprovalResult
				switch strings.ToUpper(userInput) {
				case "Y", "YES", "是", "同意":
					approvalResult = &commontool.ApprovalResult{Approved: true}
					fmt.Println("  ✓ 已同意，正在继续...")
				case "A", "ALWAYS", "总是同意":
					// 将此工具加入白名单，后续不再审批
					if approvalInfo != nil {
						approvalMw.SetPreApproved(approvalInfo.ToolName)
						fmt.Printf("  ✓ 已将 '%s' 加入白名单，后续不再审批\n", approvalInfo.ToolName)
					} else {
						fmt.Println("  ✓ 已同意，正在继续...")
					}
					approvalResult = &commontool.ApprovalResult{Approved: true}
				case "N", "NO", "不", "否", "不同意":
					reason := "用户拒绝了工具调用"
					approvalResult = &commontool.ApprovalResult{
						Approved:         false,
						DisapproveReason: &reason,
					}
					fmt.Println("  ✗ 已拒绝")
				default:
					// 其他文字视为拒绝并附上理由
					approvalResult = &commontool.ApprovalResult{
						Approved:         false,
						DisapproveReason: &userInput,
					}
					fmt.Println("  ✗ 已拒绝")
				}
				fmt.Println(strings.Repeat("═", 40))

				// 恢复 agent 执行：将审批结果注入到中断点
				resumeIter, err := runner.ResumeWithParams(ctx, checkPointID, &adk.ResumeParams{
					Targets: map[string]any{rootCauseID: approvalResult},
				})
				if err != nil {
					log.Printf("恢复执行失败: %v", err)
					break
				}

				// 替换 iter 为恢复后的 iterator，继续处理剩余事件（工具结果+模型回复）
				iter = resumeIter
				continue eventLoop
			}

			// ===== 正常输出处理 =====
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
 * 创建获取天气的工具
 */
func createWeatherTools() []tool.BaseTool {
	getWeather, _ := utils.InferTool(
		"get_weather",
		"Get the current weather in a given location",
		func(ctx context.Context, input *GetWeatherInput) (*GetWeatherOutput, error) {
			return &GetWeatherOutput{
				Temperature: 22.5,
				Unit:        input.Unit,
				Condition:   "Sunny",
			}, nil
		},
	)

	getForecast, _ := utils.InferTool(
		"get_forecast",
		"Get the weather forecast for multiple days ahead",
		func(ctx context.Context, input *GetForecastInput) (*GetForecastOutput, error) {
			forecasts := make([]DayForecast, input.Days)
			for i := 0; i < input.Days; i++ {
				forecasts[i] = DayForecast{
					Day:         fmt.Sprintf("Day %d", i+1),
					Temperature: 20.0 + float64(i),
					Condition:   "Partly Cloudy",
				}
			}
			return &GetForecastOutput{Forecasts: forecasts}, nil
		},
	)

	return []tool.BaseTool{getWeather, getForecast}
}

/**
 * 创建金融工具
 */
func createFinanceTools() []tool.BaseTool {
	getStockPrice, _ := utils.InferTool(
		"get_stock_price",
		"Get the current stock price and market data for a given ticker symbol",
		func(ctx context.Context, input *GetStockPriceInput) (*GetStockPriceOutput, error) {
			return &GetStockPriceOutput{
				Ticker: input.Ticker,
				Price:  150.25,
				Change: 2.5,
			}, nil
		},
	)

	convertCurrency, _ := utils.InferTool(
		"convert_currency",
		"Convert an amount from one currency to another using current exchange rates",
		func(ctx context.Context, input *ConvertCurrencyInput) (*ConvertCurrencyOutput, error) {
			rate := 0.85
			return &ConvertCurrencyOutput{
				OriginalAmount:  input.Amount,
				ConvertedAmount: input.Amount * rate,
				ExchangeRate:    rate,
			}, nil
		},
	)

	return []tool.BaseTool{getStockPrice, convertCurrency}
}

func newApprovalMiddleware() *approvalMiddleware {
	return &approvalMiddleware{
		BaseChatModelAgentMiddleware: &adk.BaseChatModelAgentMiddleware{},
		preApproved:                  make(map[string]bool),
	}
}

// approvalMiddleware 审批中间件结构体，用于拦截需要人工审批的 Tool 调用
type approvalMiddleware struct {
	// 嵌入基础聊天模型代理中间件，继承其基本功能
	*adk.BaseChatModelAgentMiddleware
	// preApproved 记录用户标记为"总是同意"的工具名，后续不再审批
	preApproved map[string]bool
}

// SetPreApproved 将指定工具加入白名单，后续调用不再触发审批中断
func (m *approvalMiddleware) SetPreApproved(toolName string) {
	m.preApproved[toolName] = true
}

// WrapInvokableToolCall 包装非流式（一次性返回结果）的 Tool 调用端点
// ctx: 上下文，用于传递请求级别的信息
// endpoint: 原始的 Tool 调用端点，实际执行 Tool 逻辑的地方
// tCtx: Tool 上下文，包含 Tool 名称等元信息
// 返回值: 包装后的端点函数，以及可能的错误
func (m *approvalMiddleware) WrapInvokableToolCall(
	_ context.Context,
	endpoint adk.InvokableToolCallEndpoint,
	tCtx *adk.ToolContext,
) (adk.InvokableToolCallEndpoint, error) {

	//tCtx.Name 需要拦截的工具名称 我这里拦截获取多日日期需要用户同意
	if tCtx.Name != "get_forecast" {
		return endpoint, nil
	}

	// 返回一个新的闭包端点函数，实现了审批拦截逻辑
	return func(ctx context.Context, args string, opts ...tool.Option) (string, error) {
		// 🚀 如果该工具已被用户标记为"总是同意"，直接放行
		if m.preApproved[tCtx.Name] {
			return endpoint(ctx, args, opts...)
		}

		// 从上下文中获取中断状态：wasInterrupted 表示之前是否被中断过，storedArgs 是之前存储的参数
		wasInterrupted, _, storedArgs := tool.GetInterruptState[string](ctx)

		// 如果之前没有被中断过，说明这是第一次调用，需要触发审批中断
		if !wasInterrupted {
			// 返回空字符串，并触发一个有状态中断，将审批信息和原始参数一起存储
			return "", tool.StatefulInterrupt(ctx, &commontool.ApprovalInfo{
				ToolName:        tCtx.Name, // 记录被调用的 Tool 名称
				ArgumentsInJSON: args,      // 记录调用参数的 JSON 字符串
			}, args) // 将 args 作为中断状态保存起来，恢复时可以取出
		}

		// 如果之前被中断过，尝试获取恢复上下文（审批结果）
		// isTarget 表示恢复数据是否针对当前中断点，hasData 表示是否有数据，data 是审批结果
		isTarget, hasData, data := tool.GetResumeContext[*commontool.ApprovalResult](ctx)
		if isTarget && hasData {
			// 如果审批通过
			if data.Approved {
				// 使用之前存储的参数继续执行原始的 Tool 端点
				return endpoint(ctx, storedArgs, opts...)
			}
			// 如果审批被驳回，且驳回了具体原因
			if data.DisapproveReason != nil {
				// 返回包含驳回原因的错误信息字符串，但不返回 error
				return fmt.Sprintf("tool '%s' disapproved: %s", tCtx.Name, *data.DisapproveReason), nil
			}
			// 审批被驳回但没有具体原因，返回简单的驳回信息
			return fmt.Sprintf("tool '%s' disapproved", tCtx.Name), nil
		}

		// 如果恢复数据不是针对当前中断点的（可能被其他恢复操作消费了）
		isTarget, _, _ = tool.GetResumeContext[any](ctx)
		if !isTarget {
			// 重新触发中断，将之前存储的参数再次保存，等待正确的审批
			return "", tool.StatefulInterrupt(ctx, &commontool.ApprovalInfo{
				ToolName:        tCtx.Name,
				ArgumentsInJSON: storedArgs, // 使用存储的参数，因为可能在恢复过程中参数发生了变化
			}, storedArgs)
		}

		// 如果恢复上下文是有效的（但不是审批结果类型），直接执行原始端点
		// 这是一种保底逻辑，确保不阻止正常流程
		return endpoint(ctx, storedArgs, opts...)
	}, nil
}

// WrapStreamableToolCall 包装流式 Tool 调用端点（逐步返回执行结果）
// 逻辑与 WrapInvokableToolCall 基本一致，但需要处理流式返回值
func (m *approvalMiddleware) WrapStreamableToolCall(
	_ context.Context,
	endpoint adk.StreamableToolCallEndpoint,
	tCtx *adk.ToolContext,
) (adk.StreamableToolCallEndpoint, error) {
	//tCtx.Name 需要拦截的工具名称 我这里拦截获取多日日期需要用户同意
	if tCtx.Name != "get_forecast" {
		return endpoint, nil
	}
	// 返回包装后的流式端点
	return func(ctx context.Context, args string, opts ...tool.Option) (*schema.StreamReader[string], error) {
		// 🚀 如果该工具已被用户标记为"总是同意"，直接放行
		if m.preApproved[tCtx.Name] {
			return endpoint(ctx, args, opts...)
		}

		// 检查中断状态
		wasInterrupted, _, storedArgs := tool.GetInterruptState[string](ctx)
		// 首次调用，触发审批中断
		if !wasInterrupted {
			// 流式端点返回 nil 的 StreamReader 和中断信号
			return nil, tool.StatefulInterrupt(ctx, &commontool.ApprovalInfo{
				ToolName:        tCtx.Name,
				ArgumentsInJSON: args,
			}, args)
		}

		// 尝试获取审批结果
		isTarget, hasData, data := tool.GetResumeContext[*commontool.ApprovalResult](ctx)
		if isTarget && hasData {
			// 审批通过，使用存储的参数继续执行
			if data.Approved {
				return endpoint(ctx, storedArgs, opts...)
			}
			// 审批驳回，返回包含错误信息的单一数据块流
			if data.DisapproveReason != nil {
				return singleChunkReader(fmt.Sprintf("tool '%s' disapproved: %s", tCtx.Name, *data.DisapproveReason)), nil
			}
			// 审批驳回无原因
			return singleChunkReader(fmt.Sprintf("tool '%s' disapproved", tCtx.Name)), nil
		}

		// 恢复数据不匹配，重新触发中断
		isTarget, _, _ = tool.GetResumeContext[any](ctx)
		if !isTarget {
			return nil, tool.StatefulInterrupt(ctx, &commontool.ApprovalInfo{
				ToolName:        tCtx.Name,
				ArgumentsInJSON: storedArgs,
			}, storedArgs)
		}

		// 保底逻辑，直接执行原始端点
		return endpoint(ctx, storedArgs, opts...)
	}, nil
}

func singleChunkReader(msg string) *schema.StreamReader[string] {
	r, w := schema.Pipe[string](1)
	_ = w.Send(msg, nil)
	w.Close()
	return r
}

// extractApprovalInfo 从中断信息中提取根因 ID 和审批信息
// 参数:
//   - ii: 事件中的中断信息（event.Action.Interrupted）
//
// 返回值:
//   - rootCauseID: 根因中断上下文的 ID，用于 ResumeWithParams.Targets 的 key
//   - approvalInfo: 从中断上下文中提取的审批信息（工具名称、参数等）
func extractApprovalInfo(ii *adk.InterruptInfo) (rootCauseID string, approvalInfo *commontool.ApprovalInfo) {
	for _, ic := range ii.InterruptContexts {
		if ic.IsRootCause {
			rootCauseID = ic.ID
			if info, ok := ic.Info.(*commontool.ApprovalInfo); ok {
				approvalInfo = info
			}
			return
		}
	}
	return
}
