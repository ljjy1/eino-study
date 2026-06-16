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
)

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

	//创建日志回调
	logHandler := createLogCallbackHandler()
	// 注册为全局 Callback
	callbacks.AppendGlobalHandlers(logHandler)

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

	////封装动态工具的中间件
	//toolSearchMiddleware, err := toolsearch.New(ctx, &toolsearch.Config{
	//	DynamicTools:       allTools,
	//	UseModelToolSearch: false, //是否将调用工具能力交给模型
	//})
	//if err != nil {
	//	log.Fatalf("创建动态工具中间件失败: %v", err)
	//}

	// ========== 2. 创建智能体 ==========
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:        "tool_search_agent",
		Description: "An agent that can dynamically search and use tools from a large tool library",
		Instruction: `You are a helpful assistant.`,
		Model:       chatModel,
		//Handlers: []adk.ChatModelAgentMiddleware{
		//	toolSearchMiddleware,
		//},
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: allTools,
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

/**
 * 自定义一个日志回调处理
 */
func createLogCallbackHandler() callbacks.Handler {
	handler := callbacks.NewHandlerBuilder().
		OnStartFn(func(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) context.Context {
			log.Printf("[log trace] %s/%s start", info.Component, info.Name)
			return ctx
		}).
		OnEndFn(func(ctx context.Context, info *callbacks.RunInfo, output callbacks.CallbackOutput) context.Context {
			log.Printf("[log trace] %s/%s end", info.Component, info.Name)
			return ctx
		}).
		OnErrorFn(func(ctx context.Context, info *callbacks.RunInfo, err error) context.Context {
			log.Printf("[log trace] %s/%s error: %v", info.Component, info.Name, err)
			return ctx
		}).Build()

	return handler
}
