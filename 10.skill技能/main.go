package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	localbk "github.com/cloudwego/eino-ext/adk/backend/local"
	clc "github.com/cloudwego/eino-ext/callbacks/cozeloop"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/middlewares/skill"
	"github.com/cloudwego/eino/adk/prebuilt/deep"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/schema"
	"github.com/coze-dev/cozeloop-go"

	"github.com/cloudwego/eino-ext/components/model/deepseek"
)

const cozeLoopWorkspaceId = "7480846041387237385"

// 定义skill目录
const skillsDir = "/Users/zgp/项目/gowork/eino-study/10.skill技能/skills"

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
	backend, _ := localbk.NewBackend(ctx, &localbk.Config{})

	skillBackend, _ := skill.NewBackendFromFilesystem(ctx, &skill.BackendFromFilesystemConfig{
		Backend: backend,
		BaseDir: skillsDir, // = $EINO_EXT_SKILLS_DIR
	})
	//定义读取skill的中间件
	skillMiddleware, _ := skill.NewMiddleware(ctx, &skill.Config{
		Backend: skillBackend,
	})

	agent, err := deep.New(ctx, &deep.Config{
		Name:           "调用工具智能体",
		Description:    "调用工具智能体",
		ChatModel:      chatModel,
		Backend:        backend, // 提供文件系统操作能力
		StreamingShell: backend, // 提供命令执行能力
		Handlers: []adk.ChatModelAgentMiddleware{
			skillMiddleware,
		},
		MaxIteration: 50,
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
