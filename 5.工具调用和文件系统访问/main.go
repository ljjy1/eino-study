package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	store2 "mytool/store"

	localbk "github.com/cloudwego/eino-ext/adk/backend/local"
	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/adk/prebuilt/deep"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	"github.com/cloudwego/eino-ext/components/model/deepseek"
)

const (
	sessionsDir = "data/sessions"
	configFile  = "data/config.json"
)

// Config 存储应用配置（持久化到 data/config.json）
type Config struct {
	ProjectRoot string `json:"project_root"`
}

// loadConfig 从 JSON 文件读取配置
func loadConfig() (*Config, error) {
	data, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}
	return &cfg, nil
}

// saveConfig 将配置持久化到 JSON 文件
func saveConfig(cfg *Config) error {
	if err := os.MkdirAll("data", 0o755); err != nil {
		return fmt.Errorf("创建数据目录失败: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}
	if err := os.WriteFile(configFile, data, 0o644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}
	return nil
}

func main() {
	//系统提示词
	var projectRoot string
	//定义命令行参数 传参 --projectRoot "你需要处理的项目目录"
	flag.StringVar(&projectRoot, "projectRoot", "", "项目根目录路径，不传则尝试从配置文件读取（data/config.json）")
	//解析参数
	flag.Parse()

	if projectRoot != "" {
		// 传入了参数 → 保存到配置文件
		cfg := &Config{ProjectRoot: projectRoot}
		if err := saveConfig(cfg); err != nil {
			log.Printf("⚠️  保存配置失败: %v", err)
		} else {
			fmt.Printf("💾 项目目录已保存到配置文件: %s\n", configFile)
		}
	} else {
		// 未传入参数 → 尝试从配置文件读取
		cfg, err := loadConfig()
		if err != nil {
			log.Fatalf("❌ 未找到已保存的项目目录。\n" +
				"   首次使用时请通过 --projectRoot 参数指定项目目录:\n" +
				"     go run main.go --projectRoot /path/to/your/project\n" +
				"   之后可直接运行 (go run main.go)，将自动读取上次保存的配置。")
		}
		projectRoot = cfg.ProjectRoot
		fmt.Printf("📂 从配置文件读取项目目录: %s\n", projectRoot)
	}

	ctx := context.Background()

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
	// 创建 LocalBackend
	backend, err := localbk.NewBackend(ctx, &localbk.Config{})
	/**
	创建 DeepAgent,自动注册文件系统工具
	当配置了 Backend 和 StreamingShell 后,DeepAgent 会自动注册以下工具:
	read_file: 读取文件内容
	write_file: 写入文件内容
	edit_file: 编辑文件内容
	glob: 根据 glob 模式查找文件
	grep: 在文件中搜索内容
	execute: 执行 shell 命令
	*/

	agentInstruction := fmt.Sprintf(`You are a helpful assistant that helps users learn the Eino framework.
		IMPORTANT: When using filesystem tools (ls, read_file, glob, grep, etc.), you MUST use absolute paths.
		The project root directory is: %s

		- When the user asks to list files in "current directory", use path: %s
		- When the user asks to read a file with a relative path, convert it to absolute path by prepending %s
		- Example: if user says "read main.go", you should call read_file with file_path: "%s/main.go"

		Always use absolute paths when calling filesystem tools.

		=== 天气查询工作流 ===
		当用户询问某城市/区域的天气时，请按以下流程执行（所有中间步骤不可见，只输出最终结果）：
		1. 先使用 filesystem 工具（如 read_file 或 grep）读取 xlsx 文件查找用户询问的城市对应的 adcode
		2. 然后用查到的 adcode 调用 amap_weather 工具获取实时天气
		3. 将天气结果整理成一句话回复用户，不要展示查询过程

		⚠️ 重要约束：不要在回答中展示任何中间步骤，如「正在查找城市编码…」「已调用天气 API…」「查询到 adcode 为…」等。直接输出最终天气信息。
		- 如果 xlsx 中没有找到对应城市，直接回复「暂不支持该城市的天气查询」
		- 如果需要列表中的城市名（市辖区/县级市等），用 grep 在 xlsx 中搜索城市关键字`, projectRoot, projectRoot, projectRoot, projectRoot)

	// ========== 创建天气查询工具并注册到智能体 ==========
	weatherTool, err := createQueryWeatherTool()
	if err != nil {
		log.Fatalf("创建天气查询工具失败: %v", err)
	}

	agent, err := deep.New(ctx, &deep.Config{
		Name:           "调用工具智能体",
		Description:    "调用工具智能体",
		ChatModel:      chatModel,
		Instruction:    agentInstruction,
		Backend:        backend, // 提供文件系统操作能力
		StreamingShell: backend, // 提供命令执行能力
		ToolsConfig: adk.ToolsConfig{
			ToolsNodeConfig: compose.ToolsNodeConfig{
				Tools: []tool.BaseTool{weatherTool},
			},
		},
		MaxIteration: 50,
	})

	if err != nil {
		log.Fatalf("创建智能体失败: %v", err)
	}

	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
	})

	// ========== 3. 初始化持久化存储 ==========
	store, err := store2.NewStore(sessionsDir)
	if err != nil {
		log.Fatalf("初始化存储失败: %v", err)
	}
	log.Printf("💾 会话存储目录: %s", store.Dir())

	// ========== 4. 选择或创建会话 ==========
	session := selectOrCreateSession(store)

	// 加载历史消息用于对话
	history := session.GetMessages()

	// ========== 5. 开始对话 ==========
	startConversation(ctx, runner, session, history)

}

// selectOrCreateSession 让用户选择历史会话或创建新会话
func selectOrCreateSession(store *store2.Store) *store2.Session {
	sessions, err := store.ListSessions()
	if err != nil {
		log.Printf("⚠️  读取历史会话失败: %v，将创建新会话", err)
		session, createErr := store.CreateSession()
		if createErr != nil {
			log.Fatalf("创建会话失败: %v", createErr)
		}
		return session
	}

	// 有历史会话时，让用户选择
	for {
		if len(sessions) > 0 {
			fmt.Println()
			fmt.Println("📋 历史会话列表：")
			fmt.Println(strings.Repeat("─", 60))
			for i, s := range sessions {
				preview := s.Preview
				if preview == "" {
					preview = "（空会话）"
				}
				fmt.Printf("  [%d] %s  │ %s  (%d条消息)\n", i+1, s.UpdatedAt, preview, s.MsgCount)
			}
			fmt.Println(strings.Repeat("─", 60))
			fmt.Print("输入编号继续对话，或输入 new 创建新会话，或 delete <编号> 删除: ")
		} else {
			fmt.Println("\n📭 暂无历史会话，将创建新会话。")
			session, err := store.CreateSession()
			if err != nil {
				log.Fatalf("创建会话失败: %v", err)
			}
			return session
		}

		scanner := bufio.NewScanner(os.Stdin)
		if !scanner.Scan() {
			log.Fatalf("读取输入失败")
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "" {
			continue
		}

		// 处理 delete 命令
		if strings.HasPrefix(strings.ToLower(input), "delete ") {
			idxStr := strings.TrimSpace(input[7:])
			idx, err := strconv.Atoi(idxStr)
			if err != nil || idx < 1 || idx > len(sessions) {
				fmt.Println("⚠️  无效的编号，请重试")
				continue
			}
			target := sessions[idx-1]
			if err := store.DeleteSession(target.ID); err != nil {
				fmt.Printf("⚠️  删除失败: %v\n", err)
			} else {
				fmt.Printf("✅ 已删除会话 [%d] %s\n", idx, target.ID)
			}
			// 重新加载列表
			sessions, _ = store.ListSessions()
			continue
		}

		// 处理 new 命令
		if strings.ToLower(input) == "new" {
			session, err := store.CreateSession()
			if err != nil {
				log.Fatalf("创建会话失败: %v", err)
			}
			fmt.Printf("✨ 已创建新会话: %s\n", session.ID)
			return session
		}

		// 处理数字编号 — 选择历史会话
		idx, err := strconv.Atoi(input)
		if err != nil || idx < 1 || idx > len(sessions) {
			fmt.Println("⚠️  请输入有效编号、new 或 delete <编号>")
			continue
		}

		target := sessions[idx-1]
		session, err := store.LoadSession(target.ID)
		if err != nil {
			fmt.Printf("⚠️  加载会话失败: %v，请重试\n", err)
			continue
		}
		fmt.Printf("📂 已恢复会话 [%d] %s（%d条消息）\n", idx, session.ID, session.MessageCount())
		return session
	}
}

// startConversation 启动对话主循环
func startConversation(ctx context.Context, runner *adk.Runner, session *store2.Session, history []*schema.Message) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println()
	fmt.Println(strings.Repeat("━", 50))
	fmt.Println("（输入 exit / quit / 再见 / 退出 结束对话，输入 save 手动保存）")
	fmt.Println(strings.Repeat("━", 50))

	for {
		fmt.Fprint(os.Stdout, "\n你> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// 检查退出条件
		switch strings.ToLower(line) {
		case "exit", "quit", "再见", "退出", "bye":
			fmt.Printf("💾 对话已保存至: data/sessions/%s.json\n", session.ID)
			return
		case "save":
			if err := session.Save(); err != nil {
				fmt.Printf("⚠️  保存失败: %v\n", err)
			} else {
				fmt.Printf("✅ 对话已保存（%d条消息）\n", session.MessageCount())
			}
			continue
		}

		// 加入用户消息到会话
		userMsg := schema.UserMessage(line)
		_ = session.AddMessage(userMsg)
		history = append(history, userMsg)

		// 调用 Runner 流式推理
		fmt.Fprint(os.Stdout, "智能体> ")
		iter := runner.Run(ctx, history)

		var fullResponse string
		for {
			event, ok := iter.Next()
			if !ok {
				break
			}
			if event.Err != nil {
				log.Printf("推理出错: %v", event.Err)
				break
			}
			if event.Output != nil && event.Output.MessageOutput != nil {
				mv := event.Output.MessageOutput
				if mv.IsStreaming && mv.MessageStream != nil {
					for {
						chunk, recvErr := mv.MessageStream.Recv()
						if recvErr != nil {
							break // io.EOF，流结束
						}
						if chunk != nil {
							fmt.Print(chunk.Content)
							fullResponse += chunk.Content
						}
					}
				} else if mv.Message != nil {
					fmt.Print(mv.Message.Content)
					fullResponse = mv.Message.Content
				}
			}
		}
		fmt.Println()

		// 将 AI 回复加入持久化存储和历史记录
		if fullResponse != "" {
			aiMsg := schema.AssistantMessage(fullResponse, nil)
			_ = session.AddMessage(aiMsg)
			history = append(history, aiMsg)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("读取输入出错: %v", err)
	}
}

/**
* 创建工具的方式有两种
  1.直接实现InvokableTool接口或者StreamableTool接口(流式)
  2.使用把本地函数转为 tool  NewTool 方法或者InferTool 方法
* 官方文档: https://www.cloudwego.io/zh/docs/eino/core_modules/components/tools_node_guide/how_to_create_a_tool/
*  这里使用InferTool 方法实现 (需要将约束写入结构体)
*/

// 配置天气查询结构体
type WeatherQuery struct {
	//jsonschema就是配置 tool工具参数的入参约束  替代schema.ParameterInfo配置
	CityCode string `json:"citycode" jsonschema:"required,description=需要查询的城市编码"`
}

// 配置高德天气查询函数
func QueryWeather(ctx context.Context, query *WeatherQuery) (string, error) {
	//读取高德key环境变量
	key := os.Getenv("GAODE_API_KEY")
	if key == "" {
		return "", fmt.Errorf("请设置环境变量 GAODE_API_KEY")
	}
	//调用高德API进行查询
	url := fmt.Sprintf("https://restapi.amap.com/v3/weather/weatherInfo?city=%s&key=%s", query.CityCode, key)

	resp, err := http.Get(url)

	if err != nil {
		return "", fmt.Errorf("调用高德 API 失败: %w", err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Printf("关闭响应体失败: %v", err)
		}
	}(resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("读取响应体失败: %w", err)
	}

	var result map[string]interface{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return "", fmt.Errorf("解析响应体失败: %w", err)
	}
	//获取接口返回状态  0失败1成功
	status := result["status"]
	if status != "1" {
		return "", fmt.Errorf("查询失败: %s", result["info"])
	}

	//获取天气信息（带安全类型断言，防止 panic）
	livesRaw, ok := result["lives"].([]interface{})
	if !ok || len(livesRaw) == 0 {
		return "", fmt.Errorf("响应中缺少天气数据")
	}
	live, ok := livesRaw[0].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("天气数据格式异常")
	}
	return fmt.Sprintf(
		"当前%s%s天气是%s,温度是%s°C,风向是%s,风力是%s级,相对湿度是%s%%",
		safeStr(live, "province"),
		safeStr(live, "city"),
		safeStr(live, "weather"),
		safeStr(live, "temperature"),
		safeStr(live, "winddirection"),
		safeStr(live, "windpower"),
		safeStr(live, "humidity"),
	), nil
}

// safeStr 安全地从 map[string]interface{} 中读取字符串字段
func safeStr(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return "未知"
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// 将天气查询封装为工具
func createQueryWeatherTool() (tool.InvokableTool, error) {
	return utils.InferTool[*WeatherQuery, string]("amap_weather",
		fmt.Sprintf("实时天气查询工具。调用前需要先查询 /Users/zgp/项目/gowork/eino-study/5.工具调用和文件系统访问/data/AMap_adcode_citycode.xlsx 获取用户询问城市的 adcode。返回内容包括省份、城市、天气状况、温度、风向、风力、湿度等。"),
		QueryWeather,
	)
}
