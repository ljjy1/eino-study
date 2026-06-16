package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"

	"github.com/cloudwego/eino-ext/components/model/deepseek"
	"github.com/cloudwego/eino/components/prompt"
	"github.com/cloudwego/eino/schema"
)

func main() {
	//解析参数
	flag.Parse()
	// 获取并合并所有非标志参数为一个字符串，去除首尾空白
	// 传参方式1  go run main.go 今天天气怎么样  问题中间可以有换行空格等等
	// 方式2 go run main.go -- "今天天气怎么样"
	//好处不需要指定参数
	query := strings.TrimSpace(strings.Join(flag.Args(), " "))
	//如果参数是空的
	if query == "" {
		//提示用户输入问题
		log.Fatalf("请输入你的问题,方式:%v", "go run main.go \"你的问题\"")
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
		log.Fatalf("创建模型失败:%v", err)
	}
	//使用 FString 模板
	//messages := createFStringTemplateMessage(ctx, query)

	//使用 GoTemplate 模板
	//messages := createGoTemplateSimple(ctx, query)

	//使用 Jinja2 模板
	messages := createJinja2TemplateMessage(ctx, query)

	outMsg, _ := chatModel.Generate(ctx, messages)
	log.Printf("AI回复：%s", outMsg.Content)
}

/**
 * 创建模板，使用 FString
 */
func createFStringTemplateMessage(ctx context.Context, query string) []*schema.Message {
	// 创建模板，使用 FString 格式
	template := prompt.FromMessages(
		//格式
		schema.FString,
		// 系统消息模板
		schema.SystemMessage("你是一个{role}。你需要用{style}的语气回答问题。你的目标是帮助程序员保持积极乐观的心态，提供技术建议的同时也要关注他们的心理健康。"),
		// 用户消息模板
		schema.UserMessage("问题: {question}"),
	)
	//配置变量
	varibales := map[string]any{
		"role":     "程序员鼓励师",
		"style":    "积极、温暖且专业",
		"question": query,
	}
	//进行替换格式化变量
	messages, err := template.Format(ctx, varibales)
	if err != nil {
		log.Fatalf("模板格式化错误: %v", err)
	}
	return messages
}

/**
 * createGoTemplateMessage 使用 GoTemplate 格式创建提示词模板
 *
 * GoTemplate 使用 Go 标准库 text/template 的语法（{{.Field}}），相比 FString（{field}）的优势：
 *
 *   【FString 优势】
 *   1. 语法简洁，模板更易读
 *   2. 适合简单的变量替换场景
 *   3. 性能略高（纯字符串替换，无模板解析开销）
 *
 *   【GoTemplate 优势】（本函数展示的能力）
 *   1. 条件控制 — 根据变量真假动态决定渲染哪些内容（{{if .hasCode}}...{{end}}）
 *   2. 循环遍历 — 对数组/切片进行迭代输出（{{range .examples}}...{{end}}）
 *   3. 变量作用域 — 在 range 内部使用 {{.}} 访问当前元素
 *   4. 管道函数 — 支持链式调用内置函数（{{.name | printf "%.2f"}}）
 *   5. 嵌套结构体 — 支持访问深层字段（{{.user.profile.name}}）
 *   6. 复用性 — 同一模板可根据变量配置输出截然不同的内容
 *
 *   一句话：FString = 简单替换，GoTemplate = 模板编程。
 */
func createGoTemplateMessage(ctx context.Context, query string) []*schema.Message {
	// 创建模板，使用 GoTemplate 格式
	// 注意语法区别：FString 用 {var}，GoTemplate 用 {{.var}}
	template := prompt.FromMessages(
		// 使用 GoTemplate 格式
		schema.GoTemplate,

		// ===== 系统消息：展示 GoTemplate 的条件与循环能力 =====
		schema.SystemMessage(`你是{{.role}}。
你的语气风格是：{{.style}}。
{{if .personality}}你的性格特点是：{{.personality}}。{{end}}
{{if .hasCode}}你擅长以下编程语言：
{{range $i, $lang := .languages}}{{$i | add1}}. {{$lang}}
{{end}}{{end}}
{{if .responseLengthLimit}}你的回复请控制在{{.responseLengthLimit}}字以内。{{end}}
`),

		// ===== 用户消息：展示嵌套结构体访问 =====
		schema.UserMessage(`用户的问题：{{.question}}
{{if .context.thread}}对话上下文：
  - 之前的对话轮次：{{.context.thread.turnCount}}
  - 对话主题：{{.context.thread.topic}}
{{end}}
{{if .context.info.name}}用户信息：{{.context.info.name}}{{end}}
`),
	)

	// 构造变量数据，展示 GoTemplate 的各种能力
	variables := map[string]any{
		"role":        "程序员鼓励师",
		"style":       "积极、温暖且专业",
		"personality": "善于用比喻和生动的例子来解释复杂概念",
		// 条件判断：hasCode = true 时会渲染 languages 列表
		"hasCode":             len(query) > 0,
		"languages":           []string{"Go", "Python", "JavaScript", "Rust"},
		"responseLengthLimit": 500,
		"question":            query,
		// 嵌套结构体演示
		"context": map[string]any{
			"thread": map[string]any{
				"turnCount": 3,
				"topic":     "技术问题解答",
			},
			"info": map[string]any{
				"name": "深度求索用户",
			},
		},
	}

	// 执行模板渲染
	messages, err := template.Format(ctx, variables)
	if err != nil {
		log.Fatalf("GoTemplate 格式化错误: %v", err)
	}
	return messages
}

/**
 * createGoTemplateSimple 使用 GoTemplate 的简单版本 — 与 FString 功能等价的替换
 * 展示两者最直接的语法区别：{{.var}} vs {var}
 */
func createGoTemplateSimple(ctx context.Context, query string) []*schema.Message {
	// 使用 GoTemplate 格式
	template := prompt.FromMessages(
		schema.GoTemplate,
		schema.SystemMessage("你是一个{{.role}}。你需要用{{.style}}的语气回答问题。"),
		schema.UserMessage("问题: {{.question}}"),
	)

	variables := map[string]any{
		"role":     "程序员鼓励师",
		"style":    "积极、温暖且专业",
		"question": query,
	}

	messages, err := template.Format(ctx, variables)
	if err != nil {
		log.Fatalf("GoTemplate 格式化错误: %v", err)
	}
	return messages
}

/**
 * createJinja2TemplateMessage 使用 Jinja2 格式创建提示词模板
 *
 * Eino 底层使用 gonja（Go 实现的 Jinja2 兼容引擎）渲染，语法与 Python Jinja2 一致。
 *
 * ════════════════════════════════════════════════════════════════
 * Jinja2 vs GoTemplate 对比
 * ════════════════════════════════════════════════════════════════
 *
 * 【Jinja2 优势】
 *   1. 丰富的内置过滤器（filter）— chain 调用非常直观：
 *        {{ name | upper | truncate(10) }}
 *      GoTemplate 需要自定义函数或手动处理
 *   2. 循环特殊变量 — {{ loop.index }} / {{ loop.first }} / {{ loop.last }}
 *      GoTemplate 只能通过 $i 索引手动判断
 *   3. 默认值过滤器 — {{ var | default("fallback") }}
 *      GoTemplate 需要 {{if .var}}{{.var}}{{else}}fallback{{end}}
 *   4. 模板内数学运算 — {{ count + 1 }} / {{ items | length }}
 *      GoTemplate 需要自定义函数
 *   5. 字符串拼接 — 用 ~ 运算符：{{ first ~ " " ~ last }}
 *      GoTemplate 用 {{printf "%s %s" .first .last}}
 *   6. Python/Jinja2 生态用户零学习成本
 *   7. 模板可在 Python 和 Go 服务间共享
 *
 * 【GoTemplate 优势】
 *   1. 零外部依赖 — 来自 Go 标准库 text/template
 *   2. 性能更优 — 原生 Go 编译型模板引擎，无反射开销
 *   3. 类型安全 — 模板中的方法调用经过编译检查
 *   4. 自定义函数（funcmap）机制成熟灵活
 *   5. Go 开发者更熟悉其语法
 *
 * 【什么时候用 Jinja2】
 *   - 团队有 Python/Jinja2/Ansible 背景
 *   - 模板需要在 Go 和 Python 服务间共享
 *   - 需要丰富的字符串/列表内置过滤器
 *   - 偏好更加"表达式友好"的模板语法
 *   - 模板中有大量循环遍历，且依赖 loop 上下文信息
 *
 * 【什么时候用 GoTemplate】
 *   - 追求最少的外部依赖
 *   - 模板逻辑相对简单（主要是变量替换）
 *   - 性能敏感场景（高频调用模板渲染）
 *   - 深度 Go 生态，团队全员 Go 背景
 *   - 需要自定义模板函数实现复杂业务逻辑
 *
 * 【一句话总结】
 *   FString = 简单替换，GoTemplate = Go 原生模板编程，Jinja2 = 跨语言模板标准。
 */
func createJinja2TemplateMessage(ctx context.Context, query string) []*schema.Message {
	// 创建模板，使用 Jinja2 格式
	template := prompt.FromMessages(
		// 使用 Jinja2 格式
		schema.Jinja2,

		// ===== 系统消息：展示 Jinja2 的条件、过滤器和循环能力 =====
		schema.SystemMessage(`你是 {{ role }}。
你的语气风格是：{{ style | upper }}。
{%- if personality %}
你的性格特点是：{{ personality }}。
{%- endif %}
{%- if has_code %}
你擅长以下编程语言：
{%- for lang in languages %}
{{ loop.index }}. {{ lang | title }}{% if loop.last %}（以上共 {{ languages | length }} 门语言）{% endif %}
{%- endfor %}
{%- endif %}
{%- if response_length_limit %}
你的回复请控制在 {{ response_length_limit }} 字以内。
{%- endif %}
`),

		// ===== 用户消息：展示默认值过滤器和 ~ 拼接 =====
		schema.UserMessage(`用户的问题：{{ question | default("（未提供具体问题）") }}
{%- if context.thread %}
对话上下文：
  - 之前的对话轮次：{{ context.thread.turn_count }}
  - 对话主题：{{ context.thread.topic | title }}
  - 当前时间：{{ context.current_time }}
{%- endif %}
{%- if context.info %}
用户信息：{{ context.info.name ~ "（" ~ context.info.role ~ "）" }}
{%- endif %}
`),
	)

	// 构造变量数据，展示 Jinja2 的各种能力
	variables := map[string]any{
		"role":        "程序员鼓励师",
		"style":       "积极、温暖且专业",
		"personality": "善于用比喻和生动的例子来解释复杂概念",
		// 条件判断：has_code = true 时会渲染 languages 列表
		"has_code":              len(query) > 0,
		"languages":             []string{"go", "python", "javaScript", "rust"},
		"response_length_limit": 500,
		"question":              query,
		"context": map[string]any{
			"thread": map[string]any{
				"turn_count": 3,
				"topic":      "技术问题解答",
			},
			"current_time": "2025-01-20 14:30:00",
			// 展示 default 过滤器：如果 info 为 nil，模板中会显示默认值
			"info": map[string]any{
				"name": "深度求索用户",
				"role": "全栈工程师",
			},
		},
	}

	// 执行模板渲染
	messages, err := template.Format(ctx, variables)
	if err != nil {
		log.Fatalf("Jinja2 格式化错误: %v", err)
	}
	return messages
}
