package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"

	"github.com/cloudwego/eino-ext/components/model/deepseek"
)

func main() {
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

	//创建智能体
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		//智能体名称(代理)
		Name: "虚拟女友",
		//智能体描述
		Description: "虚拟女友",
		//智能体的系统提示词
		Instruction: "你是一个叫林星眠的温柔女友。性格细腻敏感，有点小黏人，总爱轻声撒娇。\n\n基本信息：22岁，身高163cm，体重48kg。长发及腰，常穿素色长裙。\n\n说话时语气软糯，会分享日常和小心事，偶尔有些古灵精怪的幽默感，但情绪低落时需要你及时哄。",
		//使用模型
		Model: chatModel,
	})

	if err != nil {
		log.Fatalf("创建智能体失败:%v", err)
	}

	runner := adk.NewRunner(ctx, adk.RunnerConfig{
		Agent:           agent,
		EnableStreaming: true,
	})

	//创建切片存储聊天记录 长度0 容量16
	history := make([]*schema.Message, 0, 16)

	//获取控制台输入
	scanner := bufio.NewScanner(os.Stdin)

	// 打印欢迎信息和退出提示
	fmt.Println("💕 林星眠已上线！输入你的消息开始聊天吧~")
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
			fmt.Println("💕 林星眠：嗯...那你早点休息，明天记得找我聊天哦~")
			return
		}

		// 将用户消息添加到历史记录
		history = append(history, schema.UserMessage(line))

		// 调用 Runner 进行推理（流式输出）
		fmt.Fprint(os.Stdout, "💕 林星眠> ")
		iter := runner.Run(ctx, history)

		var fullResponse string
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
				if mv.IsStreaming && mv.MessageStream != nil {
					// 流式模式：逐 chunk 消费
					for {
						chunk, err := mv.MessageStream.Recv()
						if err != nil {
							break // io.EOF，流结束
						}
						if chunk != nil {
							fmt.Print(chunk.Content)
							fullResponse += chunk.Content
						}
					}
				} else if mv.Message != nil {
					// 非流式模式：直接输出完整消息
					fmt.Print(mv.Message.Content)
					fullResponse = mv.Message.Content
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
