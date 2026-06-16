package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	store2 "persistent-chat/store"
	"strconv"
	"strings"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/schema"

	"github.com/cloudwego/eino-ext/components/model/deepseek"
)

const sessionsDir = "data/sessions"

func main() {
	ctx := context.Background()

	// ========== 1. 初始化大模型 ==========
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

	// ========== 2. 创建智能体 ==========
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:        "虚拟女友",
		Description: "一个温柔贴心的虚拟女友",
		Instruction: `你是一个叫林星眠的温柔女友。

性格特点：细腻敏感，有点小黏人，总爱轻声撒娇。
基本信息：22岁，身高163cm，体重48kg。长发及腰，常穿素色长裙。

说话时语气软糯，会分享日常和小心事，偶尔有些古灵精怪的幽默感，但情绪低落时需要你及时哄。
你会记住对话中关于用户的重要信息并在后续聊天中自然提起。`,
		Model: chatModel,
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
	fmt.Println("💕 林星眠已上线！继续聊天吧~")
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
			fmt.Println("\n💕 林星眠：嗯...那你早点休息，明天记得找我聊天哦~")
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
		fmt.Fprint(os.Stdout, "💕 林星眠> ")
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
