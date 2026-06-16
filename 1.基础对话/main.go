package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"os"
	"strings"

	"github.com/cloudwego/eino-ext/components/model/deepseek"
	"github.com/cloudwego/eino/schema"
)

func main() {
	//系统提示词
	var instruction string
	//定义命令行参数 传参 --instruction "你的系统提示词"  当前默认 你是个天气查询助手
	flag.StringVar(&instruction, "instruction", "你是个天气查询助手,直接回答用户询问的当前天气", "")
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

	//一次性调用模型生成回复
	generate(instruction, query)
	//开启协程 流式输出
	stream(instruction, query)
}

/**
 * Generate 一次性输出：将指令和问题发送给 DeepSeek 模型，打印完整回复
 * instruction 系统提示词
 * query 用户问题
 */
func generate(instruction string, query string) {
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

	// 创建系统消息和用户消息
	systemMessage := schema.SystemMessage(instruction)
	userMessage := schema.UserMessage(query)

	messages := []*schema.Message{systemMessage, userMessage}

	// 一次性生成回复
	result, err := chatModel.Generate(ctx, messages)
	if err != nil {
		log.Fatalf("生成回复失败:%v", err)
	}
	log.Printf("AI回复: %s", result.Content)
}

/**
 * Stream 流式输出：将指令和问题发送给 DeepSeek 模型，逐字打印回复
 * instruction 系统提示词
 * query 用户问题
 */
func stream(instruction string, query string) {
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
	//创建系统消息
	systemMessage := schema.SystemMessage(instruction)
	//创建用户消息
	userMessage := schema.UserMessage(query)

	messages := []*schema.Message{systemMessage, userMessage}
	//流式输出
	outStream, err := chatModel.Stream(ctx, messages)
	if err != nil {
		log.Fatalf("生成回复失败:%v", err)
	}
	//需要关闭流
	defer outStream.Close()

	//收集所有
	var full string

	log.Println("AI回复:")
	for {
		//读取流
		frame, err := outStream.Recv()
		//判断是结束
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("生成回复失败:%v", err)
		}
		//打印输出
		log.Print(frame.Content)
		full += frame.Content
	}
	log.Println()
	log.Printf("总回复:%s", full)
}
