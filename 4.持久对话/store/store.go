package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/eino/schema"
)

// SessionInfo 会话概要信息（用于列表展示）
type SessionInfo struct {
	ID        string `json:"id"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
	MsgCount  int    `json:"msg_count"`
	Preview   string `json:"preview"`
}

// Session 表示一个持久化的会话
type Session struct {
	ID        string           `json:"id"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
	Messages  []schema.Message `json:"messages"`

	filePath string // 对应磁盘文件路径，不序列化
	mu       sync.Mutex
}

// Store 管理会话的持久化存储
type Store struct {
	dir string
	mu  sync.RWMutex
}

// NewStore 创建存储实例，dir 相对于当前工作目录
func NewStore(dir string) (*Store, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("解析存储目录失败: %w", err)
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return nil, fmt.Errorf("创建会话目录失败: %w", err)
	}
	return &Store{dir: absDir}, nil
}

// Dir 返回存储目录的绝对路径
func (s *Store) Dir() string {
	return s.dir
}

// NewSession 创建一个新会话（仅内存，不写盘）
func (s *Store) NewSession() *Session {
	now := time.Now()
	id := fmt.Sprintf("session_%s", now.Format("20060102_150405"))
	return &Session{
		ID:        id,
		CreatedAt: now,
		UpdatedAt: now,
		Messages:  make([]schema.Message, 0),
		filePath:  filepath.Join(s.dir, id+".json"),
	}
}

// CreateSession 创建一个新会话并立即写入磁盘
func (s *Store) CreateSession() (*Session, error) {
	session := s.NewSession()
	if err := session.Save(); err != nil {
		return nil, fmt.Errorf("创建会话文件失败: %w", err)
	}
	return session, nil
}

// LoadSession 从磁盘加载指定 ID 的会话
func (s *Store) LoadSession(id string) (*Session, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := filepath.Join(s.dir, id+".json")

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("会话 %s 不存在", id)
		}
		return nil, fmt.Errorf("读取会话文件失败: %w", err)
	}

	var session Session
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, fmt.Errorf("解析会话文件失败: %w", err)
	}

	session.filePath = path

	return &session, nil
}

// DeleteSession 删除指定 ID 的会话
func (s *Store) DeleteSession(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dir, id+".json")
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("会话 %s 不存在", id)
		}
		return fmt.Errorf("删除会话文件失败: %w", err)
	}
	return nil
}

// ListSessions 列出所有历史会话，按更新时间倒序排列
func (s *Store) ListSessions() ([]SessionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("读取会话目录失败: %w", err)
	}

	var infos []SessionInfo

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		id := strings.TrimSuffix(entry.Name(), ".json")
		path := filepath.Join(s.dir, entry.Name())

		data, err := os.ReadFile(path)
		if err != nil {
			continue // 跳过无法读取的文件
		}

		var raw struct {
			ID        string    `json:"id"`
			CreatedAt time.Time `json:"created_at"`
			UpdatedAt time.Time `json:"updated_at"`
			Messages  []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(data, &raw); err != nil {
			continue
		}

		// 提取第一条用户消息作为预览
		preview := ""
		for _, msg := range raw.Messages {
			if msg.Role == "user" {
				preview = truncateText(msg.Content, 40)
				break
			}
		}

		infos = append(infos, SessionInfo{
			ID:        id,
			CreatedAt: raw.CreatedAt.Format("2006-01-02 15:04"),
			UpdatedAt: raw.UpdatedAt.Format("2006-01-02 15:04"),
			MsgCount:  len(raw.Messages),
			Preview:   preview,
		})
	}

	// 按更新时间倒序
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].UpdatedAt > infos[j].UpdatedAt
	})

	return infos, nil
}

// Save 将会话写入磁盘
func (s *Session) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化会话失败: %w", err)
	}

	if err := os.WriteFile(s.filePath, data, 0o644); err != nil {
		return fmt.Errorf("写入会话文件失败: %w", err)
	}

	return nil
}

// AddMessage 追加一条消息到会话并立即写入文件
func (s *Session) AddMessage(msg *schema.Message) error {
	s.mu.Lock()
	s.Messages = append(s.Messages, *msg)
	s.UpdatedAt = time.Now()
	s.mu.Unlock()

	return s.Save()
}

// GetMessages 返回消息列表（转换为指针切片，供 Runner.Run 使用）
func (s *Session) GetMessages() []*schema.Message {
	s.mu.Lock()
	defer s.mu.Unlock()

	ptrs := make([]*schema.Message, len(s.Messages))
	for i := range s.Messages {
		ptrs[i] = &s.Messages[i]
	}
	return ptrs
}

// MessageCount 返回消息数量
func (s *Session) MessageCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Messages)
}

// truncateText 截断字符串到指定长度并添加省略号
func truncateText(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}
