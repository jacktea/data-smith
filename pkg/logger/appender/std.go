package appender

import "log"

type StdAppender struct {
	level LogLevel
}

func (s *StdAppender) SetLevel(level LogLevel) {
	s.level = level
}

func (s *StdAppender) GetName() string {
	return "std"
}

func (s *StdAppender) Logf(level LogLevel, format string, a ...any) {
	if level >= s.level {
		log.Printf("[%s] "+format, append([]any{GetLevelName(level)}, a...)...)
	}
}

func (s *StdAppender) Log(level LogLevel, msg string) {
	if level >= s.level {
		log.Printf("[%s] %s\n", GetLevelName(level), msg)
	}
}

func NewStdAppender(level LogLevel) *StdAppender {
	return &StdAppender{level: level}
}
