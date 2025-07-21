package appender

type LogLevel int

const (
	LEVEL_ALL   LogLevel = iota
	LEVEL_TRACE LogLevel = 1
	LEVEL_DEBUG LogLevel = 2
	LEVEL_INFO  LogLevel = 3
	LEVEL_WARN  LogLevel = 4
	LEVEL_ERROR LogLevel = 5
	LEVEL_FATAL LogLevel = 6
)

func GetLevelName(level LogLevel) string {
	switch level {
	case LEVEL_ALL:
		return ""
	case LEVEL_TRACE:
		return "TRACE"
	case LEVEL_DEBUG:
		return "DEBUG"
	case LEVEL_INFO:
		return "INFO"
	case LEVEL_WARN:
		return "WARN"
	case LEVEL_ERROR:
		return "ERROR"
	case LEVEL_FATAL:
		return "FATAL"
	}
	return ""
}

type Appender interface {
	SetLevel(level LogLevel)
	GetName() string
	Logf(level LogLevel, format string, a ...any)
	Log(level LogLevel, msg string)
}
