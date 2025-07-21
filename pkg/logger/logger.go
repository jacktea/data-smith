package logger

import (
	"github.com/jacktea/data-smith/pkg/logger/appender"
)

var defaultAppender *appender.CompositeAppender = appender.NewCompositeAppender(
	"default",
	appender.NewStdAppender(appender.LEVEL_INFO),
)

func RegisterAppender(appender appender.Appender) {
	defaultAppender.AddAppender(appender)
}

func SetAppenderLevel(name string, level appender.LogLevel) {
	defaultAppender.SetAppenderLevel(name, level)
}

func SetDefaultLevel(level appender.LogLevel) {
	defaultAppender.SetLevel(level)
}

func Tracef(format string, a ...any) {
	defaultAppender.Logf(appender.LEVEL_TRACE, format, a...)
}

func Trace(msg string) {
	defaultAppender.Log(appender.LEVEL_TRACE, msg)
}

func Debugf(format string, a ...any) {
	defaultAppender.Logf(appender.LEVEL_DEBUG, format, a...)
}

func Debug(msg string) {
	defaultAppender.Log(appender.LEVEL_DEBUG, msg)
}

func Infof(format string, a ...any) {
	defaultAppender.Logf(appender.LEVEL_INFO, format, a...)
}

func Info(msg string) {
	defaultAppender.Log(appender.LEVEL_INFO, msg)
}

func Warnf(format string, a ...any) {
	defaultAppender.Logf(appender.LEVEL_WARN, format, a...)
}

func Warn(msg string) {
	defaultAppender.Log(appender.LEVEL_WARN, msg)
}

func Errorf(format string, a ...any) {
	defaultAppender.Logf(appender.LEVEL_ERROR, format, a...)
}

func Error(msg string) {
	defaultAppender.Log(appender.LEVEL_ERROR, msg)
}

func Fatalf(format string, a ...any) {
	defaultAppender.Logf(appender.LEVEL_FATAL, format, a...)
}

func Fatal(msg string) {
	defaultAppender.Log(appender.LEVEL_FATAL, msg)
}
