package appender

import "sync"

type CompositeAppender struct {
	mu        sync.Mutex
	name      string
	appenders map[string]Appender
}

func (c *CompositeAppender) Logf(level LogLevel, format string, a ...any) {
	for _, appender := range c.appenders {
		appender.Logf(level, format, a...)
	}
}

func (c *CompositeAppender) Log(level LogLevel, msg string) {
	for _, appender := range c.appenders {
		appender.Log(level, msg)
	}
}

func (c *CompositeAppender) SetLevel(level LogLevel) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, appender := range c.appenders {
		appender.SetLevel(level)
	}
}

func (c *CompositeAppender) GetName() string {
	return c.name
}

func NewCompositeAppender(name string, appenders ...Appender) *CompositeAppender {
	appendersMap := make(map[string]Appender)
	for _, appender := range appenders {
		appendersMap[appender.GetName()] = appender
	}
	return &CompositeAppender{appenders: appendersMap}
}

func (c *CompositeAppender) GetAppender(name string) Appender {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.appenders[name]
}

func (c *CompositeAppender) SetAppenderLevel(name string, level LogLevel) {
	appender := c.GetAppender(name)
	if appender != nil {
		appender.SetLevel(level)
	}
}

func (c *CompositeAppender) AddAppender(appender Appender) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.appenders[appender.GetName()] = appender
}

func (c *CompositeAppender) RemoveAppender(appender Appender) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.appenders, appender.GetName())
}
