package chunk

// ChunkRange 表示一个分块的主键范围 [MinPK, MaxPK)。nil 边界表示无界，
// 因此首块必须使用 nil MinPK，末块必须使用 nil MaxPK。
type ChunkRange struct {
	ChunkIndex int
	MinPK      any
	MaxPK      any
	IsLast     bool
}

// ChunkStats 是哈希跳过前必须精确匹配的表级不变量。空表的 MinPK/MaxPK
// 都是 nil。
type ChunkStats struct {
	Count int64
	MinPK any
	MaxPK any
}

// ChunkHasher 定义数据库支持分块哈希的接口
type ChunkHasher interface {
	// GetChunkRanges 获取表的分块主键边界列表
	GetChunkRanges(table string, pk string, chunkSize int) ([]ChunkRange, error)
	// GetChunkHash 计算指定分块内指定列的数据哈希校验和
	GetChunkHash(table string, cols []string, pk string, minPK, maxPK any, isLast bool) (string, error)
}

// VerifiedChunkHasher 在 ChunkHasher 之上提供精确统计信息。调用方只有在
// 两端 count/min/max 都一致时，才可以把概率哈希用于跳过逐行扫描。
// 独立接口保留现有 ChunkHasher 实现的源代码兼容性；不支持统计的实现会安全
// 回退到逐行比较。
type VerifiedChunkHasher interface {
	ChunkHasher
	GetChunkStats(table string, pk string) (ChunkStats, error)
}
