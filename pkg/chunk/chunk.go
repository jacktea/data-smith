package chunk

// ChunkRange 表示一个分块的主键范围 [MinPK, MaxPK)
// 当 IsLast 为 true 时，范围通常为 [MinPK, +∞) 即主键 >= MinPK
type ChunkRange struct {
	ChunkIndex int
	MinPK      any
	MaxPK      any
	IsLast     bool
}

// ChunkHasher 定义数据库支持分块哈希的接口
type ChunkHasher interface {
	// GetChunkRanges 获取表的分块主键边界列表
	GetChunkRanges(table string, pk string, chunkSize int) ([]ChunkRange, error)
	// GetChunkHash 计算指定分块内指定列的数据哈希校验和
	GetChunkHash(table string, cols []string, pk string, minPK, maxPK any, isLast bool) (string, error)
}
