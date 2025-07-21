package consts

type DBType string

const (
	DBTypeMySQL    DBType = "mysql"
	DBTypePostgres DBType = "postgres"
	DBTypeUnknown  DBType = "unknown"
)
