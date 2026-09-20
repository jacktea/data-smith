package diff

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jacktea/data-smith/pkg/conn"
	"github.com/jacktea/data-smith/pkg/consts"
	"github.com/jacktea/data-smith/pkg/db/base"
	"github.com/jacktea/data-smith/pkg/utils"
)

// C5 影子事务两阶段数据比对：
//
//	在 source 连接上 BEGIN，把结构 forward 语句逐条应用在事务内，数据比对
//	经同会话读取对齐后的影子结构，结束后 ROLLBACK。结构零落库、单连接、
//	生成的 up = 结构 forward + 影子状态下的数据差异，一次执行即可对齐。
//
// 仅 PostgreSQL 支持事务性 DDL；MySQL source 回退直接比对（漂移容错语义），
// 由 dataDiffModeAuto 在编排层决定。

// DataDiffMode 控制 diff-full 数据比对的执行方式。
const (
	// DataDiffModeAuto：source 为 PostgreSQL 且存在结构差异时自动启用影子
	// 事务，否则直接比对。
	DataDiffModeAuto = "auto"
	// DataDiffModeShadow 强制影子事务（无结构差异时等价直接比对）。
	DataDiffModeShadow = "shadow"
	// DataDiffModeDirect 禁用影子事务，数据比对直接跑在当前结构上
	// （依赖公共列/主键列集的既有漂移容错）。
	DataDiffModeDirect = "direct"
)

// ValidateDataDiffMode 校验 --data-diff-mode 取值；空串视为默认 auto
// （编程调用可省略该字段）。
func ValidateDataDiffMode(mode string) error {
	switch mode {
	case "", DataDiffModeAuto, DataDiffModeShadow, DataDiffModeDirect:
		return nil
	default:
		return fmt.Errorf("data-diff-mode must be one of %s, %s or %s", DataDiffModeAuto, DataDiffModeShadow, DataDiffModeDirect)
	}
}

// IsValidDataDiffMode 报告 mode 是否为合法的数据比对模式（含空串=默认）。
func IsValidDataDiffMode(mode string) bool {
	return ValidateDataDiffMode(mode) == nil
}

// NormalizeDataDiffMode 把空串归一为引擎默认 auto，供调用方持久化与展示
// 生效模式；其余值原样返回。
func NormalizeDataDiffMode(mode string) string {
	if mode == "" {
		return DataDiffModeAuto
	}
	return mode
}

// decideShadowDataDiff 计算实际是否启用影子事务：显式模式优先；auto 仅在
// source 为 PostgreSQL 且结构 forward 非空时启用。影子事务依赖事务性 DDL，
// 强制 shadow 而 source 非 PostgreSQL 属配置错误——MySQL 的 DDL 会隐式提交，
// 事务内应用会直接污染 source 库。
func decideShadowDataDiff(mode string, sourceType consts.DBType, forwardStatements []string) (bool, error) {
	switch mode {
	case DataDiffModeShadow:
		if sourceType != consts.DBTypePostgres {
			return false, fmt.Errorf("data-diff-mode=shadow requires a PostgreSQL source (MySQL DDL auto-commits and cannot run inside the shadow transaction)")
		}
		return true, nil
	case DataDiffModeDirect:
		return false, nil
	default:
		return sourceType == consts.DBTypePostgres && len(forwardStatements) > 0, nil
	}
}

// sessionBinder 由 base 驱动适配器实现，影子事务借它把行读取收敛到事务会话。
type sessionBinder interface {
	BindSession(session base.Querier)
}

// shadowTx 是 source 连接上的一个未提交事务：持有事务本身并负责语句应用与
// 回滚；会话绑定由调用方通过 SessionBinder 完成。
type shadowTx struct {
	tx *sql.Tx
}

// beginShadowTx 在 source 连接池上开启影子事务（独占一条连接）。
func beginShadowTx(ctx context.Context, srcDB conn.DBAdapter) (*shadowTx, error) {
	if srcDB == nil || srcDB.GetConn() == nil {
		return nil, fmt.Errorf("source database connection is required for the shadow transaction")
	}
	tx, err := srcDB.GetConn().BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin shadow transaction: %w", err)
	}
	return &shadowTx{tx: tx}, nil
}

// apply 在影子事务内逐条执行结构 forward 语句；任一失败即返回带语句预览的
// 错误，由调用方统一回滚。仅应用 forward：影子事务内绝不执行 down 脚本。
func (s *shadowTx) apply(ctx context.Context, statements []string) error {
	for i, statement := range statements {
		if _, err := s.tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply shadow DDL %d/%d %q: %w", i+1, len(statements), statementPreview(statement), err)
		}
	}
	return nil
}

func (s *shadowTx) rollback() error {
	if err := s.tx.Rollback(); err != nil && err != sql.ErrTxDone {
		return fmt.Errorf("rollback shadow transaction: %w", err)
	}
	return nil
}

// finishShadowDataDiff closes the bound read session, explicitly rolls back
// the shadow transaction, and reports recovery only after rollback succeeds.
func finishShadowDataDiff(txs *shadowTx, unbind func(), report func(string)) error {
	if unbind != nil {
		unbind()
	}
	if err := txs.rollback(); err != nil {
		return err
	}
	if report != nil {
		report("回滚影子事务, source 结构恢复原状")
	}
	return nil
}

// statementPreview 返回语句的单行短预览，用于失败定位；实现收敛在公共包
// pkg/utils（exec-sql 失败定位复用同一格式）。
func statementPreview(statement string) string {
	return utils.StatementPreview(statement)
}

// bindSession 把适配器读取通道切换到影子事务会话；适配器不支持会话绑定时
// 返回错误（影子模式下行读取必须与 DDL 同会话才可见）。
func bindSession(srcDB conn.DBAdapter, tx *sql.Tx) (unbind func(), err error) {
	binder, ok := srcDB.(sessionBinder)
	if !ok {
		return nil, fmt.Errorf("source adapter %T does not support shadow session binding", srcDB)
	}
	binder.BindSession(tx)
	return func() { binder.BindSession(nil) }, nil
}
