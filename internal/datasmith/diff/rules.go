package diff

import (
	"fmt"
	"path"
	"sort"
	"strings"

	pkgconfig "github.com/jacktea/data-smith/pkg/config"
	"github.com/jacktea/data-smith/pkg/conn"
	pkgdiff "github.com/jacktea/data-smith/pkg/diff"
)

// C6 表选择/排除能力：
//   - 规则表名含 * 或 ? 视为通配模式，展开为命中且具有行身份的基础表；
//   - 规则整体省略（空清单）等价于单一通配 "*"，即整库比对；
//   - 排除表（默认账本表 + 配置排除）对显式与展开规则一律生效，
//     保证迁移账本（schema_migrations 等）不出现在任何比对产物中。

const wildcardChars = "*?"

// isWildcardPattern 判定规则表名是否为通配模式。
func isWildcardPattern(table string) bool {
	return strings.ContainsAny(table, wildcardChars)
}

// hasWildcardRules 报告规则集是否需要展开：空清单（整库语义）或含通配条目。
func hasWildcardRules(rules []pkgconfig.Rule) bool {
	if len(rules) == 0 {
		return true
	}
	for _, rule := range rules {
		if isWildcardPattern(rule.Table) {
			return true
		}
	}
	return false
}

// matchTablePattern 通配匹配表名；大小写不敏感（先按原样匹配，再按小写回退）。
func matchTablePattern(pattern, name string) bool {
	if matched, err := path.Match(pattern, name); err == nil && matched {
		return true
	}
	lowerPattern, lowerName := strings.ToLower(pattern), strings.ToLower(name)
	if lowerPattern == pattern && lowerName == name {
		return false
	}
	matched, err := path.Match(lowerPattern, lowerName)
	return err == nil && matched
}

// validateWildcardRules 校验通配条目只携带表名模式：比对列/业务键/忽略列
// 无法作用于一组表，携带即配置错误。
func validateWildcardRules(rules []pkgconfig.Rule) error {
	for _, rule := range rules {
		if !isWildcardPattern(rule.Table) {
			continue
		}
		if strings.TrimSpace(rule.Table) == "" {
			return fmt.Errorf("wildcard table pattern must not be blank")
		}
		if len(rule.Columns) > 0 || len(rule.ComparisonKey) > 0 || len(rule.IgnoreColumns) > 0 {
			return fmt.Errorf("wildcard rule %q must not set columns, comparisonKey or ignoreColumns", rule.Table)
		}
	}
	return nil
}

// expandRules 把通配/省略规则展开为具体表规则。候选表来自两侧（过滤后）的
// 基础表集合：
//   - 影子模式（结构 forward 已在事务内对齐 source）：候选为 target 全部
//     基础表，行身份以 target 侧为准（对齐后两侧结构一致）；
//   - 直接模式：候选为两侧交集，且两侧均具有行身份；单侧表属结构差异，
//     无行身份表不可比对，均跳过并告警。
//
// 显式（非通配）规则原样保留，排除过滤在之后统一进行。
func expandRules(rules []pkgconfig.Rule, srcTables, tgtTables map[string]*conn.Table, shadow bool, report func(string)) ([]pkgconfig.Rule, []string) {
	log := func(format string, args ...any) {
		if report != nil {
			report(fmt.Sprintf(format, args...))
		}
	}
	expanded := make([]pkgconfig.Rule, 0, len(rules))
	skipped := []string{}
	patterns := make([]string, 0, len(rules))
	explicit := make(map[string]bool)
	for _, rule := range rules {
		if isWildcardPattern(rule.Table) {
			patterns = append(patterns, rule.Table)
			continue
		}
		expanded = append(expanded, rule)
		explicit[rule.Table] = true
	}
	if len(rules) == 0 {
		// 规则整体省略 = 整库语义。
		patterns = append(patterns, "*")
	}
	if len(patterns) == 0 {
		return expanded, skipped
	}
	if len(expanded) == 0 && len(patterns) == 1 && patterns[0] == "*" {
		log("规则省略, 按整库模式展开为全部有行身份的表")
	}

	hasIdentity := func(tbl *conn.Table) bool {
		return len(pkgdiff.RowIdentityColumns(tbl)) > 0
	}
	// 候选按名称排序保证展开结果确定性；通配命中重复表只保留一份。
	candidates := make([]string, 0, len(tgtTables))
	for name := range tgtTables {
		candidates = append(candidates, name)
	}
	sort.Strings(candidates)
	matched := make(map[string]bool)
	for _, name := range candidates {
		if !matchedAny(patterns, name) {
			continue
		}
		// 显式规则优先：通配同时命中的表不重复展开，保留其列配置。
		if explicit[name] {
			matched[name] = true
			continue
		}
		matched[name] = true
		tgtTable := tgtTables[name]
		srcTable := srcTables[name]
		if srcTable == nil {
			if !shadow {
				log("跳过表 %s: 仅 target 侧存在(结构差异已由结构比对覆盖)", name)
				skipped = append(skipped, name)
				continue
			}
			// 影子模式：forward DDL 已在事务内补齐该表（空表），行身份以 target 为准。
			if !hasIdentity(tgtTable) {
				log("跳过表 %s: 无行身份(缺主键与非空唯一索引)", name)
				skipped = append(skipped, name)
				continue
			}
			expanded = append(expanded, pkgconfig.Rule{Table: name})
			continue
		}
		if !hasIdentity(tgtTable) || (!shadow && !hasIdentity(srcTable)) {
			log("跳过表 %s: 无行身份(缺主键与非空唯一索引)", name)
			skipped = append(skipped, name)
			continue
		}
		expanded = append(expanded, pkgconfig.Rule{Table: name})
	}
	// 单侧存在的表（影子模式下 forward DDL 会消除该差异，无需特判）。
	if !shadow {
		for name := range srcTables {
			if matched[name] || !matchedAny(patterns, name) {
				continue
			}
			log("跳过表 %s: 仅 source 侧存在(结构差异已由结构比对覆盖)", name)
			skipped = append(skipped, name)
		}
	}
	sort.Slice(expanded, func(i, j int) bool { return expanded[i].Table < expanded[j].Table })
	sort.Strings(skipped)
	return expanded, skipped
}

func matchedAny(patterns []string, name string) bool {
	for _, pattern := range patterns {
		if matchTablePattern(pattern, name) {
			return true
		}
	}
	return false
}

// resolveDataRules 汇总数据规则的解析顺序：省略/通配展开（整库语义）→
// 排除过滤（默认账本表 + 配置排除）。返回最终规则、被排除的表名。
func resolveDataRules(srcDB, tgtDB conn.DBAdapter, params DataDiffParams, opts dataPhaseOptions, report func(string)) ([]pkgconfig.Rule, []string, error) {
	rules := params.Rules
	if hasWildcardRules(rules) {
		snapshot := opts.schemaSnapshot
		if snapshot == nil {
			srcSchema, err := srcDB.ReadSchema()
			if err != nil {
				return nil, nil, fmt.Errorf("read source schema for rule expansion: %w", err)
			}
			tgtSchema, err := tgtDB.ReadSchema()
			if err != nil {
				return nil, nil, fmt.Errorf("read target schema for rule expansion: %w", err)
			}
			snapshot = &schemaPair{src: baseTables(srcSchema.Tables), tgt: baseTables(tgtSchema.Tables)}
		}
		expanded, skipped := expandRules(rules, snapshot.src, snapshot.tgt, opts.shadow, report)
		rules = expanded
		if len(skipped) > 0 {
			report(fmt.Sprintf("通配展开跳过 %d 张表(单侧存在或无行身份): %s", len(skipped), strings.Join(skipped, ", ")))
		}
		report(fmt.Sprintf("通配展开后共 %d 张表参与数据比对", len(rules)))
	}
	kept, excluded := filterRulesByExcludes(rules, pkgconfig.EffectiveExcludeTables(params.ExcludeTables))
	for _, name := range excluded {
		report(fmt.Sprintf("排除表 %s: 命中排除清单(含默认账本表), 不参与数据比对", name))
	}
	return kept, excluded, nil
}

// filterSequencesByOwnedTable 排除被排除表拥有的 SERIAL 隐式序列：
// Sequence.OwnedBy 形如 "table.column"，表名部分命中排除清单即排除。
func filterSequencesByOwnedTable(sequences map[string]*conn.Sequence, excludes []string) map[string]*conn.Sequence {
	if len(sequences) == 0 || len(excludes) == 0 {
		return sequences
	}
	excSet := make(map[string]bool, len(excludes)*2)
	for _, name := range excludes {
		excSet[name] = true
		excSet[strings.ToLower(name)] = true
	}
	result := make(map[string]*conn.Sequence, len(sequences))
	for name, sequence := range sequences {
		if sequence != nil && sequence.OwnedBy != "" {
			ownedTable := sequence.OwnedBy
			if idx := strings.LastIndex(ownedTable, "."); idx >= 0 {
				ownedTable = ownedTable[:idx]
			}
			if excSet[ownedTable] || excSet[strings.ToLower(ownedTable)] {
				continue
			}
		}
		result[name] = sequence
	}
	return result
}

// filterRulesByExcludes 依据排除清单（含默认账本表）过滤数据规则，返回保留
// 规则与被排除的表名（保持规则顺序）。
func filterRulesByExcludes(rules []pkgconfig.Rule, excludes []string) ([]pkgconfig.Rule, []string) {
	if len(excludes) == 0 {
		return rules, []string{}
	}
	excSet := make(map[string]bool, len(excludes)*2)
	for _, name := range excludes {
		excSet[name] = true
		excSet[strings.ToLower(name)] = true
	}
	kept := make([]pkgconfig.Rule, 0, len(rules))
	excluded := []string{}
	for _, rule := range rules {
		if excSet[rule.Table] || excSet[strings.ToLower(rule.Table)] {
			excluded = append(excluded, rule.Table)
			continue
		}
		kept = append(kept, rule)
	}
	return kept, excluded
}
