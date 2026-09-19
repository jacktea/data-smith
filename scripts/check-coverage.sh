#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
profile=$(mktemp "${TMPDIR:-/tmp}/datasmith-coverage.XXXXXX")
trap 'rm -f "$profile"' EXIT HUP INT TERM

"$repo_dir/scripts/integration-test.sh" coverage "$profile"

awk '
BEGIN {
	group_count = 5
	group_name[1] = "db"
	group_name[2] = "diff"
	group_name[3] = "sql"
	group_name[4] = "migrate"
	group_name[5] = "exec"
}
NR == 1 { next }
{
	file = $1
	sub(/:.*/, "", file)
	statements = $(NF - 1) + 0
	covered = $NF + 0
	key = $1 " " statements
	if (!(key in seen)) {
		seen[key] = 1
		total["overall"] += statements
	}
	if (covered > 0 && !(key in hit_seen)) {
		hit_seen[key] = 1
		hit["overall"] += statements
	}

	group = ""
	if (file ~ /\/pkg\/db\//) group = "db"
	else if (file ~ /\/(pkg\/diff|internal\/datasmith\/diff)\//) group = "diff"
	else if (file ~ /\/pkg\/sql\//) group = "sql"
	else if (file ~ /\/(pkg\/migrate|internal\/datasmith\/migrate)\//) group = "migrate"
	else if (file ~ /\/internal\/datasmith\/exec\//) group = "exec"

	if (group != "" && !(group SUBSEP key in group_seen)) {
		group_seen[group SUBSEP key] = 1
		total[group] += statements
	}
	if (group != "" && covered > 0 && !(group SUBSEP key in group_hit_seen)) {
		group_hit_seen[group SUBSEP key] = 1
		hit[group] += statements
	}
}
END {
	failed = 0
	overall = total["overall"] == 0 ? 0 : 100 * hit["overall"] / total["overall"]
	printf "overall %.1f%% (%d/%d statements; required >= 60.0%%)\n", overall, hit["overall"], total["overall"]
	if (overall + 0.00001 < 60) failed = 1
	for (i = 1; i <= group_count; i++) {
		name = group_name[i]
		pct = total[name] == 0 ? 0 : 100 * hit[name] / total[name]
		printf "%s %.1f%% (%d/%d statements; required >= 70.0%%)\n", name, pct, hit[name], total[name]
		if (pct + 0.00001 < 70) failed = 1
	}
	exit failed
}
' "$profile"
