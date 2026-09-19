#!/bin/sh
set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose_file="$repo_dir/test/integration/docker-compose.yml"

cleanup() {
	docker compose -f "$compose_file" down --volumes --remove-orphans >/dev/null 2>&1 || true
}

trap cleanup EXIT HUP INT TERM
cleanup
docker compose -f "$compose_file" up --detach --wait --wait-timeout 120

export DATASMITH_INTEGRATION=1
export DATASMITH_FIXTURE_ID=data-smith-integration-v1
export DATASMITH_MYSQL_SOURCE_PORT=33306
export DATASMITH_MYSQL_TARGET_PORT=33307
export DATASMITH_MYSQL_USER=root
export DATASMITH_MYSQL_PASSWORD=datasmith_root_test_password
export DATASMITH_POSTGRES_SOURCE_PORT=35432
export DATASMITH_POSTGRES_TARGET_PORT=35433
export DATASMITH_POSTGRES_USER=datasmith_test
export DATASMITH_POSTGRES_PASSWORD=datasmith_test_password

mode=${1:-test}
case "$mode" in
	test)
		go test -tags=integration -count=1 -timeout=10m ./...
		;;
	coverage)
		if [ "$#" -ne 2 ]; then
			echo "usage: $0 coverage COVER_PROFILE" >&2
			exit 2
		fi
		go test -tags=integration -count=1 -timeout=10m -covermode=atomic -coverpkg=./... -coverprofile="$2" ./...
		;;
	*)
		echo "unknown mode: $mode" >&2
		exit 2
		;;
esac
