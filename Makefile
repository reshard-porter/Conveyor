TEST_DB ?= postgres://postgres:postgres@localhost:5433/postgres?sslmode=disable

.PHONY: build test test-db test-db-stop lint run up down bench

build:
	CGO_ENABLED=0 go build -o bin/conveyor ./cmd/conveyor

# Starts a throwaway Postgres for the integration tests on port 5433.
test-db:
	docker run -d --rm --name conveyor-test-db -e POSTGRES_PASSWORD=postgres -p 5433:5432 postgres:16-alpine
	@until docker exec conveyor-test-db pg_isready -U postgres >/dev/null 2>&1; do sleep 0.5; done

test-db-stop:
	docker stop conveyor-test-db

# -p 1: packages share one database, so run them one at a time.
test:
	CONVEYOR_TEST_DATABASE_URL="$(TEST_DB)" go test -race -count=1 -p 1 ./...

lint:
	gofmt -l . | (! grep .) && go vet ./...

run:
	DATABASE_URL="$(TEST_DB)" LOG_FORMAT=text go run ./cmd/conveyor serve

bench:
	DATABASE_URL="$(TEST_DB)" go run ./cmd/conveyor bench -jobs 20000 -concurrency 50

up:
	docker compose up --build

down:
	docker compose down -v
