pre-commit-run-all:
	GOARCH=amd64 CGO_ENABLED=1 pre-commit run --all-files

install-go-deps:
	echo "no deps"

run-e2e-tests:
	echo "no e2e tests!"
