PRECOMMIT_VERSION="4.2.0"

.PHONY: all check-dependencies install-pre-commit autoupdate run-all-files clean

all: check-dependencies install-pre-commit

check-dependencies:
	@echo "Checking for python3..."
	@command -v python3 >/dev/null 2>&1 || { echo >&2 "Python3 is not installed. Please install Python3 to continue."; exit 1; }
	@echo "Checking for pip3..."
	@command -v pip3 >/dev/null 2>&1 || { echo >&2 "pip3 not found. Please install Python 3 and pip3."; exit 1; }

install-pre-commit:
	@echo "Checking for pre-commit..."
	@pre-commit --version >/dev/null 2>&1 || { echo >&2 "Installing 'pre-commit'..."; pip3 install pre-commit; }
	pre-commit install

autoupdate:
	pre-commit autoupdate

run-all-files:
	pre-commit run --all-files

clean:
	pre-commit uninstall
