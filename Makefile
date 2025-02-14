.PHONY: install-local
install-local:
	cargo build --release --bin dbdaemon
	cargo build --release --bin dbdaemon-api
	sudo install -m755 target/release/dbdaemon /usr/local/bin/
	sudo install -m755 target/release/dbdaemon-api /usr/local/bin/
