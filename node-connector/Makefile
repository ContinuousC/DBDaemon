.PHONY: publish

db_connector.js: $(shell find ../Rust/dbdaemon-api -name '*.rs')
	cd ../Rust && cargo run --bin dbdaemon-api ../Node/db_connector.tmpl.js

publish:
	npm publish
