.PHONY: test lint fmt proto

PROTO_SRC := ../thyme/proto
PROTO_FILES := \
	thyme/schema.proto \
	thyme/pycode.proto \
	thyme/expr.proto \
	thyme/connector.proto \
	thyme/dataset.proto \
	thyme/featureset.proto \
	thyme/services.proto \
	thyme/state.proto

proto:
	rm -rf thyme/gen/_tmp
	mkdir -p thyme/gen/_tmp
	uv run python -m grpc_tools.protoc \
		--proto_path=$(PROTO_SRC) \
		--python_out=thyme/gen/_tmp \
		--pyi_out=thyme/gen/_tmp \
		$(PROTO_FILES)
	# protoc nests output under the proto package dir (thyme/); flatten it
	mv thyme/gen/_tmp/thyme/*.py thyme/gen/
	mv thyme/gen/_tmp/thyme/*.pyi thyme/gen/
	rm -rf thyme/gen/_tmp
	# Rewrite sibling imports: `from thyme import X_pb2` → `from thyme.gen import X_pb2`
	sed -i -E 's/^from thyme import ([a-z_]+_pb2) as /from thyme.gen import \1 as /' thyme/gen/*.py
	sed -i -E 's/^from thyme import ([a-z_]+_pb2)$$/from thyme.gen import \1/' thyme/gen/*.pyi

test:
	uv run pytest -v

lint:
	uv run ruff check .

fmt:
	uv run ruff format .
