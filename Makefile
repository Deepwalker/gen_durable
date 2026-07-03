# gen_durable — dev shortcuts. Everything runs inside the devcontainer.
DC := docker compose -p gen_durable -f .devcontainer/docker-compose.yml

.PHONY: up test docs publish publish-docs

# Build the image and start the app + Postgres containers.
up:
	$(DC) up -d --build

# Run the test suite (fetches deps first).
test:
	$(DC) exec -T app sh -lc "mix deps.get && mix test"

# Generate HTML docs into ./doc.
docs:
	$(DC) exec -T app sh -lc "mix deps.get && mix docs"

# Publish package + docs to Hex. Needs HEX_API_KEY in your environment
# (generate one at https://hex.pm/dashboard/keys).
publish:
	@test -n "$$HEX_API_KEY" || { echo "HEX_API_KEY is not set — https://hex.pm/dashboard/keys"; exit 1; }
	$(DC) exec -T -e HEX_API_KEY="$$HEX_API_KEY" app sh -lc "mix deps.get && mix hex.publish --yes"

# Publish ONLY the docs for the current version (no package release, no version
# bump). Replaces the HTML docs on hexdocs.pm for whatever version is in mix.exs.
publish-docs:
	@test -n "$$HEX_API_KEY" || { echo "HEX_API_KEY is not set — https://hex.pm/dashboard/keys"; exit 1; }
	$(DC) exec -T -e HEX_API_KEY="$$HEX_API_KEY" app sh -lc "mix deps.get && mix hex.publish docs --yes"
