# gen_durable — dev shortcuts. Everything runs inside the devcontainer.
DC := docker compose -p gen_durable -f .devcontainer/docker-compose.yml

.PHONY: up test docs

# Build the image and start the app + Postgres containers.
up:
	$(DC) up -d --build

# Run the test suite (fetches deps first).
test:
	$(DC) exec -T app sh -lc "mix deps.get && mix test"

# Generate HTML docs into ./doc.
docs:
	$(DC) exec -T app sh -lc "mix deps.get && mix docs"
