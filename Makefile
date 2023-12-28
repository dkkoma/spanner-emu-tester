.PHONY: build run shell shell-gcloud logs clean down

run:
	docker compose run --rm --remove-orphans app sh -c "go run main.go"
build:
	docker compose build
shell:
	docker compose run --rm -it app ash
shell-gcloud:
	docker compose run --rm -it gcloud-cli bash

clean:
	docker compose down --rmi local --volumes --remove-orphans
down: clean
