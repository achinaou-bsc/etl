set dotenv-load

default:
  @just --list --unsorted

check-formatting:
  scala fmt --check .

format:
  scala fmt .

run:
  scala run .

clean:
  rm --recursive --force dist
  scala clean .

compile:
  scala compile .

test:


package:
  mkdir --parents dist

  scala --power \
    package --suppress-outdated-dependency-warning --assembly \
      --preamble=false \
      --output etl.jar \
      .

  mv etl.jar dist

local-env ENVIRONMENT:
  cp .env.d/{{ENVIRONMENT}}.env .env

local-env-create:
  docker compose up --detach

local-env-start:
  docker compose start

local-env-stop:
  docker compose stop

local-env-destroy:
  docker compose down --volumes

local-env-reset: local-env-destroy local-env-create
