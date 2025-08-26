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

env ENVIRONMENT:
  cp .env.d/{{ENVIRONMENT}}.env .env
