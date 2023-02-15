export
UID="$(shell id -u)"
DOCKER_BUILDKIT=1
BUILDKIT_PROGRESS=plain

.PHONY: check 

check:
	poetry install
	poetry run isort option_bot/
	poetry run black option_bot/
	poetry run flake8 option_bot/

update_db:
	alembic upgrade head

revert_db:
	alembic downgrade