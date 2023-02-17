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

DESCRIPTION="DB Update"
update_db:
	alembic revision --autogenerate -m $(DESCRIPTION)
	_upgrade_db

revert_db:
	alembic downgrade -1

_upgrade_db:
	alembic upgrade head