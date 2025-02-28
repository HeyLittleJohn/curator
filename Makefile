export
UID="$(shell id -u)"
DOCKER_BUILDKIT=1
BUILDKIT_PROGRESS=plain

DESCRIPTION="DB Update"
update_db:
	alembic revision --autogenerate -m $(DESCRIPTION)
	make _upgrade_db

revert_db:
	alembic downgrade -1

_upgrade_db:
	alembic upgrade head