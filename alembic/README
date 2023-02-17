# Alembic

## Generic single-database configuration with an async dbapi

Alembic can be used to take your current DB state and compare it with your schema files. Alembic
then generates a delta to get your DB from its current state to the desired state based on the
schema files.

The primary way to interact with alembic is via `Make` commands defined in the `Makefile`. Below is the main command for updates.

```make update_db DESCRIPTION="<input your description>"```

This will generate a script based on the schema - db diff. And will then apply those changes to the db. The make commands are built using the below commands.

This creates the revision script:

`alembic revision --autogenerate -m "<description>"`

This will run all the db migrations. THIS WILL NOT LOAD ANY DATA. It just changes the table schemas.

`alembic upgrade head`

If you want to go back to a previous db schema state run this.

`alembic downgrade -1`
