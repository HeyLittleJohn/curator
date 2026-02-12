---
description: Detects changes in database schema/models, generates an Alembic migration with a descriptive name
---

# Database Migration Skill

## Instructions

1.  **Analyze Changes**: Review the recent changes made to the SQLAlchemy models or schema definitions in the codebase.
2.  **Generate Name**: Create a concise, snake_case migration message (slug) that summarizes the changes (e.g., `add_user_profile_table`, `add_index_to_orders`, `remove_legacy_columns`).
3.  **Create Revision**: Run the following command in the terminal, replacing `<message>` with the generated name:
    ```bash
    uv run alembic revision --autogenerate -m "<message>"
    ```
4.  **Apply Upgrade**: Once the revision is created successfully, run the following command to apply the changes:
    ```bash
    uv run alembic upgrade head
    ```