from sqlalchemy import JSON
from sqlalchemy.dialects.postgresql import JSONB

# JSONB в PostgreSQL, обычный JSON в других БД (SQLite в тестах)
JSONType = JSON().with_variant(JSONB(), "postgresql")
