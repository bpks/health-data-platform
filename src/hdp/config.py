"""Runtime settings, read from environment variables.

The same code runs on a laptop, in CI, in a Fabric notebook or on Databricks;
only the environment variables change:

    HDP_ENV           local | dev | test | prod        (default: local)
    HDP_BASE_PATH     root of the lake                 (default: data/lake)
                      e.g. abfss://<container>@<account>.dfs.core.windows.net
    HDP_FORMAT        parquet | delta                  (default: parquet)
    HDP_PSEUDO_SALT   secret used to pseudonymise identifiers. In Azure this is
                      read from Key Vault and exported before the job starts.
    HDP_SLA_DAYS      decision target used by the gold measures (default: 60)
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

DEV_ONLY_SALT = "dev-only-salt-never-use-outside-local-development"
TABLE_FORMATS = {"parquet", "delta"}


@dataclass(frozen=True)
class Settings:
    env: str = "local"
    base_path: str = "data/lake"
    table_format: str = "parquet"
    pseudo_salt: str = field(default=DEV_ONLY_SALT, repr=False)  # never print the salt
    sla_days: int = 60

    def __post_init__(self) -> None:
        if self.table_format not in TABLE_FORMATS:
            raise ValueError(f"HDP_FORMAT must be one of {sorted(TABLE_FORMATS)}, got {self.table_format!r}")
        if self.env != "local" and self.pseudo_salt == DEV_ONLY_SALT:
            # Fail fast: a known salt makes pseudonyms of low-entropy identifiers reversible.
            raise ValueError("HDP_PSEUDO_SALT must be set to a secret value when HDP_ENV is not 'local'")

    @classmethod
    def from_env(cls, **overrides: object) -> Settings:
        values: dict[str, object] = {
            "env": os.getenv("HDP_ENV", "local"),
            "base_path": os.getenv("HDP_BASE_PATH", "data/lake"),
            "table_format": os.getenv("HDP_FORMAT", "parquet"),
            "pseudo_salt": os.getenv("HDP_PSEUDO_SALT", DEV_ONLY_SALT),
            "sla_days": int(os.getenv("HDP_SLA_DAYS", "60")),
        }
        values.update({k: v for k, v in overrides.items() if v is not None})
        return cls(**values)  # type: ignore[arg-type]

    @property
    def is_local(self) -> bool:
        return "://" not in self.base_path

    def path(self, *parts: str) -> str:
        return "/".join([self.base_path.rstrip("/"), *parts])

    # Layer helpers keep the folder convention in one place.
    def landing(self) -> str:
        return self.path("landing")

    def bronze(self, entity: str) -> str:
        return self.path("bronze", entity)

    def silver(self, entity: str) -> str:
        return self.path("silver", entity)

    def gold(self, table: str) -> str:
        return self.path("gold", table)

    def control(self, table: str) -> str:
        return self.path("_control", table)

    def quarantine(self) -> str:
        return self.path("quarantine", "records")
