# Notebook wrapper: run the pipeline from a Microsoft Fabric or Databricks notebook.
#
# STATUS: a template, not run against a real Fabric or Databricks workspace. The pipeline itself is
# tested locally (see tests/); this file only shows how the same code is started on those platforms.
#
# Setup
#   1. Build the wheel (`python -m build --wheel`) and attach it to the notebook environment
#      (Fabric: Environment -> Custom libraries; Databricks: cluster library or the asset bundle).
#   2. Put the pseudonymisation salt in Key Vault as the secret `hdp-pseudo-salt`.
#   3. Set the two values in the first cell.
#
# Nothing in the pipeline hard-codes a path or a secret: it reads HDP_* environment variables.

# %% Cell 1: configuration
import os

LAKE_BASE_PATH = "abfss://lake@<storage-account>.dfs.core.windows.net"  # Terraform output: lake_base_path
KEY_VAULT_URI = "https://<key-vault>.vault.azure.net/"  # Terraform output: key_vault_uri

os.environ["HDP_ENV"] = "dev"  # dev | test | prod (anything but "local" requires a real salt)
os.environ["HDP_BASE_PATH"] = LAKE_BASE_PATH
os.environ["HDP_FORMAT"] = "delta"  # Delta is native on both platforms

# %% Cell 2: read the secret. Pick the line for your platform; never print the value.
try:  # Microsoft Fabric
    os.environ["HDP_PSEUDO_SALT"] = notebookutils.credentials.getSecret(KEY_VAULT_URI, "hdp-pseudo-salt")  # noqa: F821
except NameError:  # Databricks (secret scope backed by Key Vault)
    os.environ["HDP_PSEUDO_SALT"] = dbutils.secrets.get(scope="hdp", key="pseudo-salt")  # noqa: F821

# %% Cell 3: run
from hdp.config import Settings
from hdp.run import run_pipeline, summarise
from hdp.spark_session import get_spark

settings = Settings.from_env()
spark = get_spark(settings)  # reuses the notebook's existing session
ctx = run_pipeline(spark, settings, stage="all")
print(summarise(ctx))

# %% Cell 4: fail the notebook (and so the pipeline run and the alert) if any stage failed
failed = [r for r in ctx.records if r["status"] == "failed"]
if failed:
    raise RuntimeError(f"{len(failed)} stage(s) failed: " + ", ".join(f"{r['stage']}/{r['entity']}" for r in failed))
