"""
env_loader.py
─────────────
Loads .env and resolves the active environment's credentials into the flat
variable names that the rest of the codebase uses (e.g. AMO_SUBDOMAIN).

Usage (replace `from dotenv import load_dotenv` + `load_dotenv()` with):

    from env_loader import load_env
    load_env()

Switch between environments by changing ENVIRONMENT in .env:
    ENVIRONMENT=dev   → uses DEV_* variables
    ENVIRONMENT=prod  → uses PROD_* variables
"""

import os
from dotenv import load_dotenv

# Keys whose active-environment value is promoted to the flat name.
# All other keys are shared across environments and stay as-is.
_ENV_SCOPED_KEYS = [
    "AMO_SUBDOMAIN",
    "AMO_CLIENT_ID",
    "AMO_CLIENT_SECRET",
    "AMO_REDIRECT_URI",
    "AMO_AUTH_CODE",
    "AMO_TOKEN_STORE",
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    "GOOGLE_SHEET_ID",
    "GOOGLE_WORKSHEET_NAME",
]


def load_env() -> str:
    """
    Load .env and promote the active environment's prefixed variables to their
    canonical flat names.  Returns the active environment name ('dev' or 'prod').
    """
    load_dotenv()

    env = os.getenv("ENVIRONMENT", "dev").strip().lower()
    prefix = f"{env.upper()}_"

    for key in _ENV_SCOPED_KEYS:
        prefixed_val = os.getenv(f"{prefix}{key}")
        if prefixed_val is not None:
            os.environ[key] = prefixed_val

    return env
