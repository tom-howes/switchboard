"""Connector registry: provider id -> a ready connector.

A dict of factories rather than an if/elif chain so tests can swap one entry
for a fake, and so adding a provider is one line here plus its module.
"""

from __future__ import annotations

from typing import Callable

from ..config import get_settings
from ..core.connector import Connector
from ..engine.errors import PermanentError
from .stripe import StripeConnector


def _stripe() -> Connector:
    s = get_settings()
    return StripeConnector(api_key=s.stripe_api_key, webhook_secret=s.stripe_webhook_secret)


FACTORIES: dict[str, Callable[[], Connector]] = {
    "stripe": _stripe,
}


def get_connector(provider: str) -> Connector:
    factory = FACTORIES.get(provider)
    if factory is None:
        raise PermanentError(f"no connector for provider {provider!r}")
    return factory()
