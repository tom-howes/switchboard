"""Connector registry: provider id -> a ready connector.

A dict of factories rather than an if/elif chain so tests can swap one entry
for a fake, and so adding a coded provider is one line here plus its module.

Anything not in FACTORIES is looked up in providers/*.json, the generic REST
connector's config files. A provider file may not reuse a coded connector's
id: the clash is an error at load time rather than one silently shadowing
the other.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Callable

from ..config import get_settings
from ..core.connector import Connector
from ..engine.errors import PermanentError
from .generic import GenericConfig, GenericRestConnector, load_configs
from .hubspot import HubSpotConnector
from .stripe import StripeConnector


def _stripe() -> Connector:
    s = get_settings()
    return StripeConnector(api_key=s.stripe_api_key, webhook_secret=s.stripe_webhook_secret)


def _hubspot() -> Connector:
    s = get_settings()
    return HubSpotConnector(client_id=s.hubspot_client_id, client_secret=s.hubspot_client_secret)


FACTORIES: dict[str, Callable[[], Connector]] = {
    "stripe": _stripe,
    "hubspot": _hubspot,
}


@lru_cache
def generic_configs() -> dict[str, GenericConfig]:
    configs = load_configs()
    clashes = set(configs) & set(FACTORIES)
    if clashes:
        raise ValueError(f"providers/ files reuse built-in connector ids: {sorted(clashes)}")
    return configs


def get_connector(provider: str) -> Connector:
    factory = FACTORIES.get(provider)
    if factory is not None:
        return factory()
    config = generic_configs().get(provider)
    if config is not None:
        return GenericRestConnector(config)
    raise PermanentError(f"no connector for provider {provider!r}")
