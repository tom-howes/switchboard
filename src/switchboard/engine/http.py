"""Turn HTTP outcomes into the engine's two failure kinds.

Every connector calls through `request`, so the decision "is this worth
retrying?" is made once, here, rather than re-decided (differently) in each
connector. A connector that wants different behaviour for a provider quirk
can still catch and re-raise, but the default is shared.
"""

from __future__ import annotations

import httpx

from .errors import AuthError, NotFoundError, PermanentError, RetryableError

TIMEOUT = httpx.Timeout(30.0, connect=10.0)


def _retry_after(response: httpx.Response) -> float | None:
    # Retry-After may also be an HTTP date. Seconds is what Stripe and HubSpot
    # send in practice; a date falls back to our own backoff, which is safe.
    value = response.headers.get("retry-after", "")
    try:
        return float(value)
    except ValueError:
        return None


def raise_for_status(response: httpx.Response, provider: str) -> None:
    status = response.status_code
    if status < 400:
        return
    detail = f"{provider} {response.request.method} {response.request.url.path} -> {status}"
    if status == 429:
        raise RetryableError(f"{detail} rate limited", retry_after=_retry_after(response))
    if status in (401, 403):
        raise AuthError(detail)
    if status == 404:
        raise NotFoundError(detail)
    if status >= 500 or status == 408:
        raise RetryableError(detail)
    # Remaining 4xx: we sent something the provider will never accept.
    raise PermanentError(f"{detail}: {response.text[:500]}")


async def request(client: httpx.AsyncClient, method: str, url: str, *, provider: str, **kwargs) -> httpx.Response:
    try:
        response = await client.request(method, url, **kwargs)
    except httpx.TransportError as exc:
        # DNS failure, refused connection, timeout: the network, not the request.
        raise RetryableError(f"{provider} {method} {url}: {type(exc).__name__}") from exc
    raise_for_status(response, provider)
    return response
