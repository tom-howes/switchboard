"""The two kinds of failure the engine distinguishes.

The whole retry policy hangs off one question: would trying again help?
A 503 or a timeout probably clears on its own. A 401 or a payload the mapping
cannot read will fail identically on every attempt, so retrying it only burns
the provider's rate limit and delays the moment a human finds out.
"""

from __future__ import annotations


class RetryableError(Exception):
    """Transient: timeouts, 5xx, 429.

    `retry_after` is for when the provider has told us how long to wait (a 429
    with a Retry-After header). Their number beats our backoff guess, because
    retrying sooner than they asked just earns another 429.
    """

    def __init__(self, message: str, *, retry_after: float | None = None):
        super().__init__(message)
        self.retry_after = retry_after


class PermanentError(Exception):
    """Will fail the same way every time: bad credentials, a 400, a mapping
    that cannot read the payload. Goes straight to the dead letter table.
    """


class AuthError(PermanentError):
    """The provider rejected our credentials (401/403).

    Permanent for this job, and also a fact about the connection: every other
    job for it will fail the same way. The worker flips the connection to
    'needs_reauth', and the claim query stops handing out its jobs until a
    human reconnects. They stay pending rather than burning their retry budget.
    """


class NotFoundError(PermanentError):
    """A 404. Its own type because "this record no longer exists" is an answer
    fetch_one needs to act on, not a failure to report."""
