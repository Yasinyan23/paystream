class DomainError(Exception):
    """An expected business error; the API maps each subclass to a Problem Details response."""


class IdempotencyKeyReusedError(DomainError):
    def __init__(self, key: str) -> None:
        super().__init__(f"Idempotency-Key {key!r} was already used with a different request body")
        self.key = key
