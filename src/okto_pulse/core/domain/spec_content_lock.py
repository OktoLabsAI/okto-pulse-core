"""Public content-lock error shared by application and inbound adapters."""


class SpecLockedError(Exception):
    """A content edit requires reopening the Spec in a new Draft edition.

    Moving within the same edition preserves Current; reopening clears its
    pointer and preserves history. Operational regression evidence for an
    eligible scenario does not unlock semantic content.
    """

    def __init__(
        self,
        spec_id: str,
        current_validation_id: str | None = None,
        message: str | None = None,
    ):
        self.spec_id = spec_id
        self.current_validation_id = current_validation_id
        self.message = message or (
            "Spec is locked because validation passed. "
            "Move the spec to draft to open a new edition "
            "(Current validation will be cleared; history is preserved)."
        )
        super().__init__(self.message)
