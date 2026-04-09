"""Permission system for agent access control."""


class Permissions:
    """Permission constants for agent access control."""

    # Board
    BOARD_READ = "board:read"

    # Cards
    CARDS_CREATE = "cards:create"
    CARDS_UPDATE = "cards:update"
    CARDS_DELETE = "cards:delete"
    CARDS_MOVE = "cards:move"

    # Comments
    COMMENTS_CREATE = "comments:create"
    COMMENTS_UPDATE = "comments:update"
    COMMENTS_DELETE = "comments:delete"

    # Q&A
    QA_CREATE = "qa:create"
    QA_ANSWER = "qa:answer"
    QA_DELETE = "qa:delete"

    # Specs
    SPECS_CREATE = "specs:create"
    SPECS_UPDATE = "specs:update"
    SPECS_DELETE = "specs:delete"
    SPECS_MOVE = "specs:move"

    # Attachments
    ATTACHMENTS_UPLOAD = "attachments:upload"
    ATTACHMENTS_DELETE = "attachments:delete"

    # Self
    SELF_UPDATE = "self:update"

    ALL = [
        BOARD_READ,
        CARDS_CREATE,
        CARDS_UPDATE,
        CARDS_DELETE,
        CARDS_MOVE,
        SPECS_CREATE,
        SPECS_UPDATE,
        SPECS_DELETE,
        SPECS_MOVE,
        COMMENTS_CREATE,
        COMMENTS_UPDATE,
        COMMENTS_DELETE,
        QA_CREATE,
        QA_ANSWER,
        QA_DELETE,
        ATTACHMENTS_UPLOAD,
        ATTACHMENTS_DELETE,
        SELF_UPDATE,
    ]

    # Default permissions for new agents
    DEFAULT = [
        BOARD_READ,
        CARDS_CREATE,
        CARDS_UPDATE,
        CARDS_MOVE,
        SPECS_CREATE,
        SPECS_UPDATE,
        SPECS_MOVE,
        COMMENTS_CREATE,
        QA_CREATE,
        QA_ANSWER,
        ATTACHMENTS_UPLOAD,
        SELF_UPDATE,
    ]


def has_permission(agent_permissions: list[str] | None, required: str) -> bool:
    """Check if agent has a specific permission.

    If agent_permissions is None, the agent has full access (backwards compat).
    """
    if agent_permissions is None:
        return True
    return required in agent_permissions


def check_permission(agent_permissions: list[str] | None, required: str) -> str | None:
    """Check permission and return error message if denied.

    Returns None if allowed, error message string if denied.
    """
    if has_permission(agent_permissions, required):
        return None
    return f"Permission denied: requires '{required}'"
