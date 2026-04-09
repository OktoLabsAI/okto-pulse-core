"""Authentication abstractions — provider pattern with FastAPI dependencies."""

from abc import ABC, abstractmethod
from typing import Any

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

security = HTTPBearer(auto_error=False)


class AuthProvider(ABC):
    """Abstract authentication provider.

    Ecosystem packages implement this to plug in Clerk, internal-BFF, or
    any other auth mechanism.
    """

    @abstractmethod
    async def get_current_user(
        self,
        request: Request,
        credentials: HTTPAuthorizationCredentials | None,
    ) -> dict[str, Any] | None: ...

    @abstractmethod
    def get_user_id(self, user: dict[str, Any] | None) -> str: ...

    @abstractmethod
    async def get_realm_id(
        self,
        request: Request,
        user: dict[str, Any] | None,
    ) -> str | None: ...


_auth_provider: AuthProvider | None = None


def configure_auth(provider: AuthProvider) -> None:
    """Register the active AuthProvider at startup."""
    global _auth_provider
    _auth_provider = provider


def get_auth_provider() -> AuthProvider:
    """Return the registered AuthProvider or raise."""
    if _auth_provider is None:
        raise RuntimeError("AuthProvider not configured. Call configure_auth() first.")
    return _auth_provider


# ---------------------------------------------------------------------------
# FastAPI dependencies that delegate to the registered provider
# ---------------------------------------------------------------------------


async def get_current_user(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
) -> dict[str, Any] | None:
    """Resolve the current user via the registered AuthProvider."""
    return await get_auth_provider().get_current_user(request, credentials)


async def get_current_user_id(
    user: dict | None = Depends(get_current_user),
) -> str:
    """Extract user ID from the resolved user dict."""
    return get_auth_provider().get_user_id(user)


def require_user(user_id: str = Depends(get_current_user_id)) -> str:
    """Dependency that ensures a user is authenticated."""
    return user_id


async def get_realm_id(
    request: Request,
    user: dict | None = Depends(get_current_user),
) -> str | None:
    """Resolve the realm/org ID via the registered AuthProvider."""
    return await get_auth_provider().get_realm_id(request, user)
