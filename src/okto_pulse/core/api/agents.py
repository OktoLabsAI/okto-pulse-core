"""Agent API endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.infra.database import get_db
from okto_pulse.core.models import (
    AgentBoardOverridesUpdate,
    AgentBoardResponse,
    AgentCreate,
    AgentResponse,
    AgentSummary,
    AgentUpdate,
)
from okto_pulse.core.services import AgentService, BoardService

router = APIRouter()


# ---------------------------------------------------------------------------
# Global agent CRUD (ownership via created_by)
# ---------------------------------------------------------------------------


@router.post("", response_model=AgentResponse, status_code=status.HTTP_201_CREATED)
async def create_agent(
    data: AgentCreate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Create a new global agent (not tied to any board)."""
    service = AgentService(db)
    agent, api_key = await service.create_agent(user_id, data)
    await db.commit()
    agent = await service.get_agent(agent.id)

    response = AgentResponse.model_validate(agent)
    response.api_key = api_key
    return response


@router.get("", response_model=list[AgentResponse])
async def list_my_agents(
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all agents owned by the current user (with api_key)."""
    service = AgentService(db)
    agents = await service.list_agents_for_user(user_id)
    return agents


@router.get("/board/{board_id}", response_model=list[AgentSummary])
async def list_agents_for_board(
    board_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """List all agents with access to a board."""
    board_service = BoardService(db)
    board = await board_service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")

    service = AgentService(db)
    agents = await service.list_agents_for_board(board_id)
    return agents


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Get an agent by ID (owner only)."""
    service = AgentService(db)
    agent = await service.get_agent(agent_id)
    if not agent or agent.created_by != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")
    return agent


@router.patch("/{agent_id}", response_model=AgentResponse)
async def update_agent(
    agent_id: str,
    data: AgentUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update an agent (owner only)."""
    service = AgentService(db)
    agent = await service.get_agent(agent_id)
    if not agent or agent.created_by != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")

    updated = await service.update_agent(agent_id, data)
    await db.commit()

    from okto_pulse.core.mcp.server import invalidate_agent_cache
    invalidate_agent_cache(agent_id)

    updated = await service.get_agent(agent_id)
    return updated


@router.post("/{agent_id}/regenerate-key", response_model=dict)
async def regenerate_agent_key(
    agent_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Regenerate an agent's API key (owner only)."""
    service = AgentService(db)
    agent = await service.get_agent(agent_id)
    if not agent or agent.created_by != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")

    updated, new_key = await service.regenerate_key(agent_id)
    await db.commit()

    return {"message": "API key regenerated", "api_key": new_key}


@router.delete("/{agent_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_agent(
    agent_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Delete an agent (owner only)."""
    service = AgentService(db)
    agent = await service.get_agent(agent_id)
    if not agent or agent.created_by != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")

    await service.delete_agent(agent_id)
    await db.commit()


# ---------------------------------------------------------------------------
# Board access grant / revoke
# ---------------------------------------------------------------------------


@router.post(
    "/{agent_id}/boards/{board_id}",
    response_model=AgentBoardResponse,
    status_code=status.HTTP_201_CREATED,
)
async def grant_board_access(
    agent_id: str,
    board_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Grant an agent access to a board. Requires owning both the agent and the board."""
    service = AgentService(db)
    agent = await service.get_agent(agent_id)
    if not agent or agent.created_by != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")

    board_service = BoardService(db)
    board = await board_service.get_board(board_id, user_id)
    if not board:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board not found")

    if await service.agent_has_board_access(agent_id, board_id):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Access already granted")

    grant = await service.grant_board_access(agent_id, board_id, user_id)
    await db.commit()
    return grant


@router.patch("/{agent_id}/boards/{board_id}", response_model=AgentBoardResponse)
async def update_board_overrides(
    agent_id: str,
    board_id: str,
    data: AgentBoardOverridesUpdate,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Update permission overrides for an agent on a board (ceiling model)."""
    service = AgentService(db)
    agent = await service.get_agent(agent_id)
    if not agent or agent.created_by != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")

    ab = await service.update_board_overrides(agent_id, board_id, data.permission_overrides)
    if not ab:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Board access not found")
    await db.commit()

    from okto_pulse.core.mcp.server import invalidate_agent_cache
    invalidate_agent_cache(agent_id)

    return ab


@router.delete("/{agent_id}/boards/{board_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_board_access(
    agent_id: str,
    board_id: str,
    user_id: str = Depends(require_user),
    db: AsyncSession = Depends(get_db),
):
    """Revoke an agent's access to a board. Requires owning the agent or the board."""
    service = AgentService(db)
    agent = await service.get_agent(agent_id)
    if not agent or agent.created_by != user_id:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Agent not found")

    revoked = await service.revoke_board_access(agent_id, board_id)
    if not revoked:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Access not found")
    await db.commit()
