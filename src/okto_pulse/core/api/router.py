"""Main API router combining all sub-routers."""

from fastapi import APIRouter

from okto_pulse.core.api.boards import router as boards_router
from okto_pulse.core.api.cards import router as cards_router
from okto_pulse.core.api.agents import router as agents_router
from okto_pulse.core.api.attachments import router as attachments_router
from okto_pulse.core.api.qa import router as qa_router
from okto_pulse.core.api.comments import router as comments_router
from okto_pulse.core.api.ideations import router as ideations_router
from okto_pulse.core.api.refinements import router as refinements_router
from okto_pulse.core.api.guidelines import router as guidelines_router
from okto_pulse.core.api.specs import router as specs_router
from okto_pulse.core.api.analytics import router as analytics_router
from okto_pulse.core.api.architecture import router as architecture_router
from okto_pulse.core.api.presets import router as presets_router
from okto_pulse.core.api.sprints import router as sprints_router
from okto_pulse.core.api.kg_routes import router as kg_router
from okto_pulse.core.api.me import router as me_router
from okto_pulse.core.api.discovery import router as discovery_router
from okto_pulse.core.api.settings import router as settings_router
from okto_pulse.core.api.queue_health import router as queue_health_router
from okto_pulse.core.api.kg_health import router as kg_health_router
from okto_pulse.core.api.kg_tick import router as kg_tick_router
from okto_pulse.core.api.dead_letter import router as dead_letter_router
from okto_pulse.core.api.traceability import router as traceability_router

api_router = APIRouter(prefix="/api/v1")

api_router.include_router(boards_router, prefix="/boards", tags=["boards"])
api_router.include_router(cards_router, prefix="/cards", tags=["cards"])
api_router.include_router(ideations_router, tags=["ideations"])
api_router.include_router(refinements_router, tags=["refinements"])
api_router.include_router(specs_router, tags=["specs"])
api_router.include_router(guidelines_router, tags=["guidelines"])
api_router.include_router(agents_router, prefix="/agents", tags=["agents"])
api_router.include_router(attachments_router, prefix="/attachments", tags=["attachments"])
api_router.include_router(qa_router, prefix="/qa", tags=["qa"])
api_router.include_router(comments_router, prefix="/comments", tags=["comments"])
api_router.include_router(analytics_router, tags=["analytics"])
api_router.include_router(architecture_router, tags=["architecture"])
api_router.include_router(presets_router, prefix="/presets", tags=["presets"])
api_router.include_router(sprints_router, tags=["sprints"])
api_router.include_router(kg_router, tags=["knowledge-graph"])
api_router.include_router(me_router, tags=["me"])
api_router.include_router(discovery_router, tags=["discovery"])
api_router.include_router(settings_router, tags=["settings"])
api_router.include_router(queue_health_router, tags=["queue-health"])
api_router.include_router(kg_health_router, tags=["kg-health"])
api_router.include_router(kg_tick_router, tags=["kg-tick"])
api_router.include_router(dead_letter_router, tags=["dead-letter"])
api_router.include_router(traceability_router, tags=["traceability"])
