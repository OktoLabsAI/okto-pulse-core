"""Enqueue authored work outside the graph; event retry owns enqueue failures."""
from okto_pulse.core.domain.learning_materialization_work import LearningCaptureWorkRef
from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import LearningCaptureAdmitted


@register_handler(LearningCaptureAdmitted.event_type)
class LearningCaptureMaterializationEnqueuer:
    async def handle(self, event, session):
        if not isinstance(event, LearningCaptureAdmitted):
            raise ValueError('learning_capture_event_invalid')
        from okto_pulse.core.kg.cognitive_closeout_production import open_cognitive_closeout_pending
        reference = LearningCaptureWorkRef(event.bug_id, event.capture.learning_id, event.capture.generation)
        open_cognitive_closeout_pending(board_id=event.board_id, source_ref=reference.encode(),
            artifact_type='bug', content_hash=event.capture.fingerprint)
