"""Current assessment admissibility is independent of persistence format."""

from dataclasses import replace

import pytest

from okto_pulse.core.domain.guideline_policy import GuidelineEnforcement
from okto_pulse.core.domain.guideline_semantic_assessment import (
    SemanticAssessmentAssessor, SemanticAssessmentInadmissibleError,
    validate_semantic_assessment_admissibility,
)
from test_native_semantic_history_currentness import _fixture


@pytest.mark.parametrize("enforcement,self_assessment,confidence_delta,cause", [
    (GuidelineEnforcement.BLOCKING, True, 0, "assessor_separation_required"),
    (GuidelineEnforcement.BLOCKING, False, 0, None),
    (GuidelineEnforcement.ADVISORY, True, 0, None),
    (GuidelineEnforcement.ADVISORY, False, 0, None),
    (GuidelineEnforcement.BLOCKING, False, -1, "confidence_below_minimum"),
    (GuidelineEnforcement.ADVISORY, True, -1, "confidence_below_minimum"),
    (GuidelineEnforcement.BLOCKING, True, -1, "confidence_below_minimum"),
])
def test_native_admissibility_preserves_confidence_and_blocking_separation(enforcement, self_assessment, confidence_delta, cause):
    _, subject, binding, _ = _fixture()
    binding = replace(binding, enforcement=enforcement, configuration_digest=None)
    assessor = SemanticAssessmentAssessor(agent_id=subject.last_semantic_editor_id if self_assessment else "independent-reviewer")
    arguments = dict(assessor=assessor, confidence=binding.minimum_confidence + confidence_delta,
                     binding=binding, subject_snapshot=subject)
    if cause is None:
        validate_semantic_assessment_admissibility(**arguments)
    else:
        with pytest.raises(SemanticAssessmentInadmissibleError) as error:
            validate_semantic_assessment_admissibility(**arguments)
        assert error.value.cause == cause
