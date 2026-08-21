"""Core application-service catalog construction.

The catalog is the temporary composition point while F05 replaces the service
internals with explicit persistence ports. Its relational construction input is
private and is never exposed by the UnitOfWork or to application use cases.
"""

from __future__ import annotations

from functools import cached_property


class CoreAnalyticsOperations:
    def __init__(
        self,
        relational_context: object,
        *,
        code_traceability_read: object | None = None,
    ) -> None:
        self.__relational_context = relational_context
        self.__code_traceability_read = code_traceability_read

    async def board_is_owned_by(self, board_id: str, user_id: str) -> bool:
        from okto_pulse.core.services.analytics_service import board_is_owned_by

        return await board_is_owned_by(self.__relational_context, board_id, user_id)

    async def blockers(
        self,
        board_id: str,
        *,
        stale_hours: int = 72,
        filter_type: str | None = None,
    ):  # noqa: ANN201
        from okto_pulse.core.services.analytics_service import compute_blockers
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return await compute_blockers(
            self.__relational_context,
            board_id,
            stale_hours=stale_hours,
            filter_type=filter_type,
            spec_dependency_persistence=(
                require_relational_application_adapter().spec_dependencies(
                    self.__relational_context
                )
            ),
        )

    async def mcp_board_analytics(
        self,
        board_id: str,
        *,
        metric_type: str,
        dt_from,
        dt_to,
    ):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import (
            compute_mcp_board_analytics,
        )

        return await compute_mcp_board_analytics(
            self.__relational_context,
            board_id,
            metric_type=metric_type,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def board_kg(
        self,
        *,
        query,
        as_of,
        population_scope,
        exclusions,
    ):  # noqa: ANN001, ANN201
        from okto_pulse.core.ports.board_kg_analytics import BoardKgAnalyticsQuery

        if isinstance(query, BoardKgAnalyticsQuery):
            from okto_pulse.core.ports.relational_application import (
                require_relational_application_adapter,
            )
            from okto_pulse.core.services.board_kg_analytics import (
                BoardKgEffectivenessService,
            )

            evidence = require_relational_application_adapter().board_kg_analytics_read(
                self.__relational_context
            )
            return await BoardKgEffectivenessService.project(
                self.__relational_context,
                query=query,
                evidence_port=evidence,
            )
        from okto_pulse.core.services.board_kg_analytics import (
            BoardKgAnalyticsService,
        )

        return await BoardKgAnalyticsService.project_from_public_services(
            self.__relational_context,
            query=query,
            as_of=as_of,
            population_scope=population_scope,
            exclusions=exclusions,
        )

    async def delivery_forecast(self, *, query):  # noqa: ANN001, ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )
        from okto_pulse.core.services.delivery_forecast import (
            DeliveryForecastService,
        )

        evidence = require_relational_application_adapter().delivery_forecast_read(
            self.__relational_context
        )
        return await DeliveryForecastService.project(
            self.__relational_context,
            query=query,
            evidence_port=evidence,
        )

    async def delivery_intelligence(
        self,
        *,
        query,
        actor_id,
        operator_visibility,
        cursor_offset,
        limit,
        minimum_sample_size,
    ):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import (
            compute_delivery_intelligence,
        )

        return await compute_delivery_intelligence(
            self.__relational_context,
            query=query,
            actor_id=actor_id,
            operator_visibility=operator_visibility,
            cursor_offset=cursor_offset,
            limit=limit,
            minimum_sample_size=minimum_sample_size,
        )

    async def canonical_coverage(self, *, query, as_of):  # noqa: ANN001, ANN201
        from okto_pulse.core.domain.code_traceability import (
            CodeTraceabilityProjectionProfile,
            CodeTraceabilitySubjectType,
        )
        from okto_pulse.core.ports.code_traceability import (
            CodeTraceabilityProjectionQuery,
        )
        from okto_pulse.core.services.analytics_service import _af, _analytics_list
        from okto_pulse.core.services.coverage_traceability_read_model import (
            build_coverage_traceability_projection,
        )

        boards = await _analytics_list(
            self.__relational_context,
            "board",
            filters=(_af("id", "eq", query.board_id),),
            limit=2,
        )
        if len(boards) != 1:
            raise ValueError("coverage_traceability_board_authority_invalid")
        specs = await _analytics_list(
            self.__relational_context,
            "spec",
            filters=(
                _af("board_id", "eq", query.board_id),
                _af("archived", "is_false"),
            ),
        )
        cards = await _analytics_list(
            self.__relational_context,
            "card",
            filters=(_af("board_id", "eq", query.board_id),),
        )
        contexts = None
        if self.__code_traceability_read is not None:
            contexts = tuple(
                [
                    await self.__code_traceability_read.spec_context(
                        CodeTraceabilityProjectionQuery(
                            board_id=query.board_id,
                            subject_type=CodeTraceabilitySubjectType.SPEC,
                            subject_id=str(spec.id),
                            subject_version=int(getattr(spec, "version", 1)),
                            profile=CodeTraceabilityProjectionProfile.SUMMARY,
                        )
                    )
                    for spec in specs
                ]
            )
        return build_coverage_traceability_projection(
            query=query,
            as_of=as_of,
            board=boards[0],
            specs=specs,
            cards=cards,
            code_traceability_contexts=contexts,
        )

    async def canonical_flow_health(self, *, query, as_of):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import _af, _analytics_list
        from okto_pulse.core.services.coverage_traceability_read_model import (
            build_coverage_traceability_projection,
        )
        from okto_pulse.core.services.flow_health_read_model import (
            build_flow_health_projection,
        )

        boards = await _analytics_list(
            self.__relational_context,
            "board",
            filters=(_af("id", "eq", query.board_id),),
            limit=2,
        )
        if len(boards) != 1:
            raise ValueError("flow_health_board_authority_invalid")
        specs = await _analytics_list(
            self.__relational_context,
            "spec",
            filters=(_af("board_id", "eq", query.board_id),),
        )
        cards = await _analytics_list(
            self.__relational_context,
            "card",
            filters=(_af("board_id", "eq", query.board_id),),
        )
        events = await _analytics_list(
            self.__relational_context,
            "domain_event",
            filters=(
                _af("board_id", "eq", query.board_id),
                _af(
                    "event_type",
                    "in",
                    (
                        "card.created",
                        "card.moved",
                        "card.completion_rejected",
                        "spec.created",
                        "spec.moved",
                    ),
                ),
                _af("occurred_at", "lte", as_of),
            ),
            order_by="occurred_at",
        )
        coverage = build_coverage_traceability_projection(
            query=query,
            as_of=as_of,
            board=boards[0],
            specs=tuple(spec for spec in specs if not spec.archived),
            cards=cards,
        )
        return build_flow_health_projection(
            query=query,
            as_of=as_of,
            board=boards[0],
            specs=specs,
            cards=cards,
            domain_events=events,
            coverage=coverage,
        )

    async def canonical_spec_readiness(self, *, query, as_of):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import _af, _analytics_list
        from okto_pulse.core.services.spec_readiness_read_model import (
            build_spec_readiness_projection,
        )

        specs = await _analytics_list(
            self.__relational_context,
            "spec",
            filters=(
                _af("board_id", "eq", query.board_id),
                _af("archived", "is_false"),
            ),
        )
        return build_spec_readiness_projection(query=query, as_of=as_of, specs=specs)

    async def canonical_policy_resource_readiness(self, *, query, as_of):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import _af, _analytics_list
        from okto_pulse.core.services.policy_resource_readiness_read_model import (
            build_policy_resource_readiness_projection,
        )

        specs = await _analytics_list(
            self.__relational_context,
            "spec",
            filters=(
                _af("board_id", "eq", query.board_id),
                _af("archived", "is_false"),
            ),
        )
        spec_ids = tuple(str(spec.id) for spec in specs)
        cards = await _analytics_list(
            self.__relational_context,
            "card",
            filters=(_af("board_id", "eq", query.board_id),),
        )
        designs = await _analytics_list(
            self.__relational_context,
            "architecture_design",
            filters=(_af("board_id", "eq", query.board_id),),
        )
        knowledge = await _analytics_list(
            self.__relational_context,
            "spec_knowledge_base",
            filters=(_af("spec_id", "in", spec_ids),),
        )
        not_applicable = await _analytics_list(
            self.__relational_context,
            "resource_not_applicable",
            filters=(
                _af("board_id", "eq", query.board_id),
                _af("active", "is_true"),
            ),
        )
        return build_policy_resource_readiness_projection(
            query=query,
            as_of=as_of,
            specs=specs,
            cards=cards,
            architecture_designs=designs,
            spec_knowledge_bases=knowledge,
            not_applicable=not_applicable,
        )

    async def funnel(self, board_id: str, *, dt_from, dt_to):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import compute_funnel

        return await compute_funnel(
            self.__relational_context,
            board_id,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def velocity(
        self,
        board_id: str,
        *,
        granularity: str,
        weeks: int,
        days: int,
        dt_from,
        dt_to,
    ):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import compute_velocity

        return await compute_velocity(
            self.__relational_context,
            board_id,
            granularity=granularity,
            weeks=weeks,
            days=days,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def coverage(self, board_id: str, *, dt_from, dt_to):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import compute_coverage

        return await compute_coverage(
            self.__relational_context,
            board_id,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def overview(self, user_id: str, *, dt_from, dt_to):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import compute_overview

        return await compute_overview(
            self.__relational_context,
            user_id,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def quality(self, board_id: str, *, dt_from, dt_to):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import compute_quality

        return await compute_quality(
            self.__relational_context,
            board_id,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def validations(self, board_id: str, *, dt_from, dt_to):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import compute_validations

        return await compute_validations(
            self.__relational_context,
            board_id,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def spec(self, board_id: str, spec_id: str):  # noqa: ANN201
        from okto_pulse.core.services.analytics_service import compute_spec_analytics

        return await compute_spec_analytics(
            self.__relational_context,
            board_id,
            spec_id,
        )

    async def sprint(self, board_id: str, sprint_id: str):  # noqa: ANN201
        from okto_pulse.core.services.analytics_service import compute_sprint_analytics

        return await compute_sprint_analytics(
            self.__relational_context,
            board_id,
            sprint_id,
        )

    async def sprints(self, board_id: str, *, dt_from, dt_to):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import compute_sprints_analytics

        return await compute_sprints_analytics(
            self.__relational_context,
            board_id,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def agents(self, board_id: str, *, dt_from, dt_to):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import compute_agents

        return await compute_agents(
            self.__relational_context,
            board_id,
            dt_from=dt_from,
            dt_to=dt_to,
        )

    async def entities(
        self,
        entity_type: str,
        board_id: str,
        *,
        offset: int,
        limit: int,
        search: str,
        dt_from,
        dt_to,
    ):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.analytics_service import (
            _list_card_entities,
            _list_ideation_entities,
            _list_spec_entities,
        )

        reader = {
            "ideation": _list_ideation_entities,
            "spec": _list_spec_entities,
            "card": _list_card_entities,
        }[entity_type]
        return await reader(
            self.__relational_context,
            board_id,
            offset,
            limit,
            dt_from,
            dt_to,
            search,
        )

    async def entity_detail(self, entity_type: str, board_id: str, entity_id: str):  # noqa: ANN201
        from okto_pulse.core.services.analytics_service import (
            _card_detail,
            _ideation_detail,
            _refinement_detail,
            _spec_detail,
            _sprint_detail,
        )

        reader = {
            "spec": _spec_detail,
            "ideation": _ideation_detail,
            "card": _card_detail,
            "refinement": _refinement_detail,
            "sprint": _sprint_detail,
        }[entity_type]
        return await reader(self.__relational_context, board_id, entity_id)


class CoreKnowledgePropagationOperations:
    """Transaction-bound facade over selective Knowledge propagation."""

    def __init__(self, relational_context: object) -> None:
        self.__relational_context = relational_context

    @staticmethod
    def _service():  # noqa: ANN205
        from okto_pulse.core.services.knowledge_propagation import (
            KnowledgePropagationService,
        )

        return KnowledgePropagationService()

    async def preflight_creation(self, command):  # noqa: ANN001, ANN201
        return await self._service().preflight_creation(
            self.__relational_context,
            command,
        )

    async def mutate(self, command):  # noqa: ANN001, ANN201
        return await self._service().mutate(self.__relational_context, command)

    async def refresh_by_knowledge_ids(self, command):  # noqa: ANN001, ANN201
        return await self._service().refresh_by_knowledge_ids(
            self.__relational_context,
            command,
        )

    async def reset_for_relink(self, command):  # noqa: ANN001, ANN201
        return await self._service().reset_for_relink(
            self.__relational_context,
            command,
        )

    async def read(self, target):  # noqa: ANN001, ANN201
        return await self._service().read(self.__relational_context, target)

    @staticmethod
    def result_from_receipt(receipt):  # noqa: ANN001, ANN205
        from okto_pulse.core.services.knowledge_propagation import (
            KnowledgeMutationResultV2Projector,
        )

        return KnowledgeMutationResultV2Projector.from_receipt(receipt)


class CoreApplicationServiceCatalog:
    """Lazily construct Core-owned application services for one transaction."""

    def __init__(self, relational_context: object) -> None:
        self.__relational_context = relational_context

    @cached_property
    def agent_authentication(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().agent_authentication(
            self.__relational_context
        )

    @cached_property
    def agents(self):  # noqa: ANN201
        from okto_pulse.core.services.main import AgentService

        return AgentService(self.__relational_context)

    @cached_property
    def analytics(self) -> CoreAnalyticsOperations:
        return CoreAnalyticsOperations(
            self.__relational_context,
            code_traceability_read=self.code_traceability_read,
        )

    @cached_property
    def amendments(self):  # noqa: ANN201
        from okto_pulse.core.services.amendment_revision_api import (
            AmendmentRevisionApiService,
        )

        return AmendmentRevisionApiService(self.__relational_context)

    @cached_property
    def architecture_designs(self):  # noqa: ANN201
        from okto_pulse.core.services.architecture import ArchitectureDesignRepository

        return ArchitectureDesignRepository(self.__relational_context)

    @cached_property
    def architecture_diagrams(self):  # noqa: ANN201
        from okto_pulse.core.services.architecture import ArchitectureDiagramStore

        return ArchitectureDiagramStore(self.__relational_context)

    @cached_property
    def architecture_propagation(self):  # noqa: ANN201
        from okto_pulse.core.services.architecture import ArchitecturePropagationService

        return ArchitecturePropagationService(self.__relational_context)

    @cached_property
    def archives(self):  # noqa: ANN201
        from okto_pulse.core.services.main import ArchiveService

        return ArchiveService(self.__relational_context)

    @cached_property
    def attachments(self):  # noqa: ANN201
        from okto_pulse.core.services.main import AttachmentService

        return AttachmentService(self.__relational_context)

    @cached_property
    def boards(self):  # noqa: ANN201
        from okto_pulse.core.services.main import BoardService

        return BoardService(self.__relational_context)

    @cached_property
    def bug_regression_preview(self):  # noqa: ANN201
        from okto_pulse.core.services.bug_regression_preview import (
            BugRegressionScenarioPreviewService,
        )

        return BugRegressionScenarioPreviewService(self.__relational_context)

    @cached_property
    def cards(self):  # noqa: ANN201
        from okto_pulse.core.services.main import CardService

        return CardService(self.__relational_context)

    @cached_property
    def comments(self):  # noqa: ANN201
        from okto_pulse.core.services.main import CommentService

        return CommentService(self.__relational_context)

    @cached_property
    def code_investigations(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().code_investigations(
            self.__relational_context
        )

    @cached_property
    def code_traceability(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().code_traceability(
            self.__relational_context
        )

    @cached_property
    def code_traceability_read(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().code_traceability_read(
            self.__relational_context
        )

    @cached_property
    def entity_pages(self):  # noqa: ANN201
        from okto_pulse.core.application.use_cases.entity_pagination import (
            EntityPageService,
        )

        return EntityPageService(self.__relational_context)

    @cached_property
    def default_board_config(self):  # noqa: ANN201
        from okto_pulse.core.services.default_board_config_api import (
            DefaultBoardConfigApiService,
        )

        return DefaultBoardConfigApiService(self.__relational_context)

    @cached_property
    def design_systems(self):  # noqa: ANN201
        from okto_pulse.core.services.design_system import DesignSystemService

        return DesignSystemService(self.__relational_context)

    @cached_property
    def discovery_catalog(self):  # noqa: ANN201
        from okto_pulse.core.services.discovery_catalog_reader import (
            DiscoveryCatalogReader,
        )

        return DiscoveryCatalogReader(self.__relational_context)

    @cached_property
    def guidelines(self):  # noqa: ANN201
        from okto_pulse.core.services.main import GuidelineService

        return GuidelineService(self.__relational_context)

    @cached_property
    def ideation_knowledge(self):  # noqa: ANN201
        from okto_pulse.core.services.main import IdeationKnowledgeService

        return IdeationKnowledgeService(self.__relational_context)

    @cached_property
    def ideation_qa(self):  # noqa: ANN201
        from okto_pulse.core.services.main import IdeationQAService

        return IdeationQAService(self.__relational_context)

    @cached_property
    def ideations(self):  # noqa: ANN201
        from okto_pulse.core.services.main import IdeationService

        return IdeationService(self.__relational_context)

    @cached_property
    def kg(self):  # noqa: ANN201
        from okto_pulse.core.application.kg_operations import (
            CoreKnowledgeGraphOperations,
        )

        return CoreKnowledgeGraphOperations(self.__relational_context)

    @cached_property
    def knowledge_propagation(self) -> CoreKnowledgePropagationOperations:
        return CoreKnowledgePropagationOperations(self.__relational_context)

    @cached_property
    def mockup_design_gate(self):  # noqa: ANN201
        from okto_pulse.core.services.design_system import MockupDesignSystemGate

        return MockupDesignSystemGate(self.__relational_context)

    @cached_property
    def permission_presets(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().permission_presets(
            self.__relational_context
        )

    @cached_property
    def quality_assessments(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().quality_assessments(
            self.__relational_context
        )

    @cached_property
    def quality_assessment_lifecycle(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().quality_assessment_lifecycle(
            self.__relational_context
        )

    @cached_property
    def checklists(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().checklists(
            self.__relational_context
        )

    @cached_property
    def spec_dependencies(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )
        from okto_pulse.core.services.spec_dependency import SpecDependencyService

        persistence = require_relational_application_adapter().spec_dependencies(
            self.__relational_context,
        )
        return SpecDependencyService(
            persistence,
            self.__relational_context,
        )

    @cached_property
    def research_decisions(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        return require_relational_application_adapter().research_decisions(
            self.__relational_context
        )

    @cached_property
    def qa(self):  # noqa: ANN201
        from okto_pulse.core.services.main import QAService

        return QAService(self.__relational_context)

    @cached_property
    def refinement_knowledge(self):  # noqa: ANN201
        from okto_pulse.core.services.main import RefinementKnowledgeService

        return RefinementKnowledgeService(self.__relational_context)

    @cached_property
    def refinement_qa(self):  # noqa: ANN201
        from okto_pulse.core.services.main import RefinementQAService

        return RefinementQAService(self.__relational_context)

    @cached_property
    def refinements(self):  # noqa: ANN201
        from okto_pulse.core.services.main import RefinementService

        return RefinementService(self.__relational_context)

    @cached_property
    def resource_gate(self):  # noqa: ANN201
        from okto_pulse.core.services.resource_gate import ResourceGateService

        return ResourceGateService(self.__relational_context)

    @cached_property
    def shares(self):  # noqa: ANN201
        from okto_pulse.core.services.main import ShareService

        return ShareService(self.__relational_context)

    @cached_property
    def spec_knowledge(self):  # noqa: ANN201
        from okto_pulse.core.services.main import SpecKnowledgeService

        return SpecKnowledgeService(self.__relational_context)

    @cached_property
    def spec_qa(self):  # noqa: ANN201
        from okto_pulse.core.services.main import SpecQAService

        return SpecQAService(self.__relational_context)

    @cached_property
    def specs(self):  # noqa: ANN201
        from okto_pulse.core.services.main import SpecService

        return SpecService(self.__relational_context)

    @cached_property
    def sprint_qa(self):  # noqa: ANN201
        from okto_pulse.core.services.main import SprintQAService

        return SprintQAService(self.__relational_context)

    @cached_property
    def sprints(self):  # noqa: ANN201
        from okto_pulse.core.services.main import SprintService

        return SprintService(self.__relational_context)

    @cached_property
    def stories(self):  # noqa: ANN201
        from okto_pulse.core.services.main import StoryService

        return StoryService(self.__relational_context)

    @cached_property
    def structured_specs(self):  # noqa: ANN201
        from okto_pulse.core.services.spec_structured_entities import (
            StructuredSpecEntityService,
        )

        return StructuredSpecEntityService(self.__relational_context)

    async def resolve_user_permissions(self, user_id: str, board_id: str):  # noqa: ANN201
        from okto_pulse.core.services.main import resolve_user_permissions

        return await resolve_user_permissions(
            self.__relational_context,
            user_id,
            board_id,
        )

    async def resolve_actor_name(
        self, actor_id: str, board_id: str | None = None
    ) -> str:
        from okto_pulse.core.services.main import resolve_actor_name

        return await resolve_actor_name(
            self.__relational_context,
            actor_id,
            board_id,
        )

    async def log_card_collaboration_activity(
        self,
        card_id: str,
        action: str,
        *,
        actor_id: str,
        actor_type: str,
        actor_name: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        from okto_pulse.core.services.main import log_card_collaboration_activity

        await log_card_collaboration_activity(
            self.__relational_context,
            card_id,
            action,
            actor_id=actor_id,
            actor_type=actor_type,
            actor_name=actor_name,
            details=details,
        )

    async def card_belongs_to_board(self, board_id: str, card_id: str) -> bool:
        from okto_pulse.core.services.main import card_belongs_to_board

        return await card_belongs_to_board(
            self.__relational_context,
            board_id,
            card_id,
        )

    async def comment_card_id(self, comment_id: str) -> str | None:
        from okto_pulse.core.services.main import comment_card_id

        return await comment_card_id(self.__relational_context, comment_id)

    async def resolve_choice_comment_actor_name(
        self, comment_id: str, actor_id: str
    ) -> str | None:
        from okto_pulse.core.services.main import resolve_choice_comment_actor_name

        return await resolve_choice_comment_actor_name(
            self.__relational_context,
            comment_id,
            actor_id,
        )

    async def qa_card_id(self, qa_id: str) -> str | None:
        from okto_pulse.core.services.main import qa_card_id

        return await qa_card_id(self.__relational_context, qa_id)

    async def compute_card_activity(
        self, card_id: str, *, limit: int = 50
    ) -> list[object]:
        from okto_pulse.core.services.main import compute_card_activity

        return await compute_card_activity(
            self.__relational_context,
            card_id,
            limit=limit,
        )

    async def compute_card_seen_status(self, card_id: str):  # noqa: ANN201
        from okto_pulse.core.services.main import compute_card_seen_status

        return await compute_card_seen_status(self.__relational_context, card_id)

    async def build_propagation_legacy_report(
        self,
        *,
        board_id: str,
        limit: int = 100,
        offset: int = 0,
        include_clean: bool = False,
        parent_type_filter: str | None = None,
        surface: str = "service",
    ) -> dict[str, object]:
        from okto_pulse.core.services.architecture_propagation_legacy import (
            build_propagation_legacy_report,
        )

        return await build_propagation_legacy_report(
            self.__relational_context,
            board_id=board_id,
            limit=limit,
            offset=offset,
            include_clean=include_clean,
            parent_type_filter=parent_type_filter,
            surface=surface,
        )

    async def resolve_effective_card_copy_plan(
        self, *, board_id: str, spec_id: str, resource_type: str
    ) -> dict[str, object]:
        from okto_pulse.core.services.effective_resource_propagation import (
            resolve_effective_card_copy_plan,
        )

        return await resolve_effective_card_copy_plan(
            self.__relational_context,
            board_id=board_id,
            spec_id=spec_id,
            resource_type=resource_type,
        )

    async def load_effective_kb_items(
        self, source_entity_type: str, source_entity_id: str
    ) -> list[dict[str, object]]:
        from okto_pulse.core.services.effective_resource_propagation import (
            load_effective_kb_items,
        )

        return await load_effective_kb_items(
            self.__relational_context,
            source_entity_type,
            source_entity_id,
        )

    async def load_effective_mockup_items(
        self, source_entity_type: str, source_entity_id: str
    ) -> list[dict[str, object]]:
        from okto_pulse.core.services.effective_resource_propagation import (
            load_effective_mockup_items,
        )

        return await load_effective_mockup_items(
            self.__relational_context,
            source_entity_type,
            source_entity_id,
        )

    async def propagate_effective_resources_to_spec(
        self,
        *,
        board_id: str,
        spec: object,
        refinement_id: str,
        user_id: str,
        mockup_ids: list[str] | None = None,
        kb_ids: list[str] | None = None,
        architecture_design_ids: list[str] | None = None,
        architecture_propagation_mode: str = "copy",
        resolved_lineage: object | None = None,
    ) -> dict[str, object]:
        from okto_pulse.core.services.effective_resource_propagation import (
            propagate_effective_resources_to_spec,
        )

        return await propagate_effective_resources_to_spec(
            self.__relational_context,
            board_id=board_id,
            spec=spec,
            refinement_id=refinement_id,
            user_id=user_id,
            mockup_ids=mockup_ids,
            kb_ids=kb_ids,
            architecture_design_ids=architecture_design_ids,
            architecture_propagation_mode=architecture_propagation_mode,
            resolved_lineage=resolved_lineage,
        )

    async def resolve_effective_spec_parent_lineage(
        self,
        *,
        board_id: str,
        ideation_id: str | None = None,
        refinement_id: str | None = None,
    ) -> object | None:
        from okto_pulse.core.services.effective_resource_propagation import (
            resolve_effective_spec_parent_lineage,
        )

        return await resolve_effective_spec_parent_lineage(
            self.__relational_context,
            board_id=board_id,
            ideation_id=ideation_id,
            refinement_id=refinement_id,
        )

    async def list_my_mentions(
        self,
        *,
        board_id: str,
        agent_id: str,
        agent_name: str | None,
        include_seen: bool,
    ):  # noqa: ANN201
        from okto_pulse.core.services.main import mcp_list_my_mentions

        return await mcp_list_my_mentions(
            self.__relational_context,
            board_id=board_id,
            agent_id=agent_id,
            agent_name=agent_name,
            include_seen=include_seen,
        )

    async def mark_mentions_seen(
        self,
        *,
        board_id: str,
        agent_id: str,
        agent_name: str | None,
        item_ids: list[str],
    ) -> tuple[int, int]:
        from okto_pulse.core.services.main import mcp_mark_mentions_seen

        return await mcp_mark_mentions_seen(
            self.__relational_context,
            board_id=board_id,
            agent_id=agent_id,
            agent_name=agent_name,
            item_ids=item_ids,
        )

    async def get_unseen_summary(
        self, *, board_id: str, agent_id: str, agent_name: str | None
    ):  # noqa: ANN201
        from okto_pulse.core.services.main import mcp_get_unseen_summary

        return await mcp_get_unseen_summary(
            self.__relational_context,
            board_id=board_id,
            agent_id=agent_id,
            agent_name=agent_name,
        )

    async def get_activity_log_rows(
        self,
        *,
        board_id: str,
        limit: int,
        cursor_pair,
        effective_offset: int,
        action: str | None,
        card_id: str | None,
        include_details: bool,
    ):  # noqa: ANN001, ANN201
        from okto_pulse.core.services.main import mcp_get_activity_log_rows

        return await mcp_get_activity_log_rows(
            self.__relational_context,
            board_id=board_id,
            limit=limit,
            cursor_pair=cursor_pair,
            effective_offset=effective_offset,
            action=action,
            card_id=card_id,
            include_details=include_details,
        )

    async def update_resource_gate_board_settings(
        self,
        board_id: str,
        user_id: str,
        *,
        require_spec_resource_task_coverage: bool,
    ):  # noqa: ANN201
        from okto_pulse.core.services.main import update_resource_gate_board_settings

        return await update_resource_gate_board_settings(
            self.__relational_context,
            board_id,
            user_id,
            require_spec_resource_task_coverage=require_spec_resource_task_coverage,
        )

    async def get_runtime_settings(self):  # noqa: ANN201
        from okto_pulse.core.services.settings_service import get_runtime_settings

        return await get_runtime_settings(self.__relational_context)

    async def put_runtime_settings(
        self,
        values: dict[str, int],
        *,
        actor_id: str,
        migration_plan_ref: str | None,
        restart_policy: str | None,
        scheduler_control: object | None,
    ):  # noqa: ANN201
        from okto_pulse.core.services.settings_service import put_runtime_settings

        return await put_runtime_settings(
            self.__relational_context,
            values,
            actor_id=actor_id,
            migration_plan_ref=migration_plan_ref,
            restart_policy=restart_policy,
            scheduler_control=scheduler_control,
        )

    async def build_lineage_graph(
        self,
        board_id: str,
        *,
        entity_type: str,
        entity_id: str,
        include_artifacts: bool,
    ):  # noqa: ANN201
        from okto_pulse.core.services.traceability import build_lineage_graph

        return await build_lineage_graph(
            self.__relational_context,
            board_id,
            entity_type=entity_type,
            entity_id=entity_id,
            include_artifacts=include_artifacts,
        )

    async def list_discovery_selector_options(
        self,
        *,
        board_id: str,
        selector_kind: str,
        identity: str,
        spec_id: str | None,
        child_type: str | None,
        status: str | None,
        q: str | None,
        limit: int,
        offset: int,
        include_superseded: bool,
    ):  # noqa: ANN201
        from okto_pulse.core.services.discovery_catalog_reader import (
            DiscoverySelectorRestAccessPolicy,
        )
        from okto_pulse.core.services.discovery_selector_catalog import (
            DiscoverySelectorAccessDenied,
            DiscoverySelectorCatalog,
            get_default_discovery_selector_cache,
        )

        policy = DiscoverySelectorRestAccessPolicy()
        if not await policy.can_read_board(
            self.__relational_context,
            identity,
            board_id,
        ):
            raise DiscoverySelectorAccessDenied("selector_access_denied")
        catalog = DiscoverySelectorCatalog(
            policy,
            cache=get_default_discovery_selector_cache(),
        )
        result = await catalog.list_options(
            self.__relational_context,
            board_id=board_id,
            selector_kind=selector_kind,
            identity=identity,
            spec_id=spec_id,
            child_type=child_type,
            status=status,
            q=q,
            limit=limit,
            offset=offset,
            include_superseded=include_superseded,
        )
        return result.to_dict(), result.cache_status

    async def execute_discovery_intent(
        self,
        *,
        identity: str,
        board_id: str,
        intent: object,
        params: dict[str, object],
    ):  # noqa: ANN201
        from okto_pulse.core.services.discovery_executor import execute_intent

        return await execute_intent(
            self.__relational_context,
            identity,
            board_id,
            intent,
            params,
        )

    async def get_application_record(
        self,
        *,
        entity: str,
        record_id: str,
        includes: tuple[str, ...] = (),
    ):  # noqa: ANN201
        from okto_pulse.core.ports.application_persistence import (
            get_application_persistence_port,
        )

        return await get_application_persistence_port().get(
            self.__relational_context,
            entity=entity,
            record_id=record_id,
            includes=includes,
        )

    async def list_application_records(self, query):  # noqa: ANN001, ANN201
        from okto_pulse.core.ports.application_persistence import (
            get_application_persistence_port,
        )

        return await get_application_persistence_port().list(
            self.__relational_context,
            query,
        )

    async def ideation_skip_overrides(
        self, ideation: object, board_id: str
    ) -> list[dict[str, object]]:
        from okto_pulse.core.services.skip_overrides import ideation_skip_overrides

        return await ideation_skip_overrides(
            self.__relational_context,
            ideation,
            board_id,
        )

    async def spec_skip_overrides(
        self, spec: object, board_id: str
    ) -> list[dict[str, object]]:
        from okto_pulse.core.services.skip_overrides import spec_skip_overrides

        return await spec_skip_overrides(
            self.__relational_context,
            spec,
            board_id,
        )

    async def build_traceability_report(
        self,
        board_id: str,
        *,
        ideation_id: str = "",
        spec_id: str = "",
        include_artifacts: bool = True,
    ) -> dict[str, object]:
        from okto_pulse.core.services.traceability import build_traceability_report

        return await build_traceability_report(
            self.__relational_context,
            board_id,
            ideation_id=ideation_id,
            spec_id=spec_id,
            include_artifacts=include_artifacts,
        )

    async def get_default_board_template(self, template_id: str):  # noqa: ANN201
        from okto_pulse.core.ports.default_board_configuration import (
            get_default_board_configuration_store,
        )

        return await get_default_board_configuration_store().get_template(
            self.__relational_context,
            template_id=template_id,
        )

    async def resolve_active_default_board_template(self, scope: str):  # noqa: ANN201
        from okto_pulse.core.ports.default_board_configuration import (
            get_default_board_configuration_store,
        )

        return await get_default_board_configuration_store().resolve_active(
            self.__relational_context,
            scope=scope,
        )

    async def read_latest_kg_tick_completed_at(self):  # noqa: ANN201
        from okto_pulse.core.ports.relational_effects import (
            get_relational_effects_port,
        )

        return await get_relational_effects_port().read_latest_kg_tick_completed_at(
            self.__relational_context
        )

    async def list_board_ids(self) -> list[str]:
        from okto_pulse.core.ports.relational_effects import (
            get_relational_effects_port,
        )

        rows = await get_relational_effects_port().list_board_ids(
            self.__relational_context
        )
        return list(rows)

    async def publish_kg_tick_events(self, *, scheduled_at: str) -> list[str]:
        from okto_pulse.core.events.handlers.kg_decay_tick import (
            publish_tick_events,
        )

        return await publish_tick_events(
            self.__relational_context,
            scheduled_at=scheduled_at,
        )

    async def publish_domain_event(self, event: object) -> None:
        from okto_pulse.core.events import publish

        await publish(event, session=self.__relational_context)


def build_application_service_catalog(
    relational_context: object,
) -> CoreApplicationServiceCatalog:
    return CoreApplicationServiceCatalog(relational_context)


__all__ = [
    "CoreAnalyticsOperations",
    "CoreApplicationServiceCatalog",
    "build_application_service_catalog",
]
