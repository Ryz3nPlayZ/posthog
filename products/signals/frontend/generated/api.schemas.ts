/**
 * Auto-generated from the Django backend OpenAPI schema.
 * To modify these types, update the Django serializers or views, then run:
 *   hogli build:openapi
 * Questions or issues? #team-devex on Slack
 *
 * PostHog API - generated
 * OpenAPI spec version: 1.0.0
 */
export interface PauseStateResponseApi {
    /**
     * The timestamp the pipeline is paused until, or null if not paused/not running.
     * @nullable
     */
    paused_until: string | null
}

export interface PaginatedPauseStateResponseListApi {
    count: number
    /** @nullable */
    next?: string | null
    /** @nullable */
    previous?: string | null
    results: PauseStateResponseApi[]
}

export interface PauseUntilRequestApi {
    /** Pause the grouping pipeline until this timestamp (ISO 8601). */
    timestamp: string
}

export interface PauseResponseApi {
    /** Always 'paused'. */
    status: string
    /** The timestamp the pipeline is paused until. */
    paused_until: string
}

/**
 * * `potential` - Potential
 * `candidate` - Candidate
 * `in_progress` - In Progress
 * `pending_input` - Pending Input
 * `ready` - Ready
 * `resolved` - Resolved
 * `failed` - Failed
 * `deleted` - Deleted
 * `suppressed` - Suppressed
 */
export type SignalReportStatusEnumApi = (typeof SignalReportStatusEnumApi)[keyof typeof SignalReportStatusEnumApi]

export const SignalReportStatusEnumApi = {
    Potential: 'potential',
    Candidate: 'candidate',
    InProgress: 'in_progress',
    PendingInput: 'pending_input',
    Ready: 'ready',
    Resolved: 'resolved',
    Failed: 'failed',
    Deleted: 'deleted',
    Suppressed: 'suppressed',
} as const

export interface SignalReportApi {
    readonly id: string
    /** @nullable */
    readonly title: string | null
    /** @nullable */
    readonly summary: string | null
    readonly status: SignalReportStatusEnumApi
    readonly total_weight: number
    readonly signal_count: number
    readonly signals_at_run: number
    readonly created_at: string
    readonly updated_at: string
    readonly artefact_count: number
    /**
     * P0–P4 from the latest priority judgment artefact (when present).
     * @nullable
     */
    readonly priority: string | null
    /**
     * Actionability choice from the latest actionability judgment artefact (when present).
     * @nullable
     */
    readonly actionability: string | null
    /**
     * Whether the issue appears already fixed, from the actionability judgment artefact.
     * @nullable
     */
    readonly already_addressed: boolean | null
    readonly is_suggested_reviewer: boolean
    /** Distinct source products contributing signals to this report (from ClickHouse). */
    readonly source_products: readonly string[]
    /**
     * PR URL from the latest implementation task run, if available.
     * @nullable
     */
    readonly implementation_pr_url: string | null
}

export interface PaginatedSignalReportListApi {
    count: number
    /** @nullable */
    next?: string | null
    /** @nullable */
    previous?: string | null
    results: SignalReportApi[]
}

/**
 * `inventory.project_context` — free-form orientation about the project's product.
 */
export interface ProjectContextApi {
    /**
     * Human-set product description on the project (max 1000 chars). When present, the most direct "what does this team's product do" answer. `null` when unset.
     * @nullable
     */
    product_description: string | null
    /** Registered app URLs for this team (toolbar / replay). The team's actual product surface; complements `$pageview.$host` discovery via `read-data-schema`. */
    app_urls: string[]
}

/**
 * One row in `inventory.product_intents`.
 */
export interface ProductIntentEntryApi {
    /** Product key the team signaled intent to use. */
    product_type: string
    /**
     * ISO-8601 timestamp the team activated the product, or null if intent only.
     * @nullable
     */
    activated_at: string | null
    /**
     * ISO-8601 timestamp the intent was first recorded.
     * @nullable
     */
    created_at: string | null
}

/**
 * One row in `inventory.integrations`. Sensitive config is intentionally excluded.
 */
export interface IntegrationEntryApi {
    /** Integration kind (e.g. `slack`, `github`, `linear`). */
    kind: string
    /**
     * ISO-8601 timestamp the integration was connected.
     * @nullable
     */
    created_at: string | null
}

/**
 * One row in `inventory.external_data_sources`.
 */
export interface ExternalDataSourceEntryApi {
    /** Warehouse source type (e.g. `Stripe`, `Postgres`, `BigQuery`). */
    source_type: string
    /** Current sync status (`Running`, `Failed`, `Paused`, etc.). */
    status: string
    /** Schema prefix used by this source, if any. */
    prefix: string
    /**
     * ISO-8601 timestamp the source was connected.
     * @nullable
     */
    created_at: string | null
}

/**
 * One row in either bucket of `inventory.signal_source_configs`.
 */
export interface SignalSourceConfigEntryApi {
    /** Source product the config applies to. */
    source_product: string
    /** Source type within the product. */
    source_type: string
}

/**
 * `inventory.signal_source_configs` split into enabled and disabled buckets.
 */
export interface SignalSourceConfigsBucketsApi {
    /** Source configs the team has explicitly enabled. */
    enabled: SignalSourceConfigEntryApi[]
    /** Source configs the team has explicitly disabled (different from never wired up). */
    disabled: SignalSourceConfigEntryApi[]
}

/**
 * One bucket in `inventory.existing_inbox_reports.by_status`.
 */
export interface InboxReportStatusBucketApi {
    /** Report status (e.g. `potential`, `candidate`, `ready`). */
    status: string
    /** Number of reports in this status (excludes deleted/suppressed). */
    count: number
}

/**
 * `inventory.existing_inbox_reports` — what's already been surfaced to the inbox.
 */
export interface ExistingInboxReportsApi {
    /** Total non-deleted, non-suppressed reports for this team. */
    total: number
    /** Per-status breakdown of inbox reports. */
    by_status: InboxReportStatusBucketApi[]
}

/**
 * One row in `inventory.recent_dashboards`.
 */
export interface RecentDashboardEntryApi {
    /** Dashboard ID — pass to `dashboard-get` to pull the full payload. */
    id: number
    /** Dashboard name (may be blank if unnamed). */
    name: string
    /**
     * ISO-8601 timestamp of the most recent view in the PostHog UI.
     * @nullable
     */
    last_accessed_at: string | null
    /**
     * ISO-8601 timestamp of the most recent data refresh. Distinct from access — a dashboard can be refreshed without anyone viewing it.
     * @nullable
     */
    last_refresh: string | null
    /**
     * ISO-8601 timestamp the dashboard was created.
     * @nullable
     */
    created_at: string | null
}

/**
 * One row in `inventory.top_events`.
 */
export interface TopEventEntryApi {
    /** Event name as captured. */
    event: string
    /** Number of occurrences in the lookback window (last 7 days). */
    count: number
    /** `uniq(person_id)` over the window — reach. Distinguishes a high-count event firing on one power user from one firing on many users. */
    distinct_users: number
    /** Count in just the last 24 hours. Compare to `count / 7` to spot bursts: a ratio well above 1/7 means the event is concentrated in the last day. */
    recent_24h_count: number
    /** `uniq(person_id)` over just the last 24 hours. A burst across many users is qualitatively different from one user in a loop. */
    recent_24h_users: number
    /**
     * ISO-8601 timestamp of the earliest occurrence within the lookback window. Compare to the window start to spot new event types: `first_seen` close to `now` ⇒ likely new or recently bursting; close to the window edge ⇒ has been around at least that long (the window can't tell you when the event *truly* first appeared).
     * @nullable
     */
    first_seen: string | null
    /**
     * ISO-8601 timestamp of the most recent occurrence within the lookback window.
     * @nullable
     */
    last_seen: string | null
}

/**
 * The deterministic inventory layer of a project profile.

Read this to orient on the team's product mix, integrations, warehouse sources, signal
coverage, and existing inbox surface in one tool call. Distinct from `SignalScratchpad`:
profile is ground truth from authoritative tables; memory is agent inference.
 */
export interface ProjectProfileInventoryApi {
    /** Free-form orientation: human-set product description + registered app URLs. */
    project_context: ProjectContextApi
    /** Product keys this team has completed onboarding for, sorted alphabetically. */
    products_in_use: string[]
    /** Products the team signaled intent to use; useful for spotting stuck onboardings. */
    product_intents: ProductIntentEntryApi[]
    /** Connected integrations (kind + connection time only — config never surfaced). */
    integrations: IntegrationEntryApi[]
    /** Connected warehouse sources (excludes soft-deleted). */
    external_data_sources: ExternalDataSourceEntryApi[]
    /** Signal source configs split into enabled / disabled buckets. */
    signal_source_configs: SignalSourceConfigsBucketsApi
    /** Counts of reports already in the inbox, grouped by status. */
    existing_inbox_reports: ExistingInboxReportsApi
    /** Per-scope counts off the activity log over the recent-activity window — cross-cutting orientation across every entity type (surveys, feature flags, experiments, dashboards, insights, cohorts, notebooks, actions, etc.). Each scope reports `edits` (total log entries), `users` (distinct user count), and `last_edit` (ISO-8601). Use to triage which scope a team has been working in lately before drilling down via the per-entity readers or `activity-log-list`. */
    recent_activity: unknown
    /** Up to 20 dashboards on this team sorted by `last_accessed_at` desc — what the team is currently looking at, not necessarily the most-trafficked. We don't have per-dashboard view counts in Postgres, only the timestamp of the most recent access. */
    recent_dashboards: RecentDashboardEntryApi[]
    /** Surveys orientation: `{total_count, active_count, recent: [...]}` where `recent` is the 5 most recently updated surveys with `id`, `name`, `type`, `status` (draft / running / stopped / archived), and `updated_at`. */
    recent_surveys: unknown
    /** Feature flag orientation: `{total_count, active_count, recent: [...]}` where `recent` is the 5 most recently updated non-deleted flags with `id`, `key`, `name`, `active`, and `updated_at`. */
    recent_feature_flags: unknown
    /** Experiment orientation: `{total_count, active_count, recent: [...]}`. The feature_flag key on each row lets the scout correlate experiments with the `recent_feature_flags` section. */
    recent_experiments: unknown
    /** Alert orientation: `{total_count, active_count, recent: [...]}` covering the 5 most recently updated alerts with their state and threshold metadata. */
    recent_alerts: unknown
    /** Hog function orientation: `{total_count, active_count, recent: [...]}` for destinations / transformations the team has wired up via the CDP pipelines. */
    recent_hog_functions: unknown
    /** Hog flow orientation: `{total_count, active_count, recent: [...]}` for the team's currently configured automation flows. */
    recent_hog_flows: unknown
    /** Notebook orientation: `{total_count, recent: [...]}` with the 5 most recently updated notebooks — useful signal for what the team has been investigating. */
    recent_notebooks: unknown
    /** Cohort orientation: `{total_count, recent: [...]}` with the 5 most recently updated cohorts on the team. */
    recent_cohorts: unknown
    /** Action orientation: `{total_count, recent: [...]}` with the 5 most recently updated actions — useful to anchor agent reasoning about what the team treats as a meaningful interaction. */
    recent_actions: unknown
    /**
     * Top ~50 events by count over the last 7 days, with first/last seen timestamps within the window. `null` if the underlying ClickHouse query failed or timed out (distinct from `[]`, which means the team has no captures in the window). Use the gap between `first_seen` and `now` to spot new event types or recent bursts.
     * @nullable
     */
    top_events: TopEventEntryApi[] | null
}

/**
 * Top-level `payload` shape on a `SignalProjectProfile` row.

v1 carries `inventory` only. Phase 7 will add `deltas`, `activity_notes`, and
`narrative` slots — they're absent (not null) in v1 responses.
 */
export interface ProjectProfilePayloadApi {
    /** Deterministic snapshot of what's true about the project. */
    inventory: ProjectProfileInventoryApi
}

/**
 * Wire shape for the project profile returned by `signals-scout-harness-project-profile-list`.

Read this once at the start of a run (after `skill-get`) to orient on the team. Cache
is per-team with a soft TTL (`PROFILE_TTL`); the response always reflects either the
latest cached profile or a freshly-built one if the cache was stale or the caller passed
`force_refresh=true`.
 */
export interface ProjectProfileApi {
    /** UUID of the `SignalProjectProfile` row. */
    profile_id: string
    /** ISO-8601 timestamp the profile was built. */
    computed_at: string
    /** ISO-8601 timestamp after which the profile is considered stale. */
    expires_at: string
    /** Schema version of the inventory builder. Bumps invalidate older cached rows. */
    source_version: string
    /** Structured profile content. v1 has `inventory` only. */
    payload: ProjectProfilePayloadApi
}

/**
 * Lightweight projection of a `SignalScoutRun` row used by `search-recent-runs`.

Status and timestamps flow from the linked `tasks.TaskRun`.
 */
export interface SignalScoutRunSummaryApi {
    /** UUID of the bridge row. */
    run_id: string
    /** Canonical skill name the run executed (e.g. `signals-scout-general`). */
    skill_name: string
    /** Skill version snapshotted at run start. */
    skill_version: number
    /** Status from the linked TaskRun: not_started | queued | in_progress | completed | failed | cancelled. */
    status: string
    /** ISO-8601 timestamp the TaskRun was created. */
    started_at: string
    /**
     * ISO-8601 timestamp the TaskRun completed; null while still running.
     * @nullable
     */
    completed_at: string | null
    /**
     * UUID of the Tasks `Task` the scout span ran inside.
     * @nullable
     */
    task_id?: string | null
    /**
     * UUID of the Tasks `TaskRun`. Pairs with `task_id` to deep-link.
     * @nullable
     */
    task_run_id?: string | null
    /**
     * Relative deep-link to the Tasks UI for this run, e.g. `/project/{team_id}/tasks/{task_id}?runId={task_run_id}`.
     * @nullable
     */
    task_url?: string | null
}

/**
 * Full `SignalScoutRun` projection used by `get-run`. Same shape as the summary
post-refactor — the bridge row no longer holds structured payloads. Future
extensions (linked Signal rows, LLMA token-cost join) land here.
 */
export interface SignalScoutRunDetailApi {
    /** UUID of the bridge row. */
    run_id: string
    /** Canonical skill name the run executed (e.g. `signals-scout-general`). */
    skill_name: string
    /** Skill version snapshotted at run start. */
    skill_version: number
    /** Status from the linked TaskRun: not_started | queued | in_progress | completed | failed | cancelled. */
    status: string
    /** ISO-8601 timestamp the TaskRun was created. */
    started_at: string
    /**
     * ISO-8601 timestamp the TaskRun completed; null while still running.
     * @nullable
     */
    completed_at: string | null
    /**
     * UUID of the Tasks `Task` the scout span ran inside.
     * @nullable
     */
    task_id?: string | null
    /**
     * UUID of the Tasks `TaskRun`. Pairs with `task_id` to deep-link.
     * @nullable
     */
    task_run_id?: string | null
    /**
     * Relative deep-link to the Tasks UI for this run, e.g. `/project/{team_id}/tasks/{task_id}?runId={task_run_id}`.
     * @nullable
     */
    task_url?: string | null
}

/**
 * One citation attached to a finding. Mirrors `SignalsScoutEvidenceEntry`.
 */
export interface EvidenceEntryApi {
    /** Source the citation came from (`error_tracking`, `session_replay`, `logs`, ...). */
    source_product: string
    /** One-sentence prose about why this evidence supports the finding. */
    summary: string
    /**
     * Optional ID of the cited entity (issue id, recording id, log query id).
     * @nullable
     */
    entity_id?: string | null
}

export interface TimeRangeApi {
    /** ISO-8601 inclusive lower bound for the finding's window. */
    date_from: string
    /** ISO-8601 inclusive upper bound for the finding's window. */
    date_to: string
}

/**
 * Request body for `emit-finding`. Run attribution is taken from the URL path.
 */
export interface EmitFindingRequestApi {
    /** Canonical evidence-bundle prose. Becomes the signal's `description`. */
    description: string
    /**
     * Agent's weight for the signal in [0, 1]. Drives ranking in the inbox.
     * @minimum 0
     * @maximum 1
     */
    weight: number
    /**
     * Agent's confidence the finding is real in [0, 1]. Persisted in `extra`.
     * @minimum 0
     * @maximum 1
     */
    confidence: number
    /**
     * Citations supporting the finding. Capped at 20 entries.
     * @maxItems 20
     */
    evidence: EvidenceEntryApi[]
    /**
     * Optional one-line hypothesis the finding tests.
     * @nullable
     */
    hypothesis?: string | null
    /**
     * Optional severity tag (`P0`-`P4`) — informational only.
     * @nullable
     */
    severity?: string | null
    /** Optional keys for downstream dedupe (e.g. `error_tracking_issue:<id>`). */
    dedupe_keys?: string[]
    /** Optional time window the finding refers to. */
    time_range?: TimeRangeApi | null
    /**
     * Optional MCP trace id for cross-system debugging.
     * @nullable
     */
    mcp_trace_id?: string | null
    /**
     * Idempotency key. Re-using the same id within a run short-circuits without re-emitting.
     * @nullable
     */
    finding_id?: string | null
}

export interface EmitFindingResponseApi {
    /** Stable id for the finding (echoed back from request, or generated). */
    finding_id: string
    /** Whether `emit_signal` was actually fired. */
    emitted: boolean
    /**
     * `ai_processing_not_approved` | `source_disabled` | null when emitted normally.
     * @nullable
     */
    skipped_reason: string | null
}

/**
 * `SignalScratchpad` projection used by `search-memory` and `remember`.
 */
export interface ScratchpadEntryApi {
    /** Agent-chosen semantic key, unique per team. */
    key: string
    /** Prose content for prompt injection. */
    content: string
    /**
     * ISO-8601 creation timestamp.
     * @nullable
     */
    created_at: string | null
    /**
     * ISO-8601 last-write timestamp.
     * @nullable
     */
    updated_at: string | null
    /**
     * Run that wrote this entry, or null if human-authored.
     * @nullable
     */
    created_by_run_id: string | null
}

/**
 * Request body for `remember`.
 */
export interface RememberRequestApi {
    /**
     * Agent-chosen semantic key. Re-using a key updates the existing entry in place.
     * @maxLength 300
     */
    key: string
    /** Prose to write. Read verbatim into future prompts. */
    content: string
    /**
     * Run that authored this memory; persisted as `created_by_run_id` for lineage. Must reference a run on this same project — cross-project run UUIDs are rejected.
     * @nullable
     */
    run_id?: string | null
}

/**
 * Request body for `forget`.
 */
export interface ForgetRequestApi {
    /**
     * Memory key to delete.
     * @maxLength 300
     */
    key: string
}

export interface ForgetResponseApi {
    /** Whether a row was actually removed (false if the key didn't exist). */
    deleted: boolean
}

/**
 * * `session_replay` - Session replay
 * `llm_analytics` - LLM analytics
 * `github` - GitHub
 * `linear` - Linear
 * `zendesk` - Zendesk
 * `conversations` - Conversations
 * `error_tracking` - Error tracking
 * `pganalyze` - pganalyze
 * `signals_scout` - Signals scout
 */
export type SourceProductEnumApi = (typeof SourceProductEnumApi)[keyof typeof SourceProductEnumApi]

export const SourceProductEnumApi = {
    SessionReplay: 'session_replay',
    LlmAnalytics: 'llm_analytics',
    Github: 'github',
    Linear: 'linear',
    Zendesk: 'zendesk',
    Conversations: 'conversations',
    ErrorTracking: 'error_tracking',
    Pganalyze: 'pganalyze',
    SignalsScout: 'signals_scout',
} as const

/**
 * * `session_analysis_cluster` - Session analysis cluster
 * `evaluation` - Evaluation
 * `issue` - Issue
 * `ticket` - Ticket
 * `issue_created` - Issue created
 * `issue_reopened` - Issue reopened
 * `issue_spiking` - Issue spiking
 * `cross_source_issue` - Cross source issue
 */
export type SignalSourceConfigSourceTypeEnumApi =
    (typeof SignalSourceConfigSourceTypeEnumApi)[keyof typeof SignalSourceConfigSourceTypeEnumApi]

export const SignalSourceConfigSourceTypeEnumApi = {
    SessionAnalysisCluster: 'session_analysis_cluster',
    Evaluation: 'evaluation',
    Issue: 'issue',
    Ticket: 'ticket',
    IssueCreated: 'issue_created',
    IssueReopened: 'issue_reopened',
    IssueSpiking: 'issue_spiking',
    CrossSourceIssue: 'cross_source_issue',
} as const

export interface SignalSourceConfigApi {
    readonly id: string
    source_product: SourceProductEnumApi
    source_type: SignalSourceConfigSourceTypeEnumApi
    enabled?: boolean
    config?: unknown
    readonly created_at: string
    readonly updated_at: string
    /** @nullable */
    readonly status: string | null
}

export interface PaginatedSignalSourceConfigListApi {
    count: number
    /** @nullable */
    next?: string | null
    /** @nullable */
    previous?: string | null
    results: SignalSourceConfigApi[]
}

export interface PatchedSignalSourceConfigApi {
    readonly id?: string
    source_product?: SourceProductEnumApi
    source_type?: SignalSourceConfigSourceTypeEnumApi
    enabled?: boolean
    config?: unknown
    readonly created_at?: string
    readonly updated_at?: string
    /** @nullable */
    readonly status?: string | null
}

export interface _UserApi {
    readonly id: number
    readonly uuid: string
    readonly first_name: string
    readonly last_name: string
    readonly email: string
}

/**
 * * `P0` - P0
 * `P1` - P1
 * `P2` - P2
 * `P3` - P3
 * `P4` - P4
 */
export type AutostartPriorityEnumApi = (typeof AutostartPriorityEnumApi)[keyof typeof AutostartPriorityEnumApi]

export const AutostartPriorityEnumApi = {
    P0: 'P0',
    P1: 'P1',
    P2: 'P2',
    P3: 'P3',
    P4: 'P4',
} as const

export type BlankEnumApi = (typeof BlankEnumApi)[keyof typeof BlankEnumApi]

export const BlankEnumApi = {
    '': '',
} as const

export interface SignalUserAutonomyConfigApi {
    readonly id: string
    readonly user: _UserApi
    autostart_priority?: AutostartPriorityEnumApi | BlankEnumApi | null
    readonly created_at: string
    readonly updated_at: string
}

export type SignalsProcessingListParams = {
    /**
     * Number of results to return per page.
     */
    limit?: number
    /**
     * The initial index from which to return the results.
     */
    offset?: number
}

export type SignalsReportsListParams = {
    /**
     * Number of results to return per page.
     */
    limit?: number
    /**
     * The initial index from which to return the results.
     */
    offset?: number
    /**
     * Comma-separated ordering clauses. Each clause is a field name optionally prefixed with '-' for descending. Allowed fields: status, is_suggested_reviewer, signal_count, total_weight, priority, created_at, updated_at, id. Defaults to '-is_suggested_reviewer,status,-updated_at'.
     */
    ordering?: string
    /**
     * Case-insensitive substring match against report title and summary.
     */
    search?: string
    /**
     * Comma-separated list of source products to include. Reports are kept if at least one of their contributing signals comes from one of these products (e.g. error_tracking, session_replay).
     */
    source_product?: string
    /**
     * Comma-separated list of statuses to include. Valid values: potential, candidate, in_progress, pending_input, ready, failed, suppressed. Defaults to all statuses except suppressed.
     */
    status?: string
    /**
     * Comma-separated list of PostHog user UUIDs. Reports are kept if their suggested reviewers include any of the given users.
     */
    suggested_reviewers?: string
}

export type SignalsScoutProjectProfileGetParams = {
    /**
     * When true, skip the cache and rebuild the profile from authoritative sources before responding. Use after seeding events, importing data, or any other change the caller knows just landed but hasn't surfaced through natural cache expiry yet. Concurrent forced rebuilds are still serialized by the team-keyed advisory lock — at most one extra `build_inventory` per simultaneous request.
     */
    force_refresh?: boolean
}

export type SignalsScoutRunsListParams = {
    /**
     * Max rows to return (default 20, hard cap 100).
     * @minimum 1
     * @maximum 100
     */
    limit?: number
    /**
     * ISO-8601 lower bound on `created_at`. Use to scope to a recent window.
     */
    since?: string
}

export type SignalsScoutScratchpadListParams = {
    /**
     * Max rows to return (default 20, hard cap 100).
     * @minimum 1
     * @maximum 100
     */
    limit?: number
    /**
     * ILIKE substring match against `content`. Omit to return the most recent entries.
     */
    text?: string
}

export type SignalsSourceConfigsListParams = {
    /**
     * Number of results to return per page.
     */
    limit?: number
    /**
     * The initial index from which to return the results.
     */
    offset?: number
}
