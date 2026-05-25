from django.db import models

from posthog.models.utils import CreatedMetaFields, UUIDModel


class PreaggregationJob(CreatedMetaFields, UUIDModel):
    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        READY = "ready", "Ready"
        STALE = "stale", "Stale"
        FAILED = "failed", "Failed"

    team = models.ForeignKey("posthog.Team", on_delete=models.CASCADE, editable=False)

    # Time range this job covers
    time_range_start = models.DateTimeField()
    time_range_end = models.DateTimeField()

    # Normalized query representation for matching
    query_hash = models.CharField(max_length=64)  # SHA256 hash for quick lookup

    # Status tracking
    status = models.CharField(max_length=20, choices=Status, default=Status.PENDING)
    computed_at = models.DateTimeField(null=True, blank=True)

    # TTL: when the preaggregated data expires in ClickHouse
    # Jobs with expires_at in the past should not be used
    expires_at = models.DateTimeField(null=True, blank=True)

    # Timestamps (created_at from CreatedMetaFields, created_by also included)
    updated_at = models.DateTimeField(auto_now=True)

    # Error tracking
    error = models.TextField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=["team_id", "query_hash"]),
            models.Index(fields=["team_id", "status"]),
            models.Index(fields=["team_id", "time_range_start", "time_range_end"]),
            models.Index(fields=["team_id", "expires_at"]),
            # Supports the eager precompute team-selection query in
            # `web_analytics_eager_from_lazy_usage`, which scans the last N
            # days of jobs grouped by team. Without this index the scan
            # degrades into a full table read once the table grows large.
            models.Index(fields=["created_at"]),
        ]

        constraints = [
            models.CheckConstraint(
                condition=models.Q(time_range_start__lt=models.F("time_range_end")),
                name="time_range_start_before_end",
            ),
        ]
