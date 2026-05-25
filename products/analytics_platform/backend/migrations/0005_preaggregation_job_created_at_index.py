from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import migrations, models


class Migration(migrations.Migration):
    # Index supports the eager precompute team-selection scan in
    # `web_analytics_eager_from_lazy_usage` (filters on created_at >= now - lookback).
    atomic = False  # Required for AddIndexConcurrently

    dependencies = [
        ("analytics_platform", "0004_unique_pending_job_index"),
    ]

    operations = [
        AddIndexConcurrently(
            model_name="preaggregationjob",
            index=models.Index(fields=["created_at"], name="analytics_p_created_e9c8a4_idx"),
        ),
    ]
