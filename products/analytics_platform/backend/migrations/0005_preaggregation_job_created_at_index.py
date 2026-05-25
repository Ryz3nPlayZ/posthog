from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import migrations, models


class Migration(migrations.Migration):
    # Index supports the eager precompute team-selection scan in
    # `_select_eager_teams` (filters on created_at >= now - lookback).
    # Index name is Django's autogen format (`<table[:11]>_<col[:7]>_<hash6>_idx`)
    # — keep this in sync with `PreaggregationJob.Meta.indexes`.
    atomic = False  # Required for AddIndexConcurrently

    dependencies = [
        ("analytics_platform", "0004_unique_pending_job_index"),
    ]

    operations = [
        AddIndexConcurrently(
            model_name="preaggregationjob",
            index=models.Index(fields=["created_at"], name="analytics_p_created_2416b7_idx"),
        ),
    ]
