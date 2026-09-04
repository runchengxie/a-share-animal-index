from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_daily_workflow_allows_manual_full_backfill() -> None:
    workflow = (ROOT / ".github/workflows/daily.yml").read_text(encoding="utf-8")

    assert "backfill_mode:" in workflow
    assert "default: missing" in workflow
    assert "backfill_days:" in workflow
    assert 'default: "1"' in workflow
    assert "BACKFILL_MODE: ${{ inputs.backfill_mode || 'missing' }}" in workflow
    assert 'BACKFILL_DAYS: ${{ inputs.backfill_days || \'1\' }}' in workflow
    assert '--backfill "$BACKFILL_DAYS" --backfill-mode "$BACKFILL_MODE"' in workflow
