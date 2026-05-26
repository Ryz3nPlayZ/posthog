import pytest
from unittest.mock import MagicMock

from ee.tasks.subscriptions.ai_subscription.delivery import (
    SLACK_MRKDWN_SECTION_LIMIT,
    _build_ai_slack_message,
    _split_into_slack_sections,
    render_ai_email_html,
)


class TestSplitIntoSlackSections:
    def test_short_text_is_single_chunk(self) -> None:
        assert _split_into_slack_sections("short report") == ["short report"]

    def test_empty_text_yields_no_chunks(self) -> None:
        assert _split_into_slack_sections("") == []

    @pytest.mark.parametrize("prefix", ["\n\n", "\n", "  \n\n  "])
    def test_leading_blank_lines_do_not_emit_empty_chunk(self, prefix: str) -> None:
        # Regression: a body starting on a paragraph boundary used to carve off an empty first chunk,
        # which Slack rejects as an empty section block.
        text = prefix + ("a" * (SLACK_MRKDWN_SECTION_LIMIT + 100))
        chunks = _split_into_slack_sections(text)
        assert chunks, "expected at least one chunk"
        assert all(chunk.strip() for chunk in chunks), "no chunk may be empty/whitespace"

    def test_no_newlines_falls_back_to_hard_cut(self) -> None:
        text = "x" * (SLACK_MRKDWN_SECTION_LIMIT * 2 + 50)
        chunks = _split_into_slack_sections(text)
        assert len(chunks) >= 3
        assert all(len(c) <= SLACK_MRKDWN_SECTION_LIMIT for c in chunks)
        assert "".join(chunks) == text

    def test_breaks_on_paragraph_boundary(self) -> None:
        para = "a" * (SLACK_MRKDWN_SECTION_LIMIT - 100)
        chunks = _split_into_slack_sections(f"{para}\n\n{para}")
        assert len(chunks) == 2
        assert chunks[0] == para
        assert chunks[1] == para


class TestRenderAiEmailHtml:
    def test_neutralizes_raw_html_but_keeps_tables(self) -> None:
        html = render_ai_email_html("## Heading\n\n<script>alert(1)</script>\n\n| a | b |\n|---|---|\n| 1 | 2 |")
        # Raw HTML in the markdown source is escaped to inert text (html=False), never a live tag.
        assert "<script>" not in html
        assert "&lt;script&gt;" in html
        # Legitimate markdown structure (headings, tables) still renders.
        assert "<table>" in html
        assert "<h2>" in html

    def test_renders_basic_markdown(self) -> None:
        html = render_ai_email_html("**bold** and *italic*")
        assert "<strong>bold</strong>" in html
        assert "<em>italic</em>" in html


def _mock_subscription() -> MagicMock:
    sub = MagicMock()
    sub.target_value = "C123|#general"
    sub.title = "Weekly report"
    sub.url = "https://app.posthog.com/project/1/subscriptions/2"
    sub.team_id = 1
    sub.id = 2
    return sub


class TestBuildAiSlackMessage:
    def test_single_section_report_has_no_thread_messages(self) -> None:
        message = _build_ai_slack_message(_mock_subscription(), "A short report.")
        assert message.channel == "C123"
        assert message.thread_messages == []
        # title block + body block + divider + actions (no "see thread" block).
        section_texts = [b["text"]["text"] for b in message.blocks if b["type"] == "section"]
        assert all(text.strip() for text in section_texts), "no empty section text allowed"

    def test_long_report_overflows_into_thread(self) -> None:
        long_markdown = ("para\n\n" * 1).join("x" * (SLACK_MRKDWN_SECTION_LIMIT - 50) for _ in range(3))
        message = _build_ai_slack_message(_mock_subscription(), long_markdown)
        assert len(message.thread_messages) >= 1
        for thread_msg in message.thread_messages:
            for block in thread_msg["blocks"]:
                assert block["text"]["text"].strip(), "thread section text must be non-empty"
