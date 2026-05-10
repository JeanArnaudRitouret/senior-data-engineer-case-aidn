"""GitHub PR reviewer — reads a unified diff from stdin, calls DeepSeek, posts a review comment."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Literal, TypeAlias

import httpx
from pydantic import BaseModel

_API_URL = "https://api.deepseek.com/v1/chat/completions"
_MODEL = "deepseek-reasoner"
_PROMPT_PATH = Path(__file__).parent.parent / "prompts" / "pr-review.md"
_MAX_LINES = 20_000
_MAX_BYTES = 500 * 1024

Severity: TypeAlias = Literal["blocking", "suggestion", "question"]


class ReviewIssue(BaseModel):
    """One flagged issue from the diff review.

    Attributes:
        file: File path from the diff header.
        line: Post-change line number; null for file-level issues.
        severity: blocking, suggestion, or question.
        message: Imperative description of the problem and fix.
    """

    file: str
    line: int | None
    severity: Severity
    message: str


class Review(BaseModel):
    """Structured review returned by the DeepSeek model.

    Attributes:
        summary: Headline summary of the diff and its risk.
        issues: Individual issues found in the added lines.
    """

    summary: str
    issues: list[ReviewIssue]


class _ApiMessage(BaseModel):
    content: str


class _ApiChoice(BaseModel):
    message: _ApiMessage


class _ApiResponse(BaseModel):
    choices: list[_ApiChoice]


def _read_diff() -> str | None:
    """Read unified diff from stdin; return None if it exceeds the size guard."""
    raw = sys.stdin.buffer.read()
    if len(raw) > _MAX_BYTES or raw.count(b"\n") > _MAX_LINES:
        return None
    return raw.decode("utf-8", errors="replace")


def _read_prompt() -> str:
    """Return the system prompt from pr-review.md.

    Raises:
        FileNotFoundError: If the prompt file is missing.
    """
    return _PROMPT_PATH.read_text(encoding="utf-8")


def _call_api(prompt: str, diff: str, api_key: str) -> Review:
    """Call the DeepSeek API and return a validated Review.

    Args:
        prompt: System prompt from pr-review.md.
        diff: Unified diff text.
        api_key: DeepSeek Bearer token.

    Returns:
        Validated Review parsed from the model's JSON output.

    Raises:
        httpx.HTTPStatusError: On non-2xx HTTP response.
        ValueError: If the API response has unexpected shape.
        pydantic.ValidationError: If model output does not match the Review schema.
    """
    response = httpx.post(
        _API_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": _MODEL,
            "temperature": 0.0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"<diff>\n{diff}\n</diff>"},
            ],
        },
        timeout=120.0,
    )
    response.raise_for_status()
    api_resp = _ApiResponse.model_validate(response.json())
    if not api_resp.choices:
        raise ValueError(f"API returned empty choices: {response.text[:300]}")
    return Review.model_validate_json(api_resp.choices[0].message.content)


def _format_comment(review: Review) -> str:
    """Render a Review as a Markdown GitHub comment."""
    lines = ["## DeepSeek Review", "", review.summary]
    sections: list[tuple[str, list[ReviewIssue]]] = [
        ("🔴 Blocking", [i for i in review.issues if i.severity == "blocking"]),
        ("🟡 Suggestions", [i for i in review.issues if i.severity == "suggestion"]),
        ("❓ Questions", [i for i in review.issues if i.severity == "question"]),
    ]
    for title, items in sections:
        if not items:
            continue
        lines += ["", f"### {title}"]
        for issue in items:
            loc = f"`{issue.file}`" + (f" L{issue.line}" if issue.line else "")
            lines.append(f"- {loc} — {issue.message}")
    return "\n".join(lines)


def _post_comment(body: str, pr_number: str, repo: str) -> None:
    """Post a comment on a GitHub PR via the gh CLI.

    Raises:
        subprocess.CalledProcessError: If gh exits non-zero.
    """
    subprocess.run(
        ["gh", "pr", "comment", pr_number, "--repo", repo, "--body", body],
        check=True,
        stdout=subprocess.DEVNULL,
        text=True,
    )


def main() -> None:
    """Orchestrate diff ingestion, model review, and PR comment posting.

    Reads unified diff from stdin. Exits 1 on failure; exits 0 if the diff
    exceeds the size guard (a notice comment is posted instead).

    Required environment variables: DEEPSEEK_API_KEY, PR_NUMBER, REPO.
    """
    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    pr_number = os.environ.get("PR_NUMBER", "")
    repo = os.environ.get("REPO", "")

    pairs: list[tuple[str, str]] = [
        ("DEEPSEEK_API_KEY", api_key),
        ("PR_NUMBER", pr_number),
        ("REPO", repo),
    ]
    missing = [k for k, v in pairs if not v]
    if missing:
        print(f"ERROR: missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    diff = _read_diff()
    if diff is None:
        _post_comment(
            "## DeepSeek Review\n\n"
            "> Diff exceeds size limit (20 000 lines / 500 KB). Review skipped.",
            pr_number,
            repo,
        )
        sys.exit(0)

    if not diff.strip():
        print("ERROR: empty diff on stdin", file=sys.stderr)
        sys.exit(1)

    try:
        review = _call_api(_read_prompt(), diff, api_key)
        _post_comment(_format_comment(review), pr_number, repo)
    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
