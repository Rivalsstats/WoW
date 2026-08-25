"""LLM-written social captions (via OpenRouter) plus the sanitization and
validation that keeps model output usable, and the bundle builder combining
them with the static blog copy."""

import json
import random
import re
import time

import openai
from openai import OpenAI

from social_posts.static_copy import build_static_blog, build_static_title


def get_openai_client(api_key: str):
    return OpenAI(
        base_url="https://openrouter.ai/api/v1",
        api_key=api_key,
    )


# The blog copy is produced from static templates (build_static_blog); the
# language model only writes a single, humorous social post. That one text has
# to satisfy the tightest platform: Bluesky allows 300 characters and, unlike
# Twitter's t.co wrapping, counts the full URL, so we cap the text well below
# 300 once the link (worst case ~60 chars) and a separating space are appended.
SOCIAL_TEXT_MAX = 230

ANGLES = [
    "Angle for this post: celebrate the single most impressive number as a milestone.",
    "Angle for this post: contrast two of the facts against each other (top vs bottom, popular vs record).",
    "Angle for this post: ask the audience a question the data makes them want to answer.",
    "Angle for this post: spotlight the subject as if introducing it to a player who has never tried it.",
    "Angle for this post: point out the detail a veteran Mythic+ player would find surprising.",
]

SOCIAL_PROMPT_TEMPLATE = """You write a single social media post for MythiStone (mythistone.com), a World of Warcraft Mythic+ statistics site built on millions of real M+ runs. Today's subject: {post_type}.
An image visualizing the data accompanies the post, so the text should complement it, not describe it.

FACTS (the only information you may use):
{data}

{angle}

RULES:
- Copy names and numbers exactly as they appear in FACTS. Never invent, round, or recalculate a value.
- Lead with the most interesting insight; no greetings, no filler, no "click here" begging.
- Land some humor: a light pun or playful jab is welcome, but keep it grounded in the data and never force it.
- Plain text only: no emojis, no markdown, no em-dashes.
- At most {max_chars} characters, ending with 2-3 hashtags such as #WoW #MythicPlus.
- Do not include any URL; the link is appended separately.

Respond with ONLY the post text: no quotes, no JSON, no code fences, no commentary.
"""


_UNICODE_REPLACEMENTS = {
    "—": "-",
    "–": "-",
    "‘": "'",
    "’": "'",
    "“": '"',
    "”": '"',
    "…": "...",
    " ": " ",
}


def sanitize_text(text):
    """Normalize fancy punctuation and strip emojis/symbols; keep newlines."""
    for src, dst in _UNICODE_REPLACEMENTS.items():
        text = text.replace(src, dst)
    text = "".join(ch for ch in text if ch == "\n" or 32 <= ord(ch) < 0x2500)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" ?\n ?", "\n", text)
    return text.strip()


def clean_social_response(raw):
    """Strip code fences and wrapping quotes from a plain-text model response."""
    raw = (raw or "").strip()
    raw = re.sub(r"^```(?:\w+)?\s*|\s*```$", "", raw).strip()
    # models sometimes wrap the whole post in matching quotes
    if len(raw) >= 2 and raw[0] in "\"'" and raw[-1] == raw[0]:
        raw = raw[1:-1].strip()
    return raw


def _digit_runs(text):
    return set(re.findall(r"\d+", text))


def validate_social_text(text, facts_text):
    """Return a list of problems; empty list means the social text is usable."""
    if not isinstance(text, str) or not text.strip():
        return ["empty social text"]
    problems = []
    # Every multi-digit number in the output must literally appear in the
    # facts; this rejects invented/garbled stats. Single digits are allowed
    # (e.g. "top 5") since they are harmless and often legitimate phrasing.
    allowed = _digit_runs(facts_text)
    unknown = {n for n in _digit_runs(text) if len(n) > 1 and n not in allowed}
    if unknown:
        problems.append(f"invents numbers not in the data: {sorted(unknown)}")
    if len(text) > SOCIAL_TEXT_MAX:
        problems.append(f"too long ({len(text)} > {SOCIAL_TEXT_MAX})")
    return problems

MODELS = [
    "z-ai/glm-5.2:free",
    "liquid/lfm-2.5-2.6b:free"
]


def generate_social_text(client, data, subject, max_retries=5):
    """Generate a single humorous social post from the data.

    Tries every model in MODELS per attempt; a model's output only counts if it
    survives validate_social_text (no invented numbers, length limit respected).
    Returns the post text WITHOUT the link; callers append the link themselves.
    """
    facts_text = json.dumps(data, ensure_ascii=False)

    for attempt in range(1, max_retries + 1):
        any_model_succeeded = False
        any_model_rate_limited = False
        prompt = SOCIAL_PROMPT_TEMPLATE.format(
            post_type=subject,
            data=facts_text,
            angle=random.choice(ANGLES),
            max_chars=SOCIAL_TEXT_MAX,
        ).strip()

        for model in MODELS:
            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                )
                any_model_succeeded = True
                raw = resp.choices[0].message.content or ""

                text = sanitize_text(clean_social_response(raw))
                problems = validate_social_text(text, facts_text)
                if problems:
                    print(
                        f"[Attempt {attempt}] Model {model} rejected: {'; '.join(problems)}. Trying next model..."
                    )
                    continue

                return text

            except openai.RateLimitError as e:
                any_model_rate_limited = True
                print(
                    f"[Attempt {attempt}] Model {model} rate-limited: {e}. Trying next model..."
                )
                continue

            except Exception as e:
                # other errors: log and try next model
                print(
                    f"[Attempt {attempt}] Model {model} failed: {e}. Trying next model..."
                )
                continue

        # If we never got to try any model successfully and all were rate-limited, bail early
        if not any_model_succeeded and any_model_rate_limited:
            raise RuntimeError("All models are rate-limited upstream")

        time.sleep(0.5)  # small backoff and retry
    raise RuntimeError(
        f"Failed to generate a valid social post in {max_retries} attempts (validation errors or rate limits)."
    )


def build_bundle(client, data, link, post_type, subject):
    """Build the stored text bundle: static title + static blog + one social post.

    The social post is written by a model and has the link appended; the title
    and blog copy are generated from fixed templates (no model involved).
    """
    social = generate_social_text(client, data, subject)
    return {
        "title": build_static_title(post_type, data),
        "blog": build_static_blog(post_type, data),
        "social": f"{social} {link}".strip(),
    }
