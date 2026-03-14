from __future__ import annotations


def count_keyword_hits(texts: list[str], keywords: list[str]) -> int:
    joined = "\n".join(t.lower() for t in texts if t)
    return sum(joined.count(keyword.lower()) for keyword in keywords)


def extract_signal_counts(
    titles: list[str],
    descriptions: list[str],
    comments: list[str],
    keyword_dictionaries: dict[str, list[str]],
) -> dict[str, int]:
    body = titles + descriptions
    comment_body = comments
    higgsfield_specific = keyword_dictionaries.get("higgsfield_specific_fit", {})
    higgsfield_specific_count = 0
    if isinstance(higgsfield_specific, dict):
        for keywords in higgsfield_specific.values():
            higgsfield_specific_count += count_keyword_hits(body, keywords)

    tool_keywords = keyword_dictionaries.get(
        "generic_tool_relevance",
        keyword_dictionaries.get("tool_relevance", []),
    )

    return {
        "audience_signal_count": count_keyword_hits(
            body, keyword_dictionaries.get("audience_fit", [])
        ),
        "monetization_signal_count": count_keyword_hits(
            body, keyword_dictionaries.get("monetization", [])
        ),
        "workflow_signal_count": count_keyword_hits(
            body, keyword_dictionaries.get("workflow", [])
        ),
        "tool_signal_count": count_keyword_hits(body, tool_keywords),
        "higgsfield_specific_signal_count": higgsfield_specific_count,
        "premium_aesthetic_signal_count": count_keyword_hits(
            body, keyword_dictionaries.get("premium_aesthetic_signals", [])
        ),
        "comment_tool_intent_count": count_keyword_hits(
            comment_body, keyword_dictionaries.get("tool_buying_intent_comments", [])
        ),
        "creative_replication_intent_count": count_keyword_hits(
            comment_body,
            keyword_dictionaries.get("creative_replication_intent_comments", []),
        ),
        "comment_creator_intent_count": count_keyword_hits(
            comment_body, keyword_dictionaries.get("creator_intent_comments", [])
        ),
        "comment_business_intent_count": count_keyword_hits(
            comment_body, keyword_dictionaries.get("business_intent_comments", [])
        ),
    }
