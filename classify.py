from typing import Optional

ROLE_MAP: dict[str, list[str]] = {
    "ml_engineer": ["machine learning engineer", "ml engineer", "mlops", "ml platform"],
    "ai_engineer": [
        "ai engineer",
        "artificial intelligence engineer",
        "llm engineer",
        "genai",
        "gen ai",
    ],
    "data_scientist": [
        "data scientist",
        "applied scientist",
        "quantitative analyst",
        "quant researcher",
    ],
    "data_analyst": [
        "data analyst",
        "business analyst",
        "analytics analyst",
        "bi analyst",
        "business intelligence",
    ],
    "data_engineer": [
        "data engineer",
        "etl engineer",
        "analytics engineer",
        "data platform",
        "data infrastructure",
    ],
    "research_scientist": [
        "research scientist",
        "research engineer",
        "nlp researcher",
        "cv researcher",
        "applied researcher",
    ],
}


def classify_role_keyword(raw_title: str) -> Optional[str]:
    """Fast keyword-based classifier. Returns role key or None."""
    normalised = raw_title.lower()
    for role, keywords in ROLE_MAP.items():
        if any(kw in normalised for kw in keywords):
            return role
    return None


#
# --  Ollama requests should be batched to be run async (saves load/unload operation)
#
def classify_role_ollama(raw_title: str) -> str:
    """Fallback: ask local Ollama to classify. Returns role key or 'other'."""
    import requests

    categories = list(ROLE_MAP.keys()) + ["other"]
    prompt = (
        f"Classify this job title into exactly one category.\n"
        f'Title: "{raw_title}"\n'
        f"Categories: {', '.join(categories)}\n"
        f"Reply with only the category name, nothing else."
    )
    resp = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": "qwen2.5:9b", "prompt": prompt, "stream": False, "think": False},
        timeout=15,
    )
    result = resp.json().get("response", "other").strip().lower()
    return result if result in categories else "other"


def classify_role(raw_title: str) -> str:
    return classify_role_keyword(raw_title) or classify_role_ollama(raw_title)
