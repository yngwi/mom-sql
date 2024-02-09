import re


def normalize_string(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s
