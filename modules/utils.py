import random
import re


def normalize_string(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def pick_random_items(lst, num_items):
    print(f"Picking {num_items} random items from a list of {len(lst)}")
    return random.sample(lst, num_items)


def join_url_parts(*parts: str) -> str:
    # Ensure the first part ends with a slash if it's not the only part
    if len(parts) > 1 and not parts[0].endswith("/"):
        parts = (parts[0] + "/",) + parts[1:]

    # Remove any leading or trailing slashes from intermediate parts
    sanitized_parts = (
        [parts[0].rstrip("/")]
        + [part.strip("/") for part in parts[1:-1]]
        + [parts[-1].lstrip("/")]
    )

    # Join the parts with a single slash between them
    full_url = "/".join(sanitized_parts)
    return full_url
