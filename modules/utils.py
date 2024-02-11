import random
import re


def normalize_string(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s


def pick_random_items(lst, num_items):
    print(f"Picking {num_items} random items from a list of {len(lst)}")
    return random.sample(lst, num_items)
