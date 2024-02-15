import random
import re
from datetime import datetime, timedelta, timezone

from dateutil import tz


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


def parse_date(date_string):
    # Pattern that includes optional milliseconds and handles both 'Z' and offset-based timezones
    pattern = r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?([+-]\d{2}:?\d{2}|Z)"
    match = re.match(pattern, date_string)
    if match:
        parts = match.groups()
        # Extract parts
        year, month, day, hour, minute, second = map(int, parts[:6])
        # Convert milliseconds to microseconds, if present
        microsecond = int(
            (parts[6] or "0").ljust(3, "0")
        )  # Pad with zeros to ensure 3 digits
        # Handle timezone
        tz_part = parts[7]
        if tz_part == "Z":
            tzinfo = timezone.utc
        else:
            tz_sign = 1 if tz_part[0] == "+" else -1
            tz_hours, tz_minutes = map(int, tz_part[1:].split(":"))
            tzinfo = timezone(timedelta(hours=tz_hours * tz_sign, minutes=tz_minutes))
        # Construct a timezone aware datetime object
        dt = datetime(
            year, month, day, hour, minute, second, microsecond, tzinfo=tzinfo
        )
        return dt.astimezone(tz.tzutc())
    else:
        raise ValueError("Invalid date string: {}".format(date_string))
