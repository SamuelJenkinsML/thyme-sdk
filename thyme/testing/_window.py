"""Window duration parsing — direct port of crates/engine/src/operators.rs:10-43."""


def parse_window_duration(s: str) -> int:
    """Parse duration strings like '30d', '1h', '90m', '30s' into seconds.

    Empty string returns 0 (unbounded window).
    """
    s = s.strip()
    if not s:
        return 0

    i = 0
    while i < len(s) and s[i].isdigit():
        i += 1

    if i == 0:
        raise ValueError(f"Invalid window duration '{s}': no numeric prefix")

    num = int(s[:i])
    unit = s[i:].strip()

    if not unit:
        raise ValueError(f"Invalid window duration '{s}': missing unit (d/h/m/s)")

    multipliers = {"d": 86400, "h": 3600, "m": 60, "s": 1}
    if unit[0] not in multipliers:
        raise ValueError(f"Unknown unit '{unit[0]}' in duration '{s}'")

    return num * multipliers[unit[0]]
