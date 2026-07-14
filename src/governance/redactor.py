import hashlib
import os


def _mask_all(value, field_cfg, policy):
    return policy.get("defaults", {}).get("mask_token", "***REDACTED***")


def _mask_partial(value, field_cfg, policy):
    s = str(value)
    keep_last = field_cfg.get("keep_last", 0)
    keep_first = field_cfg.get("keep_first", 0)
    mask_char = field_cfg.get("mask_char", "X")
    if keep_first + keep_last >= len(s):
        # value too short to partially reveal — fail closed to full mask
        return mask_char * len(s)
    first = s[:keep_first]
    last = s[len(s) - keep_last:] if keep_last else ""
    middle = mask_char * (len(s) - keep_first - keep_last)
    return f"{first}{middle}{last}"


def _hash(value, field_cfg, policy):
    hashing = policy.get("hashing", {})
    salt = os.getenv(hashing.get("salt_env_var", ""), "")
    length = hashing.get("output_length", 16)
    digest = hashlib.sha256((salt + str(value)).encode()).hexdigest()
    return digest[:length]


def _drop(value, field_cfg, policy):
    return _DROP  # sentinel; caller removes the field


def _passthrough(value, field_cfg, policy):
    return value


_DROP = object()

STRATEGIES = {
    "mask_all": _mask_all,
    "mask_partial": _mask_partial,
    "hash": _hash,
    "drop": _drop,
    "passthrough": _passthrough,
}


def apply_redaction(event, policy):
    """Returns (safe_event, decisions).
    decisions maps field_name -> strategy applied, for the audit log.
    """
    fields_cfg = policy.get("fields", {})
    defaults = policy.get("defaults", {})
    unlisted_strategy = defaults.get("unlisted_field_strategy", "drop")

    safe = {}
    decisions = {}

    for field, value in event.items():
        field_cfg = fields_cfg.get(field)
        strategy = field_cfg["strategy"] if field_cfg else unlisted_strategy

        fn = STRATEGIES.get(strategy)
        if fn is None:
            # unknown strategy in policy — fail closed, drop the field
            decisions[field] = f"unknown_strategy:{strategy}->dropped"
            continue

        result = fn(value, field_cfg or {}, policy)
        if result is _DROP:
            decisions[field] = strategy  # dropped; field omitted from safe
        else:
            safe[field] = result
            if strategy != "passthrough":
                decisions[field] = strategy

    return safe, decisions