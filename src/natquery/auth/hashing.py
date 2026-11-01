"""Password hashing with scrypt (stdlib, memory-hard) + constant-time verify."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os

_N, _R, _P, _DKLEN = 2**15, 8, 1, 32
_MAXMEM = 128 * _N * _R * 2

_COMMON = {
    "password",
    "password1",
    "123456789",
    "1234567890",
    "qwertyuiop",
    "iloveyou12",
    "admin12345",
    "welcome123",
    "iitkgp12345",
    "changeme123",
}


def _b64(b: bytes) -> str:
    return base64.b64encode(b).decode()


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.scrypt(
        password.encode(), salt=salt, n=_N, r=_R, p=_P, dklen=_DKLEN, maxmem=_MAXMEM
    )
    return f"scrypt${_N}${_R}${_P}${_b64(salt)}${_b64(dk)}"


def verify_password(password: str, stored: str) -> bool:
    try:
        scheme, n, r, p, salt, expected = stored.split("$")
        if scheme != "scrypt":
            return False
        n, r, p = int(n), int(r), int(p)
        dk = hashlib.scrypt(
            password.encode(),
            salt=base64.b64decode(salt),
            n=n,
            r=r,
            p=p,
            dklen=_DKLEN,
            maxmem=128 * n * r * 2,
        )
        return hmac.compare_digest(dk, base64.b64decode(expected))
    except Exception:
        return False


def needs_rehash(stored: str) -> bool:
    try:
        _, n, r, p, *_ = stored.split("$")
        return (int(n), int(r), int(p)) != (_N, _R, _P)
    except Exception:
        return True


# Verified against for unknown usernames so response time does not reveal existence.
DUMMY_HASH = hash_password("natquery-dummy-password")


def check_password_strength(
    password: str, username: str = "", min_length: int = 10
) -> None:
    """Raise ValueError describing the first rule violated."""
    if len(password) < min_length:
        raise ValueError(f"Password must be at least {min_length} characters.")
    if username and username.lower() in password.lower():
        raise ValueError("Password must not contain the username.")
    if password.lower() in _COMMON:
        raise ValueError("Password is too common.")
    if password.isdigit() or password.isalpha():
        raise ValueError("Password must mix letters with digits or symbols.")
