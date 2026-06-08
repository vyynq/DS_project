"""
CRYPTOGRAPHY MODULE (PBFT)
==========================
Provides signatures and hashes to detect Byzantine message tampering.
"""

import hashlib
import json

def digest(data: dict) -> str:
    """
    Create a deterministic hash for a dictionary.
    Sort keys so equivalent content produces the same hash.
    """
    # Encode the dictionary as strict JSON.
    encoded = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()[:16]

def sign_message(data: dict, private_key: str = "fake_key") -> str:
    """Simulate signing a message with a private key."""
    # A production system would use RSA, Ed25519, or another real signature scheme.
    return digest({"data": data, "key": private_key})

def verify_signature(data: dict, signature: str, public_key: str = "fake_key") -> bool:
    """Verify that a signature matches the provided data."""
    return sign_message(data, public_key) == signature
