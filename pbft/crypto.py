"""
CRYPTOGRAPHY MODULE (PBFT)
==========================
Gère les signatures et les condensats (hashes) pour s'assurer 
qu'aucun nœud byzantin ne falsifie les messages en cours de route.
"""

import hashlib
import json

def digest(data: dict) -> str:
    """
    Crée un hash (empreinte) déterministe pour un dictionnaire.
    Trie les clés pour s'assurer que le même contenu donne le même hash.
    """
    # Encodage du dictionnaire en chaîne JSON stricte
    encoded = json.dumps(data, sort_keys=True).encode('utf-8')
    return hashlib.sha256(encoded).hexdigest()[:16]

def sign_message(data: dict, private_key: str = "fake_key") -> str:
    """Simule la signature d'un message avec une clé privée."""
    # Dans un vrai système, on utiliserait RSA ou ed25519
    return digest({"data": data, "key": private_key})

def verify_signature(data: dict, signature: str, public_key: str = "fake_key") -> bool:
    """Vérifie si la signature correspond bien aux données."""
    return sign_message(data, public_key) == signature