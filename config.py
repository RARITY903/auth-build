import os
from dotenv import load_dotenv
from typing import List

load_dotenv()

def _validate_discord_token(token: str) -> str:
    """Validate Discord token format"""
    if not token or len(token) < 50:
        raise ValueError("Invalid Discord token format")
    return token

def _validate_admin_ids(admin_ids_str: str) -> List[int]:
    """Validate and parse admin user IDs"""
    if not admin_ids_str:
        return []
    
    try:
        ids = [int(uid.strip()) for uid in admin_ids_str.split(',') if uid.strip()]
        if not ids:
            return []
        return ids
    except ValueError as e:
        raise ValueError("Invalid admin user IDs format. Must be comma-separated integers.")

# Configuration
DISCORD_TOKEN = _validate_discord_token(os.getenv('DISCORD_TOKEN', ''))
ADMIN_USER_IDS = _validate_admin_ids(os.getenv('ADMIN_USER_IDS', ''))
EMBED_COLOR = 0x3498db  # Medium blue matching the logo
LOADERS_DIR = 'loaders'
DATABASE_FILE = 'auth_system.db'
LOGS_DIR = 'logs'

# Create necessary directories
for directory in [LOADERS_DIR, LOGS_DIR]:
    os.makedirs(directory, exist_ok=True)

