import re
from typing import Optional
from logger import log

class ValidationError(Exception):
    """Custom exception for validation errors"""
    pass

def validate_discord_id(user_id: str) -> bool:
    """Validate Discord user ID format"""
    try:
        uid = int(user_id)
        return uid > 0 and uid < 2**63  # Discord snowflake ID range
    except (ValueError, TypeError):
        return False

def validate_product_name(name: str) -> bool:
    """Validate product name format"""
    if not name or len(name) > 50:
        return False
    # Allow alphanumeric, spaces, hyphens, underscores
    return bool(re.match(r'^[a-zA-Z0-9 _-]+$', name))

def validate_version_string(version: str) -> bool:
    """Validate semantic version format (e.g., 1.0.0)"""
    if not version:
        return False
    # Simple semantic version check
    return bool(re.match(r'^\d+\.\d+\.\d+(-[a-zA-Z0-9.]+)?$', version))

def validate_license_key(key: str) -> bool:
    """Validate license key format (XXXX-XXXX-XXXX-XXXX)"""
    if not key:
        return False
    # Expected format: XXXX-XXXX-XXXX-XXXX where X is alphanumeric
    return bool(re.match(r'^[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}$', key))

def sanitize_input(text: str, max_length: int = 100) -> str:
    """Sanitize user input to prevent injection attacks"""
    if not text:
        return ""
    
    # Remove potentially dangerous characters
    sanitized = re.sub(r'[<>"\'\n\r\t]', '', text)
    
    # Truncate to max length
    sanitized = sanitized[:max_length]
    
    return sanitized.strip()

def validate_file_size(size_bytes: int, max_mb: int = 25) -> bool:
    """Validate file size is within limits"""
    max_bytes = max_mb * 1024 * 1024
    return 0 < size_bytes <= max_bytes

def is_admin(user_id: int, admin_ids: list) -> bool:
    """Check if user is an admin (from .env list only)"""
    return user_id in admin_ids

def format_error_message(error: Exception, context: str = "") -> str:
    """Format error message for user display"""
    message = f"An error occurred"
    if context:
        message += f" while {context}"
    message += ". Please contact support if this persists."
    
    log.error(f"{context}: {error}", exc_info=True)
    return message
