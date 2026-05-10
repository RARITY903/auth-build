import os
import hashlib
import shutil
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
from config import LOADERS_DIR
from logger import log

class LoaderGenerationError(Exception):
    """Custom exception for loader generation errors"""
    pass

class LoaderGenerator:
    """Handles loader generation with unique hashes and version management"""
    
    def __init__(self):
        self.loaders_dir = LOADERS_DIR
        self._ensure_directory()
        log.info(f"LoaderGenerator initialized with directory: {self.loaders_dir}")
    
    def _ensure_directory(self):
        """Ensure the loaders directory exists"""
        try:
            os.makedirs(self.loaders_dir, exist_ok=True)
        except OSError as e:
            log.error(f"Failed to create loaders directory: {e}", exc_info=True)
            raise LoaderGenerationError(f"Directory creation failed: {e}")
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename to prevent path traversal and invalid characters"""
        # Remove path separators and invalid characters
        sanitized = filename.replace('/', '').replace('\\', '').replace(':', '')
        sanitized = ''.join(c for c in sanitized if c.isalnum() or c in ['_', '-', '.'])
        return sanitized
    
    def generate_unique_hash(self, user_id: int, product_name: str) -> str:
        """Generate a unique hash based on user ID, product name, and timestamp"""
        if not user_id or not product_name:
            raise LoaderGenerationError("Invalid parameters for hash generation")
        
        timestamp = datetime.now().isoformat()
        hash_input = f"{user_id}-{product_name}-{timestamp}"
        unique_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
        
        log.debug(f"Generated unique hash for user {user_id}, product {product_name}")
        return unique_hash
    
    def create_loader_file(self, product_name: str, unique_hash: str, template_path: Optional[str] = None, version: str = "1.0.0") -> str:
        """Create a custom loader file with the unique hash embedded"""
        if not product_name or not unique_hash:
            raise LoaderGenerationError("Product name and unique hash are required")
        
        try:
            # Sanitize product name for filename
            safe_product_name = self._sanitize_filename(product_name)
            
            # Create output filename with hash
            filename = f"{safe_product_name}_{unique_hash}.exe"
            output_path = os.path.join(self.loaders_dir, filename)
            
            # Check if file already exists
            if os.path.exists(output_path):
                log.warning(f"Loader file already exists, overwriting: {output_path}")
            
            if template_path and os.path.exists(template_path):
                # Validate template path is within allowed directory
                if not self._is_safe_path(template_path):
                    raise LoaderGenerationError("Template path is not safe")
                
                # Copy template to new location
                shutil.copy2(template_path, output_path)
                
                # Embed the hash into the file
                self._embed_hash_in_file(output_path, unique_hash)
                log.info(f"Created loader from template: {output_path}")
            else:
                # Create a placeholder loader file
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(f"# {product_name} Loader\n")
                    f.write(f"# Version: {version}\n")
                    f.write(f"# Unique Hash: {unique_hash}\n")
                    f.write(f"# Generated: {datetime.now().isoformat()}\n")
                    f.write("# This is a placeholder loader file\n")
                    f.write(f"# HASH_IDENTIFIER: {unique_hash}\n")
                log.info(f"Created placeholder loader: {output_path}")
            
            return output_path
        except Exception as e:
            log.error(f"Failed to create loader file: {e}", exc_info=True)
            raise LoaderGenerationError(f"Loader creation failed: {e}")
    
    def _is_safe_path(self, path: str) -> bool:
        """Check if a path is safe (no path traversal)"""
        try:
            abs_path = os.path.abspath(path)
            return not ('..' in abs_path.split(os.sep))
        except:
            return False
    
    def _embed_hash_in_file(self, file_path: str, unique_hash: str):
        """Embed the unique hash into the loader file"""
        try:
            # For binary files, this would need a more sophisticated implementation
            # For now, we'll handle text files and append the hash
            with open(file_path, 'rb') as f:
                content = f.read()
            
            # Check if it's a text file
            try:
                content.decode('utf-8')
                # It's a text file, append hash
                with open(file_path, 'a', encoding='utf-8') as f:
                    f.write(f"\n# HASH_IDENTIFIER: {unique_hash}\n")
            except UnicodeDecodeError:
                # It's a binary file - in production, you would:
                # 1. Use a binary patching library
                # 2. Find a safe section to embed data
                # 3. Update checksums if needed
                log.warning("Binary file detected - hash embedding not implemented for binaries")
        except Exception as e:
            log.error(f"Failed to embed hash in file: {e}", exc_info=True)
    
    def upload_loader_version(self, product_name: str, version: str, file_path: str) -> str:
        """Upload a new loader version for auto-updates"""
        if not all([product_name, version, file_path]):
            raise LoaderGenerationError("Product name, version, and file path are required")
        
        if not os.path.exists(file_path):
            raise LoaderGenerationError(f"Source file does not exist: {file_path}")
        
        try:
            # Sanitize inputs
            safe_product_name = self._sanitize_filename(product_name)
            safe_version = self._sanitize_filename(version)
            
            # Create version-specific directory
            version_dir = os.path.join(self.loaders_dir, safe_product_name, safe_version)
            os.makedirs(version_dir, exist_ok=True)
            
            # Copy file to version directory
            filename = os.path.basename(file_path)
            destination = os.path.join(version_dir, filename)
            
            if os.path.exists(destination):
                log.warning(f"Destination file exists, overwriting: {destination}")
            
            shutil.copy2(file_path, destination)
            
            log.info(f"Uploaded loader version: {destination}")
            return destination
        except Exception as e:
            log.error(f"Failed to upload loader version: {e}", exc_info=True)
            raise LoaderGenerationError(f"Loader upload failed: {e}")
    
    def cleanup_old_loaders(self, days: int = 7):
        """Clean up loader files older than specified days"""
        if days < 0:
            raise LoaderGenerationError("Days must be a positive number")
        
        try:
            cutoff_date = datetime.now() - timedelta(days=days)
            cleaned_count = 0
            
            for root, dirs, files in os.walk(self.loaders_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                        if file_time < cutoff_date:
                            os.remove(file_path)
                            cleaned_count += 1
                            log.debug(f"Removed old loader: {file_path}")
                    except Exception as e:
                        log.warning(f"Failed to remove file {file_path}: {e}")
            
            # Remove empty directories
            for root, dirs, files in os.walk(self.loaders_dir, topdown=False):
                for dir_name in dirs:
                    dir_path = os.path.join(root, dir_name)
                    try:
                        if not os.listdir(dir_path):
                            os.rmdir(dir_path)
                            log.debug(f"Removed empty directory: {dir_path}")
                    except Exception as e:
                        log.warning(f"Failed to remove directory {dir_path}: {e}")
            
            log.info(f"Cleanup completed: {cleaned_count} files removed")
            return cleaned_count
        except Exception as e:
            log.error(f"Failed to cleanup old loaders: {e}", exc_info=True)
            raise LoaderGenerationError(f"Cleanup failed: {e}")
    
    def get_loader_stats(self) -> dict:
        """Get statistics about stored loaders"""
        try:
            stats = {
                'total_files': 0,
                'total_size': 0,
                'products': {}
            }
            
            for root, dirs, files in os.walk(self.loaders_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    stats['total_files'] += 1
                    stats['total_size'] += os.path.getsize(file_path)
                    
                    # Count by product
                    rel_path = os.path.relpath(root, self.loaders_dir)
                    product = rel_path.split(os.sep)[0] if rel_path != '.' else 'root'
                    if product not in stats['products']:
                        stats['products'][product] = 0
                    stats['products'][product] += 1
            
            stats['total_size_mb'] = round(stats['total_size'] / (1024 * 1024), 2)
            return stats
        except Exception as e:
            log.error(f"Failed to get loader stats: {e}", exc_info=True)
            return {}
