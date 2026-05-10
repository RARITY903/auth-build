import sqlite3
import hashlib
from datetime import datetime
from typing import Optional, Tuple, List
from contextlib import contextmanager
from config import DATABASE_FILE
from logger import log

class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass

class AuthDatabase:
    """Database manager for authentication system with proper error handling and logging"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        self._connect()
        self._initialize_tables()
        log.info("Database initialized successfully")
    
    def _connect(self):
        """Establish database connection with proper settings"""
        try:
            self.conn = sqlite3.connect(
                DATABASE_FILE, 
                check_same_thread=False,
                timeout=30.0
            )
            self.conn.row_factory = sqlite3.Row  # Enable column access by name
            self.cursor = self.conn.cursor()
            log.debug(f"Database connection established: {DATABASE_FILE}")
        except sqlite3.Error as e:
            log.error(f"Failed to connect to database: {e}", exc_info=True)
            raise DatabaseError(f"Database connection failed: {e}")
    
    def _initialize_tables(self):
        """Initialize database tables with proper schema and indexes"""
        try:
            # Keys table - stores license keys
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT UNIQUE NOT NULL,
                    product_name TEXT NOT NULL,
                    is_used BOOLEAN DEFAULT 0,
                    used_by INTEGER,
                    used_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    expires_at TIMESTAMP,
                    is_expired BOOLEAN DEFAULT 0
                )
            ''')
            
            # Run database migrations
            self._run_migrations()
            
            # Loaders table - stores generated loaders
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS loaders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key_id INTEGER,
                    product_name TEXT NOT NULL,
                    unique_hash TEXT NOT NULL UNIQUE,
                    file_path TEXT NOT NULL,
                    version TEXT DEFAULT '1.0.0',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (key_id) REFERENCES keys (id) ON DELETE CASCADE
                )
            ''')
            
            # Products table - stores available products
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    current_version TEXT DEFAULT '1.0.0',
                    loader_template_path TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Admins table - stores admin user IDs
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS admins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER UNIQUE NOT NULL,
                    added_by INTEGER,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Audit log table - tracks all important actions
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    details TEXT,
                    ip_address TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create indexes for better performance
            self._create_indexes()
            
            self.conn.commit()
            log.debug("Database tables initialized successfully")
        except sqlite3.Error as e:
            log.error(f"Failed to initialize database tables: {e}", exc_info=True)
            raise DatabaseError(f"Table initialization failed: {e}")
    
    def _run_migrations(self):
        """Run database migrations to add new columns"""
        try:
            # Check if keys table has expires_at column
            self.cursor.execute("PRAGMA table_info(keys)")
            columns = [column[1] for column in self.cursor.fetchall()]
            
            if 'expires_at' not in columns:
                log.info("Adding expires_at column to keys table")
                self.cursor.execute('ALTER TABLE keys ADD COLUMN expires_at TIMESTAMP')
            
            if 'is_expired' not in columns:
                log.info("Adding is_expired column to keys table")
                self.cursor.execute('ALTER TABLE keys ADD COLUMN is_expired BOOLEAN DEFAULT 0')
            
            # Check if audit_log table exists
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='audit_log'")
            if not self.cursor.fetchone():
                log.info("Creating audit_log table")
                self.cursor.execute('''
                    CREATE TABLE audit_log (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        action TEXT NOT NULL,
                        details TEXT,
                        ip_address TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            
            # Check if admins table exists
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='admins'")
            if not self.cursor.fetchone():
                log.info("Creating admins table")
                self.cursor.execute('''
                    CREATE TABLE admins (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER UNIQUE NOT NULL,
                        added_by INTEGER,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            
            self.conn.commit()
            log.info("Database migrations completed successfully")
        except sqlite3.Error as e:
            log.error(f"Failed to run migrations: {e}", exc_info=True)
            raise DatabaseError(f"Migration failed: {e}")
    
    def _create_indexes(self):
        """Create indexes for frequently queried columns"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_keys_key ON keys(key)",
            "CREATE INDEX IF NOT EXISTS idx_keys_product ON keys(product_name)",
            "CREATE INDEX IF NOT EXISTS idx_keys_used ON keys(is_used)",
            "CREATE INDEX IF NOT EXISTS idx_keys_expires_at ON keys(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_loaders_hash ON loaders(unique_hash)",
            "CREATE INDEX IF NOT EXISTS idx_loaders_product ON loaders(product_name)",
            "CREATE INDEX IF NOT EXISTS idx_products_name ON products(name)",
            "CREATE INDEX IF NOT EXISTS idx_admins_user_id ON admins(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_audit_log_user_id ON audit_log(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_audit_log_timestamp ON audit_log(timestamp)"
        ]
        
        for index_sql in indexes:
            try:
                self.cursor.execute(index_sql)
            except sqlite3.Error as e:
                log.warning(f"Failed to create index: {e}")
    
    @contextmanager
    def transaction(self):
        """Context manager for database transactions with automatic rollback on error"""
        try:
            yield
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            log.error(f"Transaction failed, rolled back: {e}", exc_info=True)
            raise DatabaseError(f"Transaction failed: {e}")
    
    def add_key(self, key: str, product_name: str) -> bool:
        """Add a new license key to the database"""
        if not key or not product_name:
            log.warning("Attempted to add key with empty key or product name")
            return False
        
        try:
            with self.transaction():
                self.cursor.execute(
                    'INSERT INTO keys (key, product_name) VALUES (?, ?)',
                    (key, product_name)
                )
            log.info(f"Added new key for product: {product_name}")
            return True
        except sqlite3.IntegrityError as e:
            log.warning(f"Key already exists or product not found: {e}")
            return False
        except sqlite3.Error as e:
            log.error(f"Failed to add key: {e}", exc_info=True)
            return False
    
    def validate_key(self, key: str) -> Tuple[bool, Optional[str]]:
        """Validate a license key and return validity status and product name"""
        if not key:
            log.warning("Attempted to validate empty key")
            return False, None
        
        try:
            self.cursor.execute(
                'SELECT id, product_name, is_used FROM keys WHERE key = ?',
                (key,)
            )
            result = self.cursor.fetchone()
            
            if not result:
                log.debug(f"Key not found in database: {key[:8]}...")
                return False, None
            
            key_id, product_name, is_used = result
            if is_used:
                log.info(f"Key already used: {key[:8]}... by user ID")
                return False, None
            
            log.debug(f"Key validated successfully: {key[:8]}...")
            return True, product_name
        except sqlite3.Error as e:
            log.error(f"Failed to validate key: {e}", exc_info=True)
            return False, None
    
    def mark_key_used(self, key: str, user_id: int) -> bool:
        """Mark a key as used by a specific user"""
        if not key or not user_id:
            log.warning("Invalid parameters for marking key as used")
            return False
        
        try:
            with self.transaction():
                self.cursor.execute(
                    '''UPDATE keys 
                       SET is_used = 1, used_by_user_id = ?, used_at = ? 
                       WHERE key = ?''',
                    (user_id, datetime.now(), key)
                )
            log.info(f"Marked key as used by user {user_id}")
            return True
        except sqlite3.Error as e:
            log.error(f"Failed to mark key as used: {e}", exc_info=True)
            return False
    
    def add_product(self, name: str, template_path: Optional[str] = None) -> bool:
        """Add a new product to the database"""
        if not name:
            log.warning("Attempted to add product with empty name")
            return False
        
        try:
            with self.transaction():
                self.cursor.execute(
                    'INSERT INTO products (name, loader_template_path) VALUES (?, ?)',
                    (name, template_path)
                )
            log.info(f"Added new product: {name}")
            return True
        except sqlite3.IntegrityError as e:
            log.warning(f"Product already exists: {name}")
            return False
        except sqlite3.Error as e:
            log.error(f"Failed to add product: {e}", exc_info=True)
            return False
    
    def get_product(self, name: str) -> Optional[Tuple]:
        """Get product information by name"""
        if not name:
            return None
        
        try:
            self.cursor.execute(
                'SELECT id, current_version, loader_template_path FROM products WHERE name = ?',
                (name,)
            )
            result = self.cursor.fetchone()
            return tuple(result) if result else None
        except sqlite3.Error as e:
            log.error(f"Failed to get product: {e}", exc_info=True)
            return None
    
    def update_product_version(self, name: str, version: str, template_path: Optional[str] = None) -> bool:
        """Update product version and optionally template path"""
        if not name or not version:
            log.warning("Invalid parameters for updating product version")
            return False
        
        try:
            with self.transaction():
                if template_path:
                    self.cursor.execute(
                        '''UPDATE products 
                           SET current_version = ?, loader_template_path = ?, updated_at = ?
                           WHERE name = ?''',
                        (version, template_path, datetime.now(), name)
                    )
                else:
                    self.cursor.execute(
                        'UPDATE products SET current_version = ?, updated_at = ? WHERE name = ?',
                        (version, datetime.now(), name)
                    )
            log.info(f"Updated product {name} to version {version}")
            return True
        except sqlite3.Error as e:
            log.error(f"Failed to update product version: {e}", exc_info=True)
            return False
    
    def save_loader(self, key_id: int, product_name: str, unique_hash: str, file_path: str, version: str) -> Optional[int]:
        """Save loader information to database"""
        if not all([key_id, product_name, unique_hash, file_path, version]):
            log.warning("Invalid parameters for saving loader")
            return None
        
        try:
            with self.transaction():
                self.cursor.execute(
                    '''INSERT INTO loaders (key_id, product_name, unique_hash, file_path, version)
                       VALUES (?, ?, ?, ?, ?)''',
                    (key_id, product_name, unique_hash, file_path, version)
                )
            loader_id = self.cursor.lastrowid
            log.info(f"Saved loader with hash: {unique_hash[:8]}...")
            return loader_id
        except sqlite3.IntegrityError as e:
            log.error(f"Loader with this hash already exists: {e}")
            return None
        except sqlite3.Error as e:
            log.error(f"Failed to save loader: {e}", exc_info=True)
            return None
    
    def get_loader_by_hash(self, unique_hash: str) -> Optional[Tuple]:
        """Get loader information by unique hash"""
        if not unique_hash:
            return None
        
        try:
            self.cursor.execute(
                'SELECT * FROM loaders WHERE unique_hash = ?',
                (unique_hash,)
            )
            result = self.cursor.fetchone()
            return tuple(result) if result else None
        except sqlite3.Error as e:
            log.error(f"Failed to get loader by hash: {e}", exc_info=True)
            return None
    
    def get_all_products(self) -> List[Tuple[str, str]]:
        """Get all products with their current versions"""
        try:
            self.cursor.execute('SELECT name, current_version FROM products ORDER BY name')
            results = self.cursor.fetchall()
            return [(row[0], row[1]) for row in results]
        except sqlite3.Error as e:
            log.error(f"Failed to get all products: {e}", exc_info=True)
            return []
    
    def get_key_id(self, key: str) -> Optional[int]:
        """Get the database ID for a key"""
        try:
            self.cursor.execute('SELECT id FROM keys WHERE key = ?', (key,))
            result = self.cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            log.error(f"Failed to get key ID: {e}", exc_info=True)
            return None
    
    def get_statistics(self) -> dict:
        """Get system statistics"""
        try:
            stats = {}
            
            self.cursor.execute('SELECT COUNT(*) FROM keys')
            stats['total_keys'] = self.cursor.fetchone()[0]
            
            self.cursor.execute('SELECT COUNT(*) FROM keys WHERE is_used = 1')
            stats['used_keys'] = self.cursor.fetchone()[0]
            
            self.cursor.execute('SELECT COUNT(*) FROM loaders')
            stats['total_loaders'] = self.cursor.fetchone()[0]
            
            self.cursor.execute('SELECT COUNT(*) FROM products')
            stats['total_products'] = self.cursor.fetchone()[0]
            
            return stats
        except sqlite3.Error as e:
            log.error(f"Failed to get statistics: {e}", exc_info=True)
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            try:
                self.conn.close()
                log.info("Database connection closed")
            except sqlite3.Error as e:
                log.error(f"Error closing database connection: {e}", exc_info=True)
    
    def add_admin(self, user_id: int, added_by: int) -> bool:
        """Add a user to the admin list"""
        try:
            with self.transaction():
                self.cursor.execute(
                    'INSERT INTO admins (user_id, added_by) VALUES (?, ?)',
                    (user_id, added_by)
                )
            log.info(f"Added admin {user_id} by {added_by}")
            return True
        except sqlite3.IntegrityError:
            log.warning(f"User {user_id} is already an admin")
            return False
        except sqlite3.Error as e:
            log.error(f"Failed to add admin: {e}", exc_info=True)
            return False
    
    def remove_admin(self, user_id: int) -> bool:
        """Remove a user from the admin list"""
        try:
            with self.transaction():
                self.cursor.execute('DELETE FROM admins WHERE user_id = ?', (user_id,))
                if self.cursor.rowcount > 0:
                    log.info(f"Removed admin {user_id}")
                    return True
                return False
        except sqlite3.Error as e:
            log.error(f"Failed to remove admin: {e}", exc_info=True)
            return False
    
    def is_admin_db(self, user_id: int) -> bool:
        """Check if a user is an admin in the database"""
        try:
            self.cursor.execute('SELECT 1 FROM admins WHERE user_id = ?', (user_id,))
            return self.cursor.fetchone() is not None
        except sqlite3.Error as e:
            log.error(f"Failed to check admin status: {e}", exc_info=True)
            return False
    
    def get_all_admins(self) -> List[int]:
        """Get all admin user IDs from database"""
        try:
            self.cursor.execute('SELECT user_id FROM admins')
            return [row[0] for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            log.error(f"Failed to get admins: {e}", exc_info=True)
            return []
    
    def log_action(self, user_id: int, action: str, details: str = None, ip_address: str = None) -> bool:
        """Log an action to the audit log"""
        try:
            with self.transaction():
                self.cursor.execute(
                    'INSERT INTO audit_log (user_id, action, details, ip_address) VALUES (?, ?, ?, ?)',
                    (user_id, action, details, ip_address)
                )
            log.debug(f"Logged action: {action} by user {user_id}")
            return True
        except sqlite3.Error as e:
            log.error(f"Failed to log action: {e}", exc_info=True)
            return False
    
    def get_user_statistics(self, user_id: int) -> dict:
        """Get detailed statistics for a specific user"""
        try:
            stats = {}
            
            # Keys redeemed by this user
            self.cursor.execute('''
                SELECT COUNT(*) FROM keys 
                WHERE used_by = ? AND is_used = 1
            ''', (user_id,))
            stats['keys_redeemed'] = self.cursor.fetchone()[0]
            
            # Loaders generated for this user
            self.cursor.execute('''
                SELECT COUNT(*) FROM loaders l
                JOIN keys k ON l.key_id = k.id
                WHERE k.used_by = ?
            ''', (user_id,))
            stats['loaders_generated'] = self.cursor.fetchone()[0]
            
            # Products redeemed by this user
            self.cursor.execute('''
                SELECT DISTINCT k.product_name, COUNT(*) as count
                FROM keys k
                WHERE k.used_by = ? AND k.is_used = 1
                GROUP BY k.product_name
                ORDER BY count DESC
            ''', (user_id,))
            stats['products'] = self.cursor.fetchall()
            
            # Recent activity
            self.cursor.execute('''
                SELECT k.product_name, k.used_at
                FROM keys k
                WHERE k.used_by = ? AND k.is_used = 1
                ORDER BY k.used_at DESC
                LIMIT 5
            ''', (user_id,))
            stats['recent_activity'] = self.cursor.fetchall()
            
            # First redemption
            self.cursor.execute('''
                SELECT MIN(used_at) FROM keys
                WHERE used_by = ? AND is_used = 1
            ''', (user_id,))
            first_redemption = self.cursor.fetchone()[0]
            stats['first_redemption'] = first_redemption
            
            # Last redemption
            self.cursor.execute('''
                SELECT MAX(used_at) FROM keys
                WHERE used_by = ? AND is_used = 1
            ''', (user_id,))
            last_redemption = self.cursor.fetchone()[0]
            stats['last_redemption'] = last_redemption
            
            return stats
        except sqlite3.Error as e:
            log.error(f"Failed to get user statistics: {e}", exc_info=True)
            return {}
    
    def get_top_users(self, limit: int = 10) -> List[tuple]:
        """Get top users by number of keys redeemed"""
        try:
            self.cursor.execute('''
                SELECT k.used_by, COUNT(*) as count, u.username
                FROM keys k
                LEFT JOIN users u ON k.used_by = u.user_id
                WHERE k.is_used = 1
                GROUP BY k.used_by
                ORDER BY count DESC
                LIMIT ?
            ''', (limit,))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            log.error(f"Failed to get top users: {e}", exc_info=True)
            return []
    
    def bulk_generate_keys(self, product_name: str, count: int, expires_days: int = None) -> List[str]:
        """Generate multiple keys in bulk"""
        try:
            import secrets
            import string
            from datetime import datetime, timedelta
            
            keys = []
            expires_at = None
            if expires_days:
                expires_at = (datetime.now() + timedelta(days=expires_days)).strftime('%Y-%m-%d %H:%M:%S')
            
            for _ in range(count):
                key = ''.join(secrets.choice(string.ascii_uppercase + string.digits) for _ in range(16))
                key = f"{key[:4]}-{key[4:8]}-{key[8:12]}-{key[12:]}"
                
                if self.add_key(key, product_name, expires_at):
                    keys.append(key)
            
            return keys
        except Exception as e:
            log.error(f"Failed to bulk generate keys: {e}", exc_info=True)
            return []
    
    def add_key(self, key: str, product_name: str, expires_at: str = None) -> bool:
        """Add a new license key to the database"""
        try:
            with self.transaction():
                self.cursor.execute(
                    'INSERT INTO keys (key, product_name, expires_at) VALUES (?, ?, ?)',
                    (key, product_name, expires_at)
                )
            log.info(f"Added key for product {product_name}")
            return True
        except sqlite3.IntegrityError:
            log.warning(f"Key {key} already exists")
            return False
        except sqlite3.Error as e:
            log.error(f"Failed to add key: {e}", exc_info=True)
            return False
    
    def validate_key(self, key: str) -> tuple[bool, str]:
        """Validate a license key and return product name if valid"""
        try:
            self.cursor.execute('''
                SELECT product_name, is_used, expires_at, is_expired 
                FROM keys 
                WHERE key = ?
            ''', (key,))
            
            result = self.cursor.fetchone()
            if not result:
                return False, ""
            
            product_name, is_used, expires_at, is_expired = result
            
            # Check if key is already used
            if is_used:
                return False, product_name
            
            # Check if key is expired
            if is_expired:
                return False, product_name
            
            # Check expiration date
            if expires_at:
                from datetime import datetime
                if datetime.now().strftime('%Y-%m-%d %H:%M:%S') > expires_at:
                    # Mark as expired
                    self.mark_key_expired(key)
                    return False, product_name
            
            return True, product_name
        except sqlite3.Error as e:
            log.error(f"Failed to validate key: {e}", exc_info=True)
            return False, ""
    
    def mark_key_expired(self, key: str) -> bool:
        """Mark a key as expired"""
        try:
            with self.transaction():
                self.cursor.execute(
                    'UPDATE keys SET is_expired = 1 WHERE key = ?',
                    (key,)
                )
            log.info(f"Marked key as expired: {key[:8]}...")
            return True
        except sqlite3.Error as e:
            log.error(f"Failed to mark key as expired: {e}", exc_info=True)
            return False
    
    def cleanup_expired_keys(self) -> int:
        """Clean up expired keys and return count of cleaned keys"""
        try:
            from datetime import datetime
            
            # Find expired keys
            self.cursor.execute('''
                UPDATE keys SET is_expired = 1 
                WHERE expires_at IS NOT NULL 
                AND expires_at < ? 
                AND is_expired = 0
            ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
            
            count = self.cursor.rowcount
            if count > 0:
                log.info(f"Marked {count} keys as expired")
            
            return count
        except sqlite3.Error as e:
            log.error(f"Failed to cleanup expired keys: {e}", exc_info=True)
            return 0
    
    def get_expiring_keys(self, days: int = 7) -> List[tuple]:
        """Get keys that will expire within specified days"""
        try:
            from datetime import datetime, timedelta
            
            future_date = (datetime.now() + timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
            
            self.cursor.execute('''
                SELECT key, product_name, expires_at 
                FROM keys 
                WHERE expires_at IS NOT NULL 
                AND expires_at <= ? 
                AND expires_at > ? 
                AND is_used = 0 
                AND is_expired = 0
                ORDER BY expires_at ASC
            ''', (future_date, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            log.error(f"Failed to get expiring keys: {e}", exc_info=True)
            return []
