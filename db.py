import os
import re
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from werkzeug.security import generate_password_hash

try:
    import oracledb
except ImportError:  # pragma: no cover
    oracledb = None


DB_CONFIG = {
    "user": os.getenv("ORACLE_USER", "rfp"),
    "password": os.getenv("ORACLE_PASSWORD", "rfp123"),
    "dsn": os.getenv("ORACLE_DSN", "localhost:1521/XEPDB1"),
}

BASE_DIR = Path(__file__).resolve().parent
# UPDATED: Use a stable SQLite file inside the project directory for Render demo deployments.
SQLITE_PATH = Path(os.getenv("SQLITE_PATH", str(BASE_DIR / "data" / "render_demo.db")))
SQLITE_INITIALIZED = False
DEMO_USERS = [
    ("admin", "admin123", "admin"),
    ("user1", "pass123", "user"),
    ("user2", "pass123", "user"),
    ("user3", "pass123", "user"),
    ("manager", "manager123", "manager"),
    ("guestdemo", "guest123", "user"),
]
OWNER_SEED_USERS = (
    ("srinidhi37", "1234"),
    ("swapna31", "1234"),
)


def using_oracle():
    if os.getenv("USE_ORACLE", "false").lower() not in {"1", "true", "yes"}:
        return False
    if oracledb is None:
        return False
    try:
        connection = oracledb.connect(**DB_CONFIG)
        connection.close()
        return True
    except Exception:
        return False


def get_connection():
    return oracledb.connect(**DB_CONFIG)


def get_sqlite_connection():
    SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(SQLITE_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=MEMORY")
    connection.execute("PRAGMA synchronous=NORMAL")
    return connection


def _normalize_row(cursor, row):
    columns = [column[0] for column in cursor.description]
    return {columns[index]: value for index, value in enumerate(row)}


@contextmanager
def get_cursor(commit=False):
    connection = None
    cursor = None
    try:
        connection = get_connection()
        cursor = connection.cursor()
        yield connection, cursor
        if commit:
            connection.commit()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def initialize_sqlite():
    global SQLITE_INITIALIZED
    if SQLITE_INITIALIZED:
        return

    connection = get_sqlite_connection()
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS app_users (
            user_id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password TEXT NOT NULL,
            email TEXT,
            role TEXT NOT NULL DEFAULT 'user',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_id TEXT,
            user_id INTEGER,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL DEFAULT 0,
            quantity INTEGER NOT NULL DEFAULT 0,
            reorder_level INTEGER NOT NULL DEFAULT 10,
            supplier_name TEXT,
            branch_name TEXT,
            image_url TEXT,
            entry_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL DEFAULT 0,
            quantity INTEGER NOT NULL DEFAULT 0,
            reorder_level INTEGER NOT NULL DEFAULT 10,
            supplier_name TEXT,
            branch_name TEXT,
            image_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
            sale_id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            sale_date TEXT DEFAULT CURRENT_TIMESTAMP,
            total_amount REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS shared_catalogs (
            share_id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user_id INTEGER,
            title TEXT NOT NULL,
            token TEXT NOT NULL UNIQUE,
            source_report_id TEXT,
            qr_filename TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS uploaded_reports (
            report_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            created_by_admin INTEGER,
            token TEXT UNIQUE,
            source_file_name TEXT,
            status TEXT NOT NULL DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS suppliers (
            supplier_id INTEGER PRIMARY KEY AUTOINCREMENT,
            supplier_name TEXT NOT NULL,
            phone TEXT,
            email TEXT,
            address TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS branches (
            branch_id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch_name TEXT NOT NULL,
            location TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS cart_items (
            cart_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            share_token TEXT,
            inventory_id INTEGER NOT NULL,
            report_id TEXT,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            unit_price REAL NOT NULL DEFAULT 0,
            quantity INTEGER NOT NULL DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            status TEXT NOT NULL DEFAULT 'Pending',
            share_token TEXT,
            report_id TEXT,
            total_amount REAL NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            inventory_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            unit_price REAL NOT NULL DEFAULT 0,
            quantity INTEGER NOT NULL,
            line_total REAL NOT NULL DEFAULT 0,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS activity_logs (
            activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_username TEXT,
            actor_role TEXT,
            action TEXT NOT NULL,
            detail TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            username TEXT,
            reason TEXT NOT NULL,
            business_name TEXT,
            proof TEXT,
            proof_file_path TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            reviewed_by INTEGER,
            reviewed_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_access_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER,
            email TEXT NOT NULL,
            code TEXT NOT NULL UNIQUE,
            status TEXT NOT NULL DEFAULT 'active',
            expires_at TIMESTAMP,
            used_at TIMESTAMP,
            generated_by INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (request_id) REFERENCES admin_requests(id)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS password_reset_tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            email TEXT,
            reset_token TEXT NOT NULL UNIQUE,
            expires_at TIMESTAMP NOT NULL,
            used_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES app_users(user_id)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_request_notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER NOT NULL,
            email TEXT,
            username TEXT,
            message TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (request_id) REFERENCES admin_requests(id)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS user_orders (
            order_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            inventory_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL DEFAULT 0,
            quantity INTEGER NOT NULL,
            total_amount REAL NOT NULL DEFAULT 0,
            share_token TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    for statement in [
        "ALTER TABLE inventory_data ADD COLUMN report_id TEXT",
        "ALTER TABLE inventory_data ADD COLUMN reorder_level INTEGER NOT NULL DEFAULT 10",
        "ALTER TABLE inventory_data ADD COLUMN supplier_name TEXT",
        "ALTER TABLE inventory_data ADD COLUMN branch_name TEXT",
        "ALTER TABLE inventory_data ADD COLUMN image_url TEXT",
        "ALTER TABLE app_users ADD COLUMN email TEXT",
        "ALTER TABLE products ADD COLUMN reorder_level INTEGER NOT NULL DEFAULT 10",
        "ALTER TABLE products ADD COLUMN supplier_name TEXT",
        "ALTER TABLE products ADD COLUMN branch_name TEXT",
        "ALTER TABLE products ADD COLUMN image_url TEXT",
        "ALTER TABLE shared_catalogs ADD COLUMN source_report_id TEXT",
        "ALTER TABLE shared_catalogs ADD COLUMN qr_filename TEXT",
        "ALTER TABLE admin_requests ADD COLUMN proof_file_path TEXT",
        "ALTER TABLE admin_requests ADD COLUMN reviewed_by INTEGER",
        "ALTER TABLE admin_requests ADD COLUMN reviewed_at TIMESTAMP",
        "ALTER TABLE admin_access_codes ADD COLUMN request_id INTEGER",
        "ALTER TABLE admin_access_codes ADD COLUMN expires_at TIMESTAMP",
        "ALTER TABLE admin_access_codes ADD COLUMN used_at TIMESTAMP",
        "ALTER TABLE admin_access_codes ADD COLUMN generated_by INTEGER",
        "ALTER TABLE password_reset_tokens ADD COLUMN email TEXT",
        "ALTER TABLE password_reset_tokens ADD COLUMN used_at TIMESTAMP",
        "ALTER TABLE admin_request_notifications ADD COLUMN email TEXT",
        "ALTER TABLE admin_request_notifications ADD COLUMN username TEXT",
    ]:
        try:
            cursor.execute(statement)
        except Exception:
            continue
    connection.commit()
    cursor.close()
    connection.close()
    seed_demo_users()
    SQLITE_INITIALIZED = True


def adapt_query_for_sqlite(query):
    updated = query
    replacements = {
        "NVL(": "IFNULL(",
        "CURRENT_TIMESTAMP": "CURRENT_TIMESTAMP",
        "SYSDATE": "CURRENT_TIMESTAMP",
        "TO_DATE(:entry_date, 'YYYY-MM-DD')": ":entry_date",
        "TO_DATE(:date_from, 'YYYY-MM-DD')": ":date_from",
        "TO_DATE(:date_to, 'YYYY-MM-DD')": ":date_to",
        "TO_DATE(:sale_date, 'YYYY-MM-DD')": ":sale_date",
        "TRUNC(entry_date)": "date(entry_date)",
        "TRUNC(created_at)": "date(created_at)",
        "TRUNC(sale_date)": "date(sale_date)",
        "TO_CHAR(TRUNC(entry_date), 'YYYY-MM-DD')": "date(entry_date)",
        "TO_CHAR(TRUNC(created_at), 'YYYY-MM-DD')": "date(created_at)",
        "TO_CHAR(TRUNC(sale_date), 'YYYY-MM-DD')": "date(sale_date)",
    }
    for source, target in replacements.items():
        updated = updated.replace(source, target)
    updated = re.sub(r"FETCH FIRST \d+ ROWS ONLY", "", updated, flags=re.IGNORECASE)
    updated = updated.replace("FOR UPDATE", "")
    return updated


def normalize_sqlite_row(row):
    return {key.upper(): row[key] for key in row.keys()}


def fetch_all(query, params=None):
    if using_oracle():
        try:
            with get_cursor() as (_connection, cursor):
                cursor.execute(query, params or {})
                return [_normalize_row(cursor, row) for row in cursor.fetchall()]
        except Exception:
            return []

    initialize_sqlite()
    connection = get_sqlite_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(adapt_query_for_sqlite(query), params or {})
        return [normalize_sqlite_row(row) for row in cursor.fetchall()]
    except Exception:
        return []
    finally:
        cursor.close()
        connection.close()


def fetch_one(query, params=None):
    if using_oracle():
        try:
            with get_cursor() as (_connection, cursor):
                cursor.execute(query, params or {})
                row = cursor.fetchone()
                return _normalize_row(cursor, row) if row else None
        except Exception:
            return None

    initialize_sqlite()
    connection = get_sqlite_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(adapt_query_for_sqlite(query), params or {})
        row = cursor.fetchone()
        return normalize_sqlite_row(row) if row else None
    except Exception:
        return None
    finally:
        cursor.close()
        connection.close()


def safe_execute(query, params=None):
    if using_oracle():
        try:
            with get_cursor(commit=True) as (_connection, cursor):
                cursor.execute(query, params or {})
                return True, "Success", cursor.rowcount
        except Exception as error:
            if oracledb is not None and isinstance(error, oracledb.IntegrityError):
                return False, "This record already exists or violates a required database rule.", 0
            return False, "The database is unavailable right now. Please verify your Oracle connection and try again.", 0

    initialize_sqlite()
    connection = get_sqlite_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(adapt_query_for_sqlite(query), params or {})
        connection.commit()
        return True, "Success", cursor.rowcount
    except sqlite3.IntegrityError:
        return False, "This record already exists or violates a required database rule.", 0
    except Exception:
        return False, "The local preview database could not process that request.", 0
    finally:
        cursor.close()
        connection.close()


def run_schema(schema_sql):
    if not using_oracle():
        initialize_sqlite()
        return True
    try:
        statements = [part.strip() for part in re.split(r";\s*(?:\r?\n|$)", schema_sql) if part.strip()]
        for statement in statements:
            try:
                with get_cursor(commit=True) as (_connection, cursor):
                    cursor.execute(statement)
            except Exception:
                continue
        return True
    except Exception:
        return False


def ensure_core_tables():
    if using_oracle():
        statements = [
            """
            CREATE TABLE app_users (
                user_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                username VARCHAR2(100) NOT NULL UNIQUE,
                password VARCHAR2(255) NOT NULL,
                email VARCHAR2(255),
                role VARCHAR2(30) DEFAULT 'user' NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE inventory_data (
                id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                report_id VARCHAR2(100),
                user_id NUMBER,
                product_name VARCHAR2(150) NOT NULL,
                category VARCHAR2(100) NOT NULL,
                price NUMBER(10,2) DEFAULT 0 NOT NULL,
                quantity NUMBER DEFAULT 0 NOT NULL,
                reorder_level NUMBER DEFAULT 10 NOT NULL,
                supplier_name VARCHAR2(150),
                branch_name VARCHAR2(150),
                image_url VARCHAR2(400),
                entry_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE products (
                product_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                name VARCHAR2(150) NOT NULL,
                category VARCHAR2(100) NOT NULL,
                price NUMBER(10,2) DEFAULT 0 NOT NULL,
                quantity NUMBER DEFAULT 0 NOT NULL,
                reorder_level NUMBER DEFAULT 10 NOT NULL,
                supplier_name VARCHAR2(150),
                branch_name VARCHAR2(150),
                image_url VARCHAR2(400),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE shared_catalogs (
                share_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                admin_user_id NUMBER,
                title VARCHAR2(200) NOT NULL,
                token VARCHAR2(100) NOT NULL UNIQUE,
                source_report_id VARCHAR2(100),
                qr_filename VARCHAR2(200),
                status VARCHAR2(30) DEFAULT 'active' NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE uploaded_reports (
                report_id VARCHAR2(100) PRIMARY KEY,
                title VARCHAR2(200) NOT NULL,
                created_by_admin NUMBER,
                token VARCHAR2(100) UNIQUE,
                source_file_name VARCHAR2(255),
                status VARCHAR2(30) DEFAULT 'active' NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE suppliers (
                supplier_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                supplier_name VARCHAR2(150) NOT NULL,
                phone VARCHAR2(50),
                email VARCHAR2(150),
                address VARCHAR2(255),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE branches (
                branch_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                branch_name VARCHAR2(150) NOT NULL,
                location VARCHAR2(150),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE cart_items (
                cart_item_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id NUMBER NOT NULL,
                share_token VARCHAR2(100),
                inventory_id NUMBER NOT NULL,
                report_id VARCHAR2(100),
                product_name VARCHAR2(150) NOT NULL,
                category VARCHAR2(100) NOT NULL,
                unit_price NUMBER(10,2) DEFAULT 0 NOT NULL,
                quantity NUMBER DEFAULT 1 NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE orders (
                order_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id NUMBER,
                username VARCHAR2(100),
                status VARCHAR2(30) DEFAULT 'Pending' NOT NULL,
                share_token VARCHAR2(100),
                report_id VARCHAR2(100),
                total_amount NUMBER(12,2) DEFAULT 0 NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE order_items (
                order_item_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                order_id NUMBER NOT NULL,
                inventory_id NUMBER NOT NULL,
                product_name VARCHAR2(150) NOT NULL,
                category VARCHAR2(100) NOT NULL,
                unit_price NUMBER(10,2) DEFAULT 0 NOT NULL,
                quantity NUMBER NOT NULL,
                line_total NUMBER(12,2) DEFAULT 0 NOT NULL
            )
            """,
            """
            CREATE TABLE activity_logs (
                activity_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                actor_username VARCHAR2(100),
                actor_role VARCHAR2(30),
                action VARCHAR2(150) NOT NULL,
                detail VARCHAR2(400),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE admin_requests (
                id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                name VARCHAR2(150) NOT NULL,
                email VARCHAR2(150) NOT NULL,
                username VARCHAR2(100),
                reason VARCHAR2(2000) NOT NULL,
                business_name VARCHAR2(150),
                proof VARCHAR2(2000),
                proof_file_path VARCHAR2(400),
                status VARCHAR2(30) DEFAULT 'pending' NOT NULL,
                reviewed_by NUMBER,
                reviewed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE admin_access_codes (
                id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                request_id NUMBER,
                email VARCHAR2(150) NOT NULL,
                code VARCHAR2(20) NOT NULL UNIQUE,
                status VARCHAR2(30) DEFAULT 'active' NOT NULL,
                expires_at TIMESTAMP,
                used_at TIMESTAMP,
                generated_by NUMBER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE password_reset_tokens (
                id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id NUMBER NOT NULL,
                email VARCHAR2(255),
                reset_token VARCHAR2(20) NOT NULL UNIQUE,
                expires_at TIMESTAMP NOT NULL,
                used_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE admin_request_notifications (
                id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                request_id NUMBER NOT NULL,
                email VARCHAR2(255),
                username VARCHAR2(100),
                message VARCHAR2(1000) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE user_orders (
                order_id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                user_id NUMBER,
                username VARCHAR2(100),
                inventory_id NUMBER NOT NULL,
                product_name VARCHAR2(150) NOT NULL,
                category VARCHAR2(100) NOT NULL,
                price NUMBER(10,2) DEFAULT 0 NOT NULL,
                quantity NUMBER NOT NULL,
                total_amount NUMBER(12,2) DEFAULT 0 NOT NULL,
                share_token VARCHAR2(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
        ]
        for statement in statements:
            try:
                with get_cursor(commit=True) as (_connection, cursor):
                    cursor.execute(statement)
            except Exception:
                continue
    else:
        initialize_sqlite()
    seed_demo_users()


def seed_demo_users():
    if using_oracle():
        for username, password, role in DEMO_USERS:
            existing = fetch_one(
                "SELECT user_id, username FROM app_users WHERE LOWER(username) = LOWER(:username)",
                {"username": username},
            )
            if existing:
                continue
            safe_execute(
                """
                INSERT INTO app_users (username, password, email, role)
                VALUES (:username, :password, :email, :role)
                """,
                {
                    "username": username,
                    "password": generate_password_hash(password),
                    "email": None,
                    "role": role,
                },
            )
        return

    connection = get_sqlite_connection()
    cursor = connection.cursor()
    try:
        for username, password, role in DEMO_USERS:
            cursor.execute("SELECT user_id FROM app_users WHERE LOWER(username) = LOWER(?)", (username,))
            if cursor.fetchone():
                continue
            cursor.execute(
                "INSERT OR IGNORE INTO app_users (username, password, email, role) VALUES (?, ?, ?, ?)",
                (username, generate_password_hash(password), None, role),
            )
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def ensure_owner_users():
    for username, seed_password in OWNER_SEED_USERS:
        existing = get_user_by_username(username)
        password_hash = generate_password_hash(seed_password)
        if existing:
            safe_execute(
                """
                UPDATE app_users
                SET role = :role,
                    password = :password
                WHERE LOWER(username) = LOWER(:username)
                """,
                {"role": "owner", "password": password_hash, "username": username},
            )
            continue

        create_user(username, password_hash, "owner")


def get_user_by_username(username):
    return fetch_one(
        """
        SELECT user_id, username, password, email, role, created_at
        FROM app_users
        WHERE LOWER(username) = LOWER(:username)
        """,
        {"username": username},
    )


def get_user_by_email(email):
    return fetch_one(
        """
        SELECT user_id, username, password, email, role, created_at
        FROM app_users
        WHERE LOWER(email) = LOWER(:email)
        """,
        {"email": email},
    )


def get_user_by_identifier(identifier):
    return fetch_one(
        """
        SELECT user_id, username, password, email, role, created_at
        FROM app_users
        WHERE LOWER(username) = LOWER(:identifier) OR LOWER(email) = LOWER(:identifier)
        """,
        {"identifier": identifier},
    )


def try_get_user_by_username(username):
    if using_oracle():
        try:
            with get_cursor() as (_connection, cursor):
                cursor.execute(
                    """
                    SELECT user_id, username, password, email, role, created_at
                    FROM app_users
                    WHERE LOWER(username) = LOWER(:username)
                    """,
                    {"username": username},
                )
                row = cursor.fetchone()
                return (_normalize_row(cursor, row) if row else None), True
        except Exception:
            return None, False

    initialize_sqlite()
    connection = get_sqlite_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT user_id, username, password, email, role, created_at
            FROM app_users
            WHERE LOWER(username) = LOWER(:username)
            """,
            {"username": username},
        )
        row = cursor.fetchone()
        return (normalize_sqlite_row(row) if row else None), True
    except Exception:
        return None, False
    finally:
        cursor.close()
        connection.close()


def create_user(username, password_hash, role="user", email=None):
    return safe_execute(
        """
        INSERT INTO app_users (username, password, email, role)
        VALUES (:username, :password, :email, :role)
        """,
        {"username": username, "password": password_hash, "email": email, "role": role},
    )


def update_user_password(user_id, password_hash):
    return safe_execute(
        """
        UPDATE app_users
        SET password = :password
        WHERE user_id = :user_id
        """,
        {"password": password_hash, "user_id": user_id},
    )


def insert_inventory_rows(user_id, rows):
    if not rows:
        return True, "No rows to upload."

    if using_oracle():
        connection = None
        cursor = None
        try:
            connection = get_connection()
            cursor = connection.cursor()
            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO inventory_data (report_id, user_id, product_name, category, price, quantity, reorder_level, supplier_name, branch_name, image_url, entry_date)
                    VALUES (:report_id, :user_id, :product_name, :category, :price, :quantity, :reorder_level, :supplier_name, :branch_name, :image_url, TO_DATE(:entry_date, 'YYYY-MM-DD'))
                    """,
                    {
                        "report_id": row.get("report_id"),
                        "user_id": user_id,
                        "product_name": row["product_name"],
                        "category": row["category"],
                        "price": row["price"],
                        "quantity": row["quantity"],
                        "reorder_level": row.get("reorder_level", 10),
                        "supplier_name": row.get("supplier_name"),
                        "branch_name": row.get("branch_name"),
                        "image_url": row.get("image_url"),
                        "entry_date": row["date"],
                    },
                )
            connection.commit()
            return True, f"{len(rows)} records uploaded successfully."
        except Exception:
            if connection:
                connection.rollback()
            return False, "The database could not save the uploaded rows."
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    initialize_sqlite()
    connection = get_sqlite_connection()
    cursor = connection.cursor()
    try:
        cursor.executemany(
            """
            INSERT INTO inventory_data (report_id, user_id, product_name, category, price, quantity, reorder_level, supplier_name, branch_name, image_url, entry_date)
            VALUES (:report_id, :user_id, :product_name, :category, :price, :quantity, :reorder_level, :supplier_name, :branch_name, :image_url, :entry_date)
            """,
            [
                {
                    "report_id": row.get("report_id"),
                    "user_id": user_id,
                    "product_name": row["product_name"],
                    "category": row["category"],
                    "price": row["price"],
                    "quantity": row["quantity"],
                    "reorder_level": row.get("reorder_level", 10),
                    "supplier_name": row.get("supplier_name"),
                    "branch_name": row.get("branch_name"),
                    "image_url": row.get("image_url"),
                    "entry_date": row["date"],
                }
                for row in rows
            ],
        )
        connection.commit()
        return True, f"{len(rows)} records uploaded successfully."
    except Exception:
        connection.rollback()
        return False, "The local preview database could not save the uploaded rows."
    finally:
        cursor.close()
        connection.close()


def get_inventory_rows_for_role(role, user_id=None):
    if role in {"admin", "manager"}:
        return fetch_all(
            """
            SELECT d.id, d.user_id, u.username, d.product_name, d.category, d.price, d.quantity, d.reorder_level, d.supplier_name, d.branch_name, d.image_url, d.entry_date, d.created_at
            FROM inventory_data d
            LEFT JOIN app_users u ON u.user_id = d.user_id
            ORDER BY d.created_at DESC, d.id DESC
            """
        )
    if role == "user" and user_id:
        return fetch_all(
            """
            SELECT d.id, d.user_id, u.username, d.product_name, d.category, d.price, d.quantity, d.reorder_level, d.supplier_name, d.branch_name, d.image_url, d.entry_date, d.created_at
            FROM inventory_data d
            LEFT JOIN app_users u ON u.user_id = d.user_id
            WHERE d.user_id = :user_id
            ORDER BY d.created_at DESC, d.id DESC
            """,
            {"user_id": user_id},
        )
    return []


def get_all_users():
    return fetch_all(
        """
        SELECT user_id, username, role, created_at
        FROM app_users
        ORDER BY created_at DESC, user_id DESC
        """
    )
