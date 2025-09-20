"""
SQL Time-travel DB + Audit Layer
Single-file program demonstrating temporal table versioning with Python + PostgreSQL.
"""

import psycopg
from datetime import datetime
import json

DB_URL = "postgresql://postgres:postgres@localhost:5432/timetravel"

def init_db():
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Create main table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                valid_from TIMESTAMP NOT NULL DEFAULT now(),
                valid_to TIMESTAMP NOT NULL DEFAULT 'infinity'
            );
            """)
            # History table
            cur.execute("""
            CREATE TABLE IF NOT EXISTS users_history (
                hid SERIAL PRIMARY KEY,
                id INT NOT NULL,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                valid_from TIMESTAMP NOT NULL,
                valid_to TIMESTAMP NOT NULL,
                archived_at TIMESTAMP NOT NULL DEFAULT now()
            );
            """)

            # Trigger function
            cur.execute("""
            CREATE OR REPLACE FUNCTION maintain_history()
            RETURNS TRIGGER AS $$
            BEGIN
                UPDATE users
                SET valid_to = now()
                WHERE id = OLD.id AND valid_to = 'infinity';

                INSERT INTO users_history (id, name, email, valid_from, valid_to)
                VALUES (OLD.id, OLD.name, OLD.email, OLD.valid_from, now());

                INSERT INTO users (id, name, email, valid_from, valid_to)
                VALUES (OLD.id, NEW.name, NEW.email, now(), 'infinity');

                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """)

            cur.execute("""
            DROP TRIGGER IF EXISTS trg_versioning ON users;
            CREATE TRIGGER trg_versioning
            BEFORE UPDATE ON users
            FOR EACH ROW
            EXECUTE FUNCTION maintain_history();
            """)

def insert_user(name, email):
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (name, email) VALUES (%s,%s) RETURNING id", (name, email))
            user_id = cur.fetchone()[0]
            print(f"Inserted user {name} with id {user_id}")
            return user_id

def update_user(user_id, name=None, email=None):
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            fields = []
            params = []
            if name: 
                fields.append("name = %s")
                params.append(name)
            if email: 
                fields.append("email = %s")
                params.append(email)
            if not fields:
                print("No fields to update.")
                return
            params.append(user_id)
            sql = "UPDATE users SET " + ", ".join(fields) + " WHERE id = %s AND valid_to='infinity'"
            cur.execute(sql, params)
            print(f"Updated user {user_id}")

def query_history(user_id):
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, email, valid_from, valid_to FROM users_history WHERE id=%s ORDER BY archived_at DESC", (user_id,))
            rows = cur.fetchall()
            print(f"History for user {user_id}:")
            for r in rows:
                print(r)

def query_as_of(user_id, ts):
    ts_obj = datetime.fromisoformat(ts)
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("""
            SELECT id, name, email, valid_from, valid_to
            FROM users
            WHERE id=%s AND valid_from <= %s AND valid_to > %s
            UNION ALL
            SELECT id, name, email, valid_from, valid_to
            FROM users_history
            WHERE id=%s AND valid_from <= %s AND valid_to > %s
            """, (user_id, ts_obj, ts_obj, user_id, ts_obj, ts_obj))
            rows = cur.fetchall()
            print(f"State of user {user_id} at {ts}:")
            for r in rows:
                print(r)

if __name__ == "__main__":
    init_db()
    uid = insert_user("Alice","alice@example.com")
    update_user(uid,email="alice@work.com")
    update_user(uid,name="Alicia")
    query_history(uid)
    query_as_of(uid,"2025-09-01T00:00:00")
