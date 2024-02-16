import secrets
import json
import argparse
import os
import sqlite3

import submission_queue.db as db

def create_user(username):
    con = db.connect_to_db()
    c = con.cursor()
    
    # Check if user already exists
    c.execute('SELECT * FROM users WHERE username = ?', (username,))
    existing_user = c.fetchone()
    if existing_user:
        raise ValueError("User already exists")
    
    token = secrets.token_hex(32)
    c.execute('''
    INSERT INTO users (username, token)
    VALUES (?, ?)
    ''', (username, token))
    con.commit()
    return token

def create_executor(executor_name):
    con = db.connect_to_db()
    c = con.cursor()
    
    # Check if executor already exists
    c.execute('SELECT * FROM executors WHERE name = ?', (executor_name,))
    existing_executor = c.fetchone()
    if existing_executor:
        raise ValueError("executor already exists")
    
    token = secrets.token_hex(32)
    c.execute('''
    INSERT INTO executors (name, token)
    VALUES (?, ?)
    ''', (executor_name, token))
    con.commit()
    return token

def reset_user_token(username):
    con = db.connect_to_db()
    c = con.cursor()
    
    # Check if user already exists
    c.execute('SELECT * FROM users WHERE username = ?', (username,))
    existing_user = c.fetchone()
    if not existing_user:
        raise ValueError("User does not exist")
    
    token = secrets.token_hex(32)
    c.execute('''
    UPDATE users
    SET token = ?
    WHERE username = ?
    ''', (token, username))
    con.commit()
    return token

def reset_executor_token(executor_name):
    con = db.connect_to_db()
    c = con.cursor()
    
    # Check if executor already exists
    c.execute('SELECT * FROM executors WHERE name = ?', (executor_name,))
    existing_executor = c.fetchone()
    if not existing_executor:
        raise ValueError("executor does not exist")
    
    token = secrets.token_hex(32)
    c.execute('''
    UPDATE executors
    SET token = ?
    WHERE name = ?
    ''', (token, executor_name))
    con.commit()
    return token

def delete_user(username):
    con = db.connect_to_db()
    c = con.cursor()
    
    # Check if user already exists
    c.execute('SELECT * FROM users WHERE username = ?', (username,))
    existing_user = c.fetchone()
    if not existing_user:
        raise ValueError("User does not exist")

    c.execute('''
    DELETE FROM users
    WHERE username = ?
    ''', (username,))
    con.commit()

def delete_executor(executor_name):
    con = db.connect_to_db()
    c = con.cursor()
    
    # Check if executor already exists
    c.execute('SELECT * FROM executors WHERE name = ?', (executor_name,))
    existing_executor = c.fetchone()
    if not existing_executor:
        raise ValueError("executor does not exist")

    c.execute('''
    DELETE FROM executors
    WHERE name = ?
    ''', (executor_name,))
    con.commit()

def get_user_token(username):
    con = db.connect_to_db()
    c = con.cursor()

    c.execute('SELECT token FROM users WHERE username = ?', (username,))
    user_token = c.fetchone()
    if not user_token:
        raise ValueError("User does not exist")

    return user_token[0]

def get_executor_token(executor_name):
    con = db.connect_to_db()
    c = con.cursor()

    c.execute('SELECT token FROM executors WHERE name = ?', (executor_name,))
    executor_token = c.fetchone()
    if not executor_token:
        raise ValueError("Executor does not exist")

    return executor_token[0]

def list_users():
    con = db.connect_to_db()
    c = con.cursor()

    c.execute('SELECT username FROM users')
    users = c.fetchall()
    return [user[0] for user in users]

def list_executors():
    con = db.connect_to_db()
    c = con.cursor()

    c.execute('SELECT name FROM executors')
    executors = c.fetchall()
    return [executor[0] for executor in executors]

def create_user_handler(args):
    token = create_user(args.username)
    print(f"Created user {args.username} with token {token}")

def create_executor_handler(args):
    token = create_executor(args.executor_name)
    print(f"Created executor {args.executor_name} with token {token}")

def reset_user_token_handler(args):
    token = reset_user_token(args.username)
    print(f"User {args.username} token: {token}")

def reset_executor_token_handler(args):
    token = reset_executor_token(args.executor_name)
    print(f"Executor {args.executor_name} token: {token}")

def delete_user_handler(args):
    delete_user(args.username)
    print(f"Deleted user {args.username}")

def delete_executor_handler(args):
    delete_executor(args.executor_name)
    print(f"Deleted executor {args.executor_name}")

def get_user_token_handler(args):
    token = get_user_token(args.username)
    print(f"User {args.username} token: {token}")

def get_executor_token_handler(args):
    token = get_executor_token(args.executor_name)
    print(f"Executor {args.executor_name} token: {token}")

def list_users_handler(args):
    users = list_users()
    print(json.dumps(users, indent=2))

def list_executors_handler(args):
    executors = list_executors()
    print(json.dumps(executors, indent=2))

def main():
    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers()

    create_user_parser = subparsers.add_parser('create-user', help='Create a new user')
    create_user_parser.add_argument('username', help='The username of the new user')
    create_user_parser.set_defaults(func=create_user_handler)

    create_executor_parser = subparsers.add_parser('create-executor', help='Create a new executor')
    create_executor_parser.add_argument('executor_name', help='The name of the new executor')
    create_executor_parser.set_defaults(func=create_executor_handler)

    reset_user_token_parser = subparsers.add_parser('reset-user-token', help='Reset the token of a user')
    reset_user_token_parser.add_argument('username', help='The username of the user')
    reset_user_token_parser.set_defaults(func=reset_user_token_handler)

    reset_executor_token_parser = subparsers.add_parser('reset-executor-token', help='Reset the token of an executor')
    reset_executor_token_parser.add_argument('executor_name', help='The name of the executor')
    reset_executor_token_parser.set_defaults(func=reset_executor_token_handler)

    delete_user_parser = subparsers.add_parser('delete-user', help='Delete a user')
    delete_user_parser.add_argument('username', help='The username of the user to delete')
    delete_user_parser.set_defaults(func=delete_user_handler)

    delete_executor_parser = subparsers.add_parser('delete-executor', help='Delete an executor')
    delete_executor_parser.add_argument('executor_name', help='The name of the executor to delete')
    delete_executor_parser.set_defaults(func=delete_executor_handler)

    get_user_token_parser = subparsers.add_parser('get-user-token', help='Get the token of a user')
    get_user_token_parser.add_argument('username', help='The username of the user')
    get_user_token_parser.set_defaults(func=get_user_token_handler)

    get_executor_token_parser = subparsers.add_parser('get-executor-token', help='Get the token of an executor')
    get_executor_token_parser.add_argument('executor_name', help='The name of the executor')
    get_executor_token_parser.set_defaults(func=get_executor_token_handler)

    list_users_parser = subparsers.add_parser('list-users', help='List all users')
    list_users_parser.set_defaults(func=list_users_handler)

    list_executors_parser = subparsers.add_parser('list-executors', help='List all executors')
    list_executors_parser.set_defaults(func=list_executors_handler)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()