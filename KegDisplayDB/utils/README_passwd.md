# KegDisplayDB Password Utility

This utility allows you to manage usernames and passwords for the KegDisplayDB web interface.

## Features

- Add new users with secure password hashing
- Update passwords for existing users
- Delete users
- List all registered users

## Usage

The passwd utility is installed as a Poetry script, which means you can run it using:

```bash
poetry run passwd [command] [options]
```

### Commands

#### Add or Update a User

To add a new user or update an existing user's password:

```bash
poetry run passwd add username
```

You will be prompted to enter a password (input will not be displayed for security).

You can also provide the password directly (less secure):

```bash
poetry run passwd add username -p password
```

For convenience, you can also just provide a username and it will be treated as an add command:

```bash
poetry run passwd username
```

#### Delete a User

To delete a user:

```bash
poetry run passwd delete username
```

#### List Users

To list all registered users:

```bash
poetry run passwd list
```

## Password Storage

Passwords are stored in `~/.KegDisplayDB/etc/passwd` using bcrypt hashing for security. The file format is:

```
username:bcrypt_hashed_password
```

## Security Notes

- Passwords are never stored in plain text
- Password input is not displayed when typed
- Password confirmation is required when adding or updating users
- Usernames are limited to alphanumeric characters, underscore, hyphen, and period 