"""Standalone script to verify Snowflake key-pair authentication."""

import os

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv


def _get_connection() -> snowflake.connector.SnowflakeConnection:
    private_key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    private_key_passphrase = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]

    with open(private_key_path, "rb") as key_file:
        private_key_data = key_file.read()

    private_key = serialization.load_pem_private_key(
        private_key_data,
        password=private_key_passphrase.encode(),
    )

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key_der,
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        role=os.environ.get("SNOWFLAKE_ROLE", ""),
    )


def main() -> None:
    load_dotenv()

    try:
        conn = _get_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT
                        CURRENT_USER(),
                        CURRENT_ROLE(),
                        CURRENT_DATABASE(),
                        CURRENT_WAREHOUSE(),
                        CURRENT_SCHEMA()
                    """
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

            labels = [
                "Current user",
                "Current role",
                "Current database",
                "Current warehouse",
                "Current schema",
            ]

            print("Snowflake session details:")
            for label, value in zip(labels, row):
                print(f"- {label}: {value}")

            print("Connection successful!")
        finally:
            conn.close()
    except Exception as error:
        print(f"Connection failed: {error}")


if __name__ == "__main__":
    main()
