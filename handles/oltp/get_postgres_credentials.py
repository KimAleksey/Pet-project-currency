from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from os import getenv

def get_postgres_rec(path: Path | None = None) -> dict[str, Any]:
    if path is None:
        # Получаем путь до файла с реквизитами подключения к PG
        base_dir = Path(__file__).resolve().parents[2]
        env_path = base_dir / "config" / ".env"
    else:
        env_path = path

    if not env_path.exists():
        raise FileNotFoundError(f"Environment file {env_path} not found")

    # Получаем реквизиты
    load_dotenv(dotenv_path=env_path)

    result = {
        "db_name": getenv("POSTGRES_DB"),
        "host": getenv("POSTGRES_HOST"),
        "port": getenv("POSTGRES_PORT"),
        "user": getenv("POSTGRES_USER"),
        "password": getenv("POSTGRES_PASSWORD"),
    }

    if not all(result is not None for result in result.values()):
        raise RuntimeError("Could not get PostgreSQL credentials")

    return result


print(get_postgres_rec())