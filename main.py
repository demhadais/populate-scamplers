import asyncio
from pydantic_settings import CliApp
from app import Settings


async def main() -> None:
    CliApp.run(Settings)


if __name__ == "__main__":
    asyncio.run(main())
