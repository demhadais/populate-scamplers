import asyncio

from app import update_scamplers_api


async def main() -> None:
    await update_scamplers_api()


if __name__ == "__main__":
    asyncio.run(main())
