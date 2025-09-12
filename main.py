from scamplepy import ScamplersClient
import asyncio
from pydantic_settings import BaseSettings, CliPositionalArg, SettingsConfigDict
from pathlib import Path

class Config(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SCAMPLERS_SPREADSHEET_", cli_parse_args=True, cli_kebab_case=True, cli_enforce_required=True)

    csv_dir: CliPositionalArg[Path]
    dataset_dirs: CliPositionalArg[list[Path]]
    csv_renaming: dict[str, str] = {}
    dry_run: bool = False
    print_requests: bool = False
    save_requests: Path | None = None
    print_responses: bool = False
    save_responses: Path | None = None

async def main():
    config = Config()


if __name__ == "__main__":
    asyncio.run(main())
