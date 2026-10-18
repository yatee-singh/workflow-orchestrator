import asyncio

from workers._stub import run_stub


def main() -> None:
    asyncio.run(run_stub("worker-shipping"))


if __name__ == "__main__":
    main()
