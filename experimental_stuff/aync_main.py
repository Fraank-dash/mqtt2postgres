# https://docs.python.org/3/library/asyncio-dev.html#concurrency-and-multithreading
import asyncio

async def main():
    print("Hello...")
    await asyncio.sleep(1)
    print("...World")

if __name__ == "__main__":
    asyncio.run(main())