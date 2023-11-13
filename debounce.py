from datetime import datetime, timedelta
import asyncio
import random
import threading


# so the idea is to wait until the function has not been called for a set amount of delay.
# if the delay expires and the function has not been called again, then i do an api call
class Debounce(object):

    def __init__(self, delay):
        self.delay = timedelta(seconds=delay)
        self.last_call = None
        self.timer = None

    def reset(self):
        self.last_call = None
        return

    def set_delay(self, delay):
        self.delay = timedelta(seconds=delay)

    def __call__(self, func):
        async def wrapped(*args, **kwargs):
            time_called = datetime.now()
            if self.timer:
                self.timer.cancel()
                self.timer = None
            if not self.last_call or time_called - self.last_call > self.delay:
                self.last_call = time_called
                return await func(*args, **kwargs)
            try:
                self.timer = asyncio.create_task(asyncio.sleep(self.delay.total_seconds()))
                await self.timer
                self.last_call = datetime.now()
                return await func(*args, **kwargs)
            except asyncio.CancelledError:
                return None

        return wrapped


debounce = Debounce(1)


@debounce
async def do_something(a):
    return a


async def main():
    random.seed(200)
    for i in range(1, 11):
        print(await do_something(i))
        sleep = round(random.random(), 2)
        print(f"sleeping for {sleep} seconds")
        await asyncio.sleep(sleep)


if __name__ == '__main__':
    asyncio.run(main())
