#!/usr/bin/python3
import asyncio
import logging
import foo
logging.basicConfig(level=logging.DEBUG)
asyncio.run(foo.Split_Reviews(1382330).main_loop())
