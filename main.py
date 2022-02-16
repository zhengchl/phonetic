import logging
import asyncio
import time
from mark_phonetic.mark_phonetic import main

if __name__ == '__main__':
    import sys
    filename = sys.argv[1]

    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    start = time.perf_counter()
    asyncio.run(main(filename))
    elapsed = time.perf_counter() - start
    logging.info(f"Program completed in {elapsed:0.5f} seconds.")