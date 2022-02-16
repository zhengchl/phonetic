import pytest
import aiosqlite
import asyncio


async def iterdump():
    async with aiosqlite.connect(":memory:") as db:
        await db.execute("create table foo (i integer, k charvar(250))")
        await db.executemany(
            "insert into foo values (?, ?)", [(1, "hello"), (2, "world")]
        )
        query_k = ['hello', 'no']
        async with db.execute(f"select * from foo where i in ({', '.join('?' for _ in query_k)})",
                              query_k) as cursor:
            async for row in cursor:
                assert row['i'] == 1


        lines = [line async for line in db.iterdump()]
        assert lines == [
                "BEGIN TRANSACTION;",
                "CREATE TABLE foo (i integer, k charvar(250));",
                "INSERT INTO \"foo\" VALUES(1,'hello');",
                "INSERT INTO \"foo\" VALUES(2,'world');",
                "COMMIT;",
            ]

def test_async():
    asyncio.run(iterdump())