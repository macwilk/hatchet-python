import asyncio
import json
import os

import httpx
from dotenv import load_dotenv

from hatchet_sdk import Context, Hatchet
from hatchet_sdk.worker.aio_timing import print_timing

load_dotenv()

hatchet = Hatchet(debug=True)

os.environ["PYTHONASYNCIODEBUG"] = "1"
riza_key = "riza_01J4QA9K2BN84Z1E3PWTMDZMC1_01J4QA9VWNFQRXBSBJFH2WS76Z"


class CodeExecutor:
    def __init__(self):
        self.client = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=1000, max_keepalive_connections=200)
        )
        self.url = "https://api.riza.io/v1/execute"
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {riza_key}",
        }

    async def execute_code(self, code: str) -> dict:
        data = {
            "language": "PYTHON",
            "code": code,
        }
        response = await self.client.post(self.url, headers=self.headers, json=data)
        return response.json()

    async def close(self):
        """
        I'm not actually running this but I should. Couldn't find where to throw it in for the
        toy demo.
        """
        await self.client.aclose()


@hatchet.workflow(on_events=["parent:create"])
class Parent:
    @hatchet.step(timeout="5m")
    async def spawn(self, context: Context):
        # print("spawning child")

        context.put_stream("spawning...")
        results = []

        n = context.workflow_input().get("n", 10000)

        for i in range(n):
            results.append(
                (
                    await context.aio.spawn_workflow(
                        "Child",
                        {"a": str(i)},
                        key=f"child{i}",
                        options={"additional_metadata": {"hello": "earth"}},
                    )
                ).result()
            )
        with print_timing("gather"):
            result = await asyncio.gather(*results)

        return {"results": result}


@hatchet.workflow(on_events=["child:create"])
class Child:
    def __init__(self, executor: CodeExecutor = CodeExecutor()):
        self.code_executor = executor

    @hatchet.step()
    async def process(self, context: Context):
        a = context.workflow_input()["a"]
        # print(f"child process {a}")
        command_exec_response = await self.code_executor.execute_code(
            "print('Hello world -- child 1!')"
        )
        with print_timing("put stream 1"):
            context.put_stream(f"child 1... {json.dumps(dict(command_exec_response))}")
        return {"status": "success " + json.dumps(dict(command_exec_response))}

    @hatchet.step()
    async def process2(self, context: Context):
        # print("child process2")
        command_exec_response = await self.code_executor.execute_code("print(1*3)")

        with print_timing("put stream 2"):
            context.put_stream(f"child 2... {json.dumps(dict(command_exec_response))}")
        return {"status2": "success " + json.dumps(dict(command_exec_response))}


def main():
    worker = hatchet.worker("fanout-worker", max_runs=800)
    worker.register_workflow(Parent())
    worker.register_workflow(Child())
    worker.start()


if __name__ == "__main__":
    main()
