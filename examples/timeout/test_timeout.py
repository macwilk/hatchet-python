from hatchet_sdk import Hatchet
import pytest

from tests.utils import fixture_bg_worker
from tests.utils.hatchet_client import hatchet_client_fixture


hatchet = hatchet_client_fixture()
worker = fixture_bg_worker(["poetry", "run", "timeout"])

# requires scope module or higher for shared event loop
@pytest.mark.asyncio(scope="session")
async def test_run_timeout(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("TimeoutWorkflow", {})
    try:
        await run.result()
        assert False, "Expected workflow to timeout"
    except Exception as e:
        assert str(e) == "Workflow Errors: ['TIMED_OUT']"

@pytest.mark.asyncio(scope="session")
async def test_run_refresh_timeout(hatchet: Hatchet):
    run = hatchet.admin.run_workflow("RefreshTimeoutWorkflow", {})
    result = await run.result()
    assert result["step1"]["status"] == "success"

    

