import asyncio
import os
from time import time
from aiohttp import ClientSession

from bot import (
    LOGGER,
    bot_start_time,
    non_queued_dl,
    non_queued_up,
    queued_dl,
    queued_up,
    task_dict,
    task_dict_lock,
)


def _get_val(key, default=""):
    val = os.environ.get(key)
    if val is not None and str(val).strip():
        return str(val).strip()
    try:
        from bot.core.config_manager import Config
        cfg_val = getattr(Config, key, None)
        if cfg_val is not None and str(cfg_val).strip():
            return str(cfg_val).strip()
    except Exception:
        pass
    return default


def _get_timeout():
    raw = (
        os.environ.get("AMATERASU_IDLE_TIMEOUT")
        or os.environ.get("IDLE_TIMEOUT")
        or os.environ.get("WZML_X_IDLE_TIMEOUT")
        or "300"
    )
    try:
        return int(raw)
    except (ValueError, TypeError):
        return 300


IDLE_TIMEOUT = _get_timeout()
NORTHFLANK_API_TOKEN = _get_val("NORTHFLANK_API_TOKEN")
NORTHFLANK_PROJECT_ID = _get_val("NORTHFLANK_PROJECT_ID")
NORTHFLANK_SERVICE_ID = _get_val("NORTHFLANK_SERVICE_ID")
NORTHFLANK_SERVICE_TYPE = _get_val("NORTHFLANK_SERVICE_TYPE", "combined")


async def scale_to_zero():
    token = _get_val("NORTHFLANK_API_TOKEN")
    project_id = _get_val("NORTHFLANK_PROJECT_ID")
    service_id = _get_val("NORTHFLANK_SERVICE_ID")
    service_type = _get_val("NORTHFLANK_SERVICE_TYPE", "combined")

    if not all([token, project_id, service_id]):
        LOGGER.error("Missing Northflank API credentials. Cannot scale to zero.")
        return False

    url = f"https://api.northflank.com/v1/projects/{project_id}/services/{service_type}/{service_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    payload = {"deployment": {"instances": 0}}

    try:
        LOGGER.info("Scaling Amaterasu from 1 to 0")
        async with ClientSession() as session:
            async with session.patch(url, headers=headers, json=payload) as response:
                if response.status in [200, 201, 202, 204]:
                    LOGGER.info("Northflank scale request succeeded. Amaterasu scaled to 0")
                    return True
                elif response.status == 404 and service_type == "combined":
                    # Fallback to deployment service endpoint if combined returns 404
                    fallback_url = f"https://api.northflank.com/v1/projects/{project_id}/services/deployment/{service_id}"
                    async with session.patch(fallback_url, headers=headers, json=payload) as fb_resp:
                        if fb_resp.status in [200, 201, 202, 204]:
                            LOGGER.info("Northflank scale request succeeded (via deployment endpoint). Amaterasu scaled to 0")
                            return True
                        LOGGER.error(f"Failed to scale down via fallback. Status: {fb_resp.status}")
                        return False
                else:
                    text = await response.text()
                    LOGGER.error(f"Failed to scale down. Status: {response.status}, Response: {text}")
                    return False
    except Exception as e:
        LOGGER.error(f"Error during scale to zero request: {e}")
        return False


async def check_idle_loop():
    timeout = _get_timeout()
    if timeout <= 0:
        LOGGER.info("Idle timeout is disabled (IDLE_TIMEOUT <= 0).")
        return

    token = _get_val("NORTHFLANK_API_TOKEN")
    project_id = _get_val("NORTHFLANK_PROJECT_ID")
    service_id = _get_val("NORTHFLANK_SERVICE_ID")

    if not all([token, project_id, service_id]):
        LOGGER.info(
            "Idle manager inactive: set NORTHFLANK_API_TOKEN, NORTHFLANK_PROJECT_ID, "
            "and NORTHFLANK_SERVICE_ID in environment variables to enable auto scale-to-zero."
        )
        return

    LOGGER.info(f"Idle manager started: will scale to 0 after {timeout}s of inactivity.")

    # Wait for the startup grace period to finish (60 seconds)
    while time() - bot_start_time < 60:
        await asyncio.sleep(5)

    idle_start_time = None

    while True:
        await asyncio.sleep(10)

        async with task_dict_lock:
            is_active = (
                len(task_dict) > 0
                or len(queued_dl) > 0
                or len(queued_up) > 0
                or len(non_queued_dl) > 0
                or len(non_queued_up) > 0
            )

        # Check for active FileToLink streaming transfers
        if not is_active:
            try:
                from web.wserver import _filetolink_active_streams
                if len(_filetolink_active_streams) > 0:
                    is_active = True
            except Exception:
                pass

        if is_active:
            if idle_start_time is not None:
                LOGGER.info("Amaterasu is active again. Resetting idle timer.")
            idle_start_time = None
        else:
            if idle_start_time is None:
                idle_start_time = time()
                LOGGER.info(f"Amaterasu is idle. Timer started: {timeout}s")
            else:
                elapsed = time() - idle_start_time
                if elapsed >= timeout:
                    LOGGER.info(f"Amaterasu idle timeout reached for {elapsed:.2f}s.")
                    success = await scale_to_zero()
                    if success:
                        LOGGER.info("Idle manager stopping after successful scale-to-zero request.")
                        return


_idle_task = None


def start_idle_manager():
    global _idle_task
    from bot import bot_loop

    _idle_task = bot_loop.create_task(check_idle_loop())
