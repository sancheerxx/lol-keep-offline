# -*- coding: utf-8 -*-
"""
keep_offline_ws.py（v4）

作用：让好友看到你“离线”，并擦掉“队列中 / 英雄选择 / 游戏中 / 正在创建房间”。

v4 改动：
  1. WebSocket 事件驱动：收到 lol-chat/me 或 gameflow-phase 事件时**立刻**校正，
     不再等定时器 —— 进对局/切阶段时不会再闪一下“游戏中”。
  2. 定时抢占间隔 0.5s -> 0.2s，且按阶段自适应：大厅/队列/选人/对局里 0.2s，
     空闲时 1s，避免无意义请求。
  3. 校正日志节流（1 秒内多次合并成一条带次数的），便于看出客户端是否在持续覆盖。

用法：
    python keep_offline_ws_fixed.py       # 交互选择
    python keep_offline_ws_fixed.py 5     # 直接锁定“离线”
"""
import asyncio
import base64
import contextlib
import json
import re
import sys
import time

import aiohttp
import psutil

AVAILABILITY_OPTIONS = {
    "1": ("chat", "在线"),
    "2": ("away", "离开"),
    "3": ("dnd", "忙碌"),
    "4": ("mobile", "手机在线"),
    "5": ("offline", "离线"),
}

ACTIVE_PHASES = {"Lobby", "Matchmaking", "ChampSelect", "GameStart",
                 "InProgress", "Reconnect", "EndOfGame", "WaitingForStats",
                 "PreEndOfGame", "None"}
GUARD_ACTIVE = 0.2
GUARD_IDLE = 1.0
WS_IDLE_SECONDS = 300
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=5)

CLEAR_FIELDS = ("gameMode", "gameQueueType", "gameServer",
                "spectatorMode", "championId", "skinname")

EVENTS = (
    "OnJsonApiEvent_lol-gameflow_v1_gameflow-phase",
    "OnJsonApiEvent_lol-chat_v1_me",
)

_state = {"phase": None, "last_phase_log": "", "last_log": 0.0, "suppressed": 0}


def basic_auth(login, password):
    raw = f"{login}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def get_lcu_credentials():
    try:
        for proc in psutil.process_iter(["name", "cmdline"]):
            name = proc.info.get("name") or ""
            if "LeagueClientUx" not in name:
                continue
            args = " ".join(proc.info.get("cmdline") or [])
            port = re.search(r"--app-port=(\d+)", args)
            token = re.search(r"--remoting-auth-token=([^\s\"]+)", args)
            if port and token:
                return port.group(1), token.group(1)
    except (psutil.Error, OSError):
        pass
    return None


def choose_availability():
    print("\n===== 请选择要维持的状态 =====")
    for key, (_, label) in AVAILABILITY_OPTIONS.items():
        print(f"  {key}. {label}")
    while True:
        choice = input("请输入数字 (1-5): ").strip()
        if choice in AVAILABILITY_OPTIONS:
            return AVAILABILITY_OPTIONS[choice][0]
        print("输入无效，请重试")


def log_correction(parts):
    """1 秒内多次校正合并打印，避免刷屏但保留次数。"""
    now = time.monotonic()
    if now - _state["last_log"] < 1.0:
        _state["suppressed"] += 1
        return
    extra = f"（另有 {_state['suppressed']} 次，1 秒内合并）" if _state["suppressed"] else ""
    _state["last_log"] = now
    _state["suppressed"] = 0
    print("[keep] 校正 " + "，".join(parts) + extra)


async def _request(http, method, url, headers, payload=None):
    kwargs = {"headers": headers, "ssl": False, "timeout": HTTP_TIMEOUT}
    if payload is not None:
        kwargs["json"] = payload
    async with http.request(method, url, **kwargs) as resp:
        return resp.status, await resp.text()


async def get_me(http, base, headers):
    try:
        status, body = await _request(http, "GET", base + "/lol-chat/v1/me", headers)
    except Exception:
        return None
    if status != 200:
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return None


async def put_me(http, base, headers, payload):
    try:
        return await _request(http, "PUT", base + "/lol-chat/v1/me", headers, payload)
    except Exception as exc:
        return -1, repr(exc)


def scrub_payload(me, availability):
    payload = {"availability": availability}
    lol = dict(me.get("lol") or {})
    if lol.get("gameStatus") != "outOfGame":
        lol["gameStatus"] = "outOfGame"
        for key in CLEAR_FIELDS:
            if key in lol:
                lol[key] = ""
    payload["lol"] = lol
    return payload


async def correct(http, base, headers, availability, me=None):
    """把 presence 校正回目标状态；me 可传入事件里的最新对象省一次 GET。"""
    if me is None:
        me = await get_me(http, base, headers)
        if me is None:
            return None

    avail_before = me.get("availability")
    lol = me.get("lol") or {}
    status_before = lol.get("gameStatus")

    payload = scrub_payload(me, availability)
    code, body = await put_me(http, base, headers, payload)

    if code not in (200, 201, 204) and "lol" in payload:
        print(f"[keep] 改写 gameStatus 被拒（HTTP {code}），后续只维持离线")
        code, body = await put_me(http, base, headers, {"availability": availability})
        if code in (200, 201, 204):
            return None
        print(f"[keep] 校正失败 HTTP {code}: {str(body)[:120]}")
        return None

    if code not in (200, 201, 204):
        print(f"[keep] 校正失败 HTTP {code}: {str(body)[:120]}")
        return None

    parts = []
    if avail_before != availability:
        parts.append(f"availability {avail_before!r} -> {availability!r}")
    if status_before != "outOfGame":
        parts.append(f"gameStatus {status_before!r} -> 'outOfGame'")
    if parts:
        log_correction(parts)
    return True


async def guard_loop(http, base, headers, availability):
    failures = 0
    while True:
        phase = _state["phase"]
        await asyncio.sleep(GUARD_ACTIVE if phase in ACTIVE_PHASES else GUARD_IDLE)

        me = await get_me(http, base, headers)
        if me is None:
            # 选人/加载阶段 LCU 会短暂卡顿，偶发读失败不算客户端关闭
            failures += 1
            if failures == 1:
                print("[keep] 暂时读取不到状态，继续重试…")
            if failures >= 10:
                print("[keep] 连续读取失败，重建连接")
                return
            await asyncio.sleep(0.5)
            continue
        failures = 0

        avail = me.get("availability")
        status = (me.get("lol") or {}).get("gameStatus")
        if avail == availability and status == "outOfGame":
            continue

        await correct(http, base, headers, availability, me)


async def handle_ws(ws, http, base, headers, availability):
    last_correct = 0.0
    while True:
        try:
            msg = await asyncio.wait_for(ws.receive(), timeout=WS_IDLE_SECONDS)
        except asyncio.TimeoutError:
            return

        if msg.type != aiohttp.WSMsgType.TEXT:
            if msg.type in (aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.ERROR):
                return
            continue

        try:
            data = json.loads(msg.data)
            if not isinstance(data, list) or len(data) < 3 or data[0] != 8:
                continue
            evt = data[2]
        except (json.JSONDecodeError, TypeError, IndexError, AttributeError):
            continue

        uri = evt.get("uri", "")
        payload = evt.get("data", "")

        if "gameflow-phase" in uri:
            now = time.monotonic()
            if payload != _state["last_phase_log"]:
                _state["last_phase_log"] = payload
                print(f"[keep] 游戏阶段: {payload}")
            _state["phase"] = payload
            # 阶段切换时客户端会重推 presence，立刻抢一次（不等定时器）
            await correct(http, base, headers, availability)
            last_correct = now

        elif "lol-chat/v1/me" in uri and isinstance(payload, dict):
            avail = payload.get("availability")
            status = (payload.get("lol") or {}).get("gameStatus")
            if avail == availability and status == "outOfGame":
                continue
            now = time.monotonic()
            if now - last_correct < 0.05:
                continue
            last_correct = now
            await correct(http, base, headers, availability, payload)


async def ws_loop(ws_http, port, http, base, headers, availability):
    url = f"wss://127.0.0.1:{port}/"
    backoff = 2
    last_notice = 0.0
    while True:
        try:
            async with ws_http.ws_connect(url, headers=headers, ssl=False) as ws:
                for evt in EVENTS:
                    await ws.send_str(json.dumps([5, evt]))
                print("[keep] WebSocket 已连接，开始监听")
                backoff = 2
                await handle_ws(ws, http, base, headers, availability)
            print("[keep] WebSocket 断开")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            now = time.monotonic()
            if now - last_notice > 30:
                print(f"[keep] WebSocket 不可用（HTTP 轮询仍在维持）: "
                      f"{type(exc).__name__}: {str(exc)[:140]}")
                last_notice = now
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)


async def session_loop(port, token, availability):
    base = f"https://127.0.0.1:{port}"
    headers = {"Authorization": basic_auth("riot", token)}

    async with aiohttp.ClientSession() as http:
        me = None
        for _ in range(4):
            me = await get_me(http, base, headers)
            if me is not None:
                break
            await asyncio.sleep(0.5)
        if me is None:
            return
        print(f"[keep] 当前: availability={me.get('availability')!r} "
              f"gameStatus={(me.get('lol') or {}).get('gameStatus')!r}")
        if not await correct(http, base, headers, availability, me):
            return
        print(f"[keep] 已锁定 {availability}，对局文字已清除")

        connector = aiohttp.TCPConnector(force_close=True)
        async with aiohttp.ClientSession(connector=connector) as ws_http:
            guard = asyncio.create_task(guard_loop(http, base, headers, availability))
            watcher = asyncio.create_task(
                ws_loop(ws_http, port, http, base, headers, availability))

            done, pending = await asyncio.wait(
                {guard, watcher}, return_when=asyncio.FIRST_COMPLETED)

            for task in pending:
                task.cancel()
            for task in pending:
                with contextlib.suppress(asyncio.CancelledError):
                    await task


async def main():
    availability = None
    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
        if choice in AVAILABILITY_OPTIONS:
            availability = AVAILABILITY_OPTIONS[choice][0]
    if availability is None:
        availability = choose_availability()

    print(f"[keep] 目标状态: {availability}，Ctrl+C 退出")

    reported_waiting = False
    while True:
        creds = get_lcu_credentials()
        if not creds:
            if not reported_waiting:
                print("[keep] 未检测到客户端（LeagueClientUx.exe），等待中…")
                reported_waiting = True
            await asyncio.sleep(2)
            continue

        reported_waiting = False
        port, token = creds
        print(f"[keep] 已定位客户端，端口 {port}")

        try:
            await session_loop(port, token, availability)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[keep] 会话异常: {exc!r}")

        await asyncio.sleep(2)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[keep] 已退出")
