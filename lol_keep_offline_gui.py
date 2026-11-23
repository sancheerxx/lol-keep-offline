# -*- coding: utf-8 -*-
"""
LOL 保持离线 · 图形版

一个窗口搞定：选状态 -> 点开始 -> 看日志。关窗口即退出。
引擎逻辑与命令行版一致（LCU 接口 + WebSocket 事件 + 定时抢占）。
"""
import asyncio
import base64
import contextlib
import json
import os
import queue
import re
import sys
import threading
import time
import tkinter as tk
from tkinter import font as tkfont

import aiohttp
import psutil

# ---------------- 主题 ----------------
BG = "#14161d"
PANEL = "#1c1f28"
PANEL_HI = "#252a36"
FG = "#e6e9ef"
FG_DIM = "#8b93a7"
ACCENT = "#4c8dff"
OK = "#3ecf8e"
WARN = "#f2b544"
ERR = "#ff6b6b"
BORDER = "#2e3441"

AVAILABILITY_OPTIONS = [
    ("offline", "离线", "好友看到灰点，完全隐身"),
    ("chat", "在线", "正常在线"),
    ("away", "离开", "黄点 + 离开"),
]

ACTIVE_PHASES = {"Lobby", "Matchmaking", "ChampCheck", "ChampSelect", "GameStart",
                 "InProgress", "Reconnect", "EndOfGame", "WaitingForStats",
                 "PreEndOfGame", "ReadyCheck", "None", "CheckIntent"}
GUARD_ACTIVE = 0.2
GUARD_IDLE = 1.0
WS_IDLE_SECONDS = 5
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=5)
CLEAR_FIELDS = ("gameMode", "gameQueueType", "gameServer",
                "spectatorMode", "championId", "skinname")
EVENTS = (
    "OnJsonApiEvent_lol-gameflow_v1_gameflow-phase",
    "OnJsonApiEvent_lol-chat_v1_me",
)

CONFIG_PATH = os.path.join(
    os.environ.get("LOCALAPPDATA") or os.path.expanduser("~"),
    "LolKeepOffline", "config.json")

AUTOSTART_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
AUTOSTART_NAME = "LolKeepOffline"


def _launch_command():
    """开机自启时写入注册表的命令行。"""
    if getattr(sys, "frozen", False):
        return f'"{sys.executable}"'
    script = os.path.abspath(__file__)
    return f'"{sys.executable}" "{script}"'


def autostart_enabled():
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, AUTOSTART_KEY) as key:
            value, _ = winreg.QueryValueEx(key, AUTOSTART_NAME)
            return bool(value)
    except (OSError, ImportError):
        return False


def set_autostart(enable):
    """写入/删除 用户级开机自启项，返回是否成功。"""
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, AUTOSTART_KEY,
                            access=winreg.KEY_SET_VALUE) as key:
            if enable:
                winreg.SetValueEx(key, AUTOSTART_NAME, 0, winreg.REG_SZ,
                                  _launch_command())
            else:
                try:
                    winreg.DeleteValue(key, AUTOSTART_NAME)
                except FileNotFoundError:
                    pass
        return True
    except (OSError, ImportError):
        return False


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


# ---------------- 引擎 ----------------
class Engine:
    """在后台线程里跑 asyncio 循环；通过 log() 回调把消息送回界面。"""

    def __init__(self, log, on_state):
        self._log = log
        self._on_state = on_state      # 回调：('running'|'waiting'|'stopped', 文案)
        self.availability = "offline"
        self.stats = {"corrections": 0}
        self._stop = threading.Event()
        self._thread = None
        self._loop = None
        self._task = None

    # ---- 线程管理 ----
    def start(self, availability):
        if self._thread and self._thread.is_alive():
            return False
        self.availability = availability
        self.stats["corrections"] = 0
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        return True

    def stop(self):
        self._stop.set()
        loop, task = self._loop, self._task
        if loop and task:
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(task.cancel)

    def running(self):
        return bool(self._thread and self._thread.is_alive())

    def _run(self):
        try:
            asyncio.run(self._main())
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._log(f"引擎异常: {exc!r}", "err")
        finally:
            self._on_state("stopped", "已停止")

    # ---- 异步主体 ----
    async def _main(self):
        self._loop = asyncio.get_running_loop()
        self._task = asyncio.current_task()

        reported = False
        while not self._stop.is_set():
            creds = get_lcu_credentials()
            if not creds:
                if not reported:
                    self._on_state("waiting", "等待客户端启动并登录…")
                    self._log("未检测到客户端，等待中（启动并登录英雄联盟即可）", "warn")
                    reported = True
                await asyncio.sleep(1.5)
                continue

            reported = False
            port, token = creds
            self._log(f"已连接客户端，端口 {port}", "dim")
            self._on_state("running", "运行中 · 已锁定状态")

            try:
                await self._session(port, token)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log(f"连接异常，2 秒后重试: {exc!r}", "warn")

            await asyncio.sleep(2)

    async def _session(self, port, token):
        base = f"https://127.0.0.1:{port}"
        headers = {"Authorization": basic_auth("riot", token)}
        avail = self.availability

        async with aiohttp.ClientSession() as http:
            me = None
            for _ in range(4):
                me = await self._get_me(http, base, headers)
                if me is not None:
                    break
                await asyncio.sleep(0.4)
            if me is None:
                self._log("读取账号信息失败，稍后重试", "warn")
                return

            await self._correct(http, base, headers, me)
            self._log(f"已锁定「{self._label(avail)}」，对局文字已清除", "ok")

            connector = aiohttp.TCPConnector(force_close=True)
            async with aiohttp.ClientSession(connector=connector) as ws_http:
                guard = asyncio.create_task(self._guard(http, base, headers))
                watcher = asyncio.create_task(
                    self._ws_loop(ws_http, port, http, base, headers))
                _, pending = await asyncio.wait(
                    {guard, watcher}, return_when=asyncio.FIRST_COMPLETED)
                for task in pending:
                    task.cancel()
                for task in pending:
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

    def _label(self, value):
        for key, label, _ in AVAILABILITY_OPTIONS:
            if key == value:
                return label
        return value

    async def _request(self, http, method, url, headers, payload=None):
        kwargs = {"headers": headers, "ssl": False, "timeout": HTTP_TIMEOUT}
        if payload is not None:
            kwargs["json"] = payload
        async with http.request(method, url, **kwargs) as resp:
            return resp.status, await resp.text()

    async def _get_me(self, http, base, headers):
        try:
            status, body = await self._request(http, "GET", base + "/lol-chat/v1/me", headers)
        except Exception:
            return None
        if status != 200:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return None

    async def _put_me(self, http, base, headers, payload):
        try:
            return await self._request(http, "PUT", base + "/lol-chat/v1/me", headers, payload)
        except Exception as exc:
            return -1, repr(exc)

    def _payload(self, me, availability):
        payload = {"availability": availability}
        lol = dict(me.get("lol") or {})
        if lol.get("gameStatus") != "outOfGame":
            lol["gameStatus"] = "outOfGame"
            for key in CLEAR_FIELDS:
                if key in lol:
                    lol[key] = ""
        payload["lol"] = lol
        return payload

    async def _correct(self, http, base, headers, me=None):
        if me is None:
            me = await self._get_me(http, base, headers)
            if me is None:
                return False

        avail_before = me.get("availability")
        status_before = (me.get("lol") or {}).get("gameStatus")
        payload = self._payload(me, self.availability)
        code, body = await self._put_me(http, base, headers, payload)

        if code not in (200, 201, 204) and "lol" in payload:
            self._log(f"该客户端拒绝改写对局状态（HTTP {code}），改为只维持在线状态", "warn")
            code, body = await self._put_me(
                http, base, headers, {"availability": self.availability})
            if code in (200, 201, 204):
                return True

        if code not in (200, 201, 204):
            self._log(f"校正失败 HTTP {code}: {str(body)[:120]}", "err")
            return False

        parts = []
        if avail_before != self.availability:
            parts.append(f"状态 {avail_before} → {self.availability}")
        if status_before != "outOfGame":
            parts.append(f"对局文字 {status_before} → outOfGame")
        if parts:
            self.stats["corrections"] += 1
            note = "" if self.stats["corrections"] % 5 else f"（累计 {self.stats['corrections']} 次）"
            self._log("校正：" + "，".join(parts) + note, "info")
        return True

    async def _guard(self, http, base, headers):
        failures = 0
        phase = "None"
        while True:
            await asyncio.sleep(GUARD_ACTIVE if phase in ACTIVE_PHASES else GUARD_IDLE)
            if self._stop.is_set():
                return
            me = await self._get_me(http, base, headers)
            if me is None:
                failures += 1
                if failures == 3:
                    self._log("暂时读不到客户端状态，继续重试…", "dim")
                if failures >= 10:
                    self._log("连续读取失败，重建连接", "warn")
                    return
                await asyncio.sleep(0.5)
                continue
            failures = 0
            if (me.get("availability") == self.availability
                    and (me.get("lol") or {}).get("gameStatus") == "outOfGame"):
                continue
            await self._correct(http, base, headers, me)

    async def _handle_ws(self, ws, http, base, headers):
        last = 0.0
        last_phase = None
        while not self._stop.is_set():
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=WS_IDLE_SECONDS)
            except asyncio.TimeoutError:
                continue
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
                if payload != last_phase:
                    last_phase = payload
                    shown = {"None": "大厅", "Lobby": "房间中",
                             "Matchmaking": "队列中", "ReadyCheck": "确认对局",
                             "ChampSelect": "英雄选择", "InProgress": "对局中",
                             "WaitingForStats": "结算中", "EndOfGame": "结算中",
                             "GameStart": "进入对局"}.get(payload, payload)
                    self._log(f"阶段：{shown}", "dim")
                await self._correct(http, base, headers)
                last = time.monotonic()

            elif "lol-chat/v1/me" in uri and isinstance(payload, dict):
                if (payload.get("availability") == self.availability
                        and (payload.get("lol") or {}).get("gameStatus") == "outOfGame"):
                    continue
                now = time.monotonic()
                if now - last < 0.05:
                    continue
                last = now
                await self._correct(http, base, headers, payload)

    async def _ws_loop(self, ws_http, port, http, base, headers):
        url = f"wss://127.0.0.1:{port}/"
        backoff = 2
        while not self._stop.is_set():
            try:
                async with ws_http.ws_connect(url, headers=headers, ssl=False) as ws:
                    for evt in EVENTS:
                        await ws.send_str(json.dumps([5, evt]))
                    self._log("实时监听已就绪", "ok")
                    backoff = 2
                    await self._handle_ws(ws, http, base, headers)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log(f"实时监听中断，改用轮询维持: {type(exc).__name__}", "dim")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ---------------- 界面 ----------------
class App:
    def __init__(self, root):
        self.root = root
        self.log_queue = queue.Queue()
        self.state_queue = queue.Queue()
        self.engine = Engine(self.push_log, self.on_state)
        self.config = self.load_config()
        saved = self.config.get("availability", "offline")
        if not any(k == saved for k, _, _ in AVAILABILITY_OPTIONS):
            saved = "offline"
        self.selected = tk.StringVar(value=saved)
        self.auto_start = tk.BooleanVar(value=self.config.get("auto_start", True))
        self.os_autostart = tk.BooleanVar(value=autostart_enabled())
        self.state = "stopped"
        self._last_idle_check = 0.0
        self._client_seen = False      # 上一次探测是否发现客户端
        self._user_stopped = False     # 用户手动停止过，客户端重启前不自动开始
        self._build()
        self.root.after(120, self.drain_logs)

    # ---------- 配置 ----------
    def load_config(self):
        try:
            with open(CONFIG_PATH, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                return {}
            return data
        except (OSError, json.JSONDecodeError):
            return {}

    def save_config(self):
        payload = {
            "availability": self.selected.get(),
            "auto_start": bool(self.auto_start.get()),
        }
        try:
            os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
        except OSError:
            pass

    # ---------- 构建 ----------
    def _build(self):
        self.root.title("LOL 保持离线")
        self.root.configure(bg=BG)
        self.root.geometry("700x720")
        self.root.minsize(660, 700)

        ui_font = self._font("Microsoft YaHei UI", 10)
        title_font = self._font("Microsoft YaHei UI", 17, "bold")
        mono_font = self._font("Consolas", 9)

        # 顶部
        header = tk.Frame(self.root, bg=BG)
        header.pack(fill="x", padx=22, pady=(20, 6))
        tk.Label(header, text="英雄联盟 · 保持离线", bg=BG, fg=FG,
                 font=title_font).pack(anchor="w")
        tk.Label(header, text="锁定隐身状态，好友看不到你在队列、选人或游戏中",
                 bg=BG, fg=FG_DIM, font=ui_font).pack(anchor="w", pady=(4, 0))

        # 状态卡片
        card = tk.Frame(self.root, bg=PANEL, highlightbackground=BORDER,
                        highlightthickness=1)
        card.pack(fill="x", padx=22, pady=14)

        tk.Label(card, text="要维持的状态", bg=PANEL, fg=FG_DIM,
                 font=ui_font).pack(anchor="w", padx=16, pady=(14, 8))

        grid = tk.Frame(card, bg=PANEL)
        grid.pack(fill="x", padx=12)
        self.buttons = {}
        for index, (key, label, desc) in enumerate(AVAILABILITY_OPTIONS):
            box = tk.Frame(grid, bg=PANEL)
            box.grid(row=index // 3, column=index % 3, sticky="ew",
                     padx=4, pady=4)
            btn = tk.Label(box, text=label, bg=PANEL_HI, fg=FG, font=ui_font,
                           pady=8, cursor="hand2")
            btn.pack(fill="x")
            btn.bind("<Button-1>", lambda _e, k=key: self.select(k))
            self.buttons[key] = btn
        for col in range(3):
            grid.columnconfigure(col, weight=1, uniform="col")

        self.desc_label = tk.Label(card, text="", bg=PANEL, fg=FG_DIM,
                                   font=self._font("Microsoft YaHei UI", 9))
        self.desc_label.pack(anchor="w", padx=16, pady=(10, 8))

        # 设置项
        opts = tk.Frame(card, bg=PANEL)
        opts.pack(fill="x", padx=16, pady=(0, 14))
        self.auto_check = tk.Checkbutton(
            opts, text="客户端启动后自动开始保持", variable=self.auto_start,
            command=self.on_toggle_auto, bg=PANEL, fg=FG, font=ui_font,
            activebackground=PANEL, activeforeground=FG, selectcolor=PANEL_HI,
            bd=0, highlightthickness=0, anchor="w")
        self.auto_check.pack(anchor="w")
        self.os_check = tk.Checkbutton(
            opts, text="开机自动启动本程序", variable=self.os_autostart,
            command=self.on_toggle_os_autostart, bg=PANEL, fg=FG, font=ui_font,
            activebackground=PANEL, activeforeground=FG, selectcolor=PANEL_HI,
            bd=0, highlightthickness=0, anchor="w")
        self.os_check.pack(anchor="w", pady=(4, 0))

        # 操作区
        action = tk.Frame(self.root, bg=BG)
        action.pack(fill="x", padx=22)

        self.main_btn = tk.Label(action, text="开始保持", bg=ACCENT, fg="#ffffff",
                                 font=self._font("Microsoft YaHei UI", 12, "bold"),
                                 pady=11, cursor="hand2")
        self.main_btn.pack(side="left", fill="x", expand=True)
        self.main_btn.bind("<Button-1>", lambda _e: self.toggle())

        # 指示灯
        status_row = tk.Frame(self.root, bg=BG)
        status_row.pack(fill="x", padx=22, pady=(12, 4))
        self.dot = tk.Canvas(status_row, width=12, height=12, bg=BG,
                             highlightthickness=0)
        self.dot.pack(side="left", pady=2)
        self.dot_id = self.dot.create_oval(2, 2, 11, 11, fill=FG_DIM, outline="")
        self.status_label = tk.Label(status_row, text="未启动", bg=BG, fg=FG_DIM,
                                     font=self._font("Microsoft YaHei UI", 9))
        self.status_label.pack(side="left", padx=8)
        self.stats_label = tk.Label(status_row, text="", bg=BG, fg=FG_DIM,
                                    font=self._font("Microsoft YaHei UI", 9))
        self.stats_label.pack(side="right")

        # 日志
        log_wrap = tk.Frame(self.root, bg=PANEL, highlightbackground=BORDER,
                            highlightthickness=1)
        log_wrap.pack(fill="both", expand=True, padx=22, pady=(10, 20))
        tk.Label(log_wrap, text="运行日志", bg=PANEL, fg=FG_DIM,
                 font=ui_font).pack(anchor="w", padx=14, pady=(10, 4))

        self.log = tk.Text(log_wrap, bg=PANEL, fg=FG, font=mono_font, bd=0,
                           relief="flat", padx=14, pady=6, height=14,
                           insertbackground=FG, wrap="word")
        self.log.pack(fill="both", expand=True, padx=(2, 2), pady=(0, 8))
        self.log.tag_config("info", foreground="#8fb6ff")
        self.log.tag_config("ok", foreground=OK)
        self.log.tag_config("warn", foreground=WARN)
        self.log.tag_config("err", foreground=ERR)
        self.log.tag_config("dim", foreground=FG_DIM)
        self.log.configure(state="disabled")

        self.select(self.selected.get())
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _font(self, family, size, weight="normal"):
        try:
            return tkfont.Font(family=family, size=size, weight=weight)
        except tk.TclError:
            return tkfont.Font(size=size, weight=weight)

    # ---------- 交互 ----------
    def select(self, key):
        self.selected.set(key)
        for k, btn in self.buttons.items():
            if k == key:
                btn.configure(bg=ACCENT, fg="#ffffff")
            else:
                btn.configure(bg=PANEL_HI, fg=FG)
        desc = next((d for k, _, d in AVAILABILITY_OPTIONS if k == key), "")
        self.desc_label.configure(text=desc)
        if self.state == "stopped":
            self.save_config()

    def on_toggle_auto(self):
        self.save_config()
        if self.auto_start.get():
            self.push_log("已开启：客户端启动后自动开始保持", "dim")
        else:
            self.push_log("已关闭自动开始", "dim")

    def on_toggle_os_autostart(self):
        want = bool(self.os_autostart.get())
        ok = set_autostart(want)
        if ok:
            self.push_log("已设为开机自动启动" if want else "已取消开机自动启动",
                          "dim" if want else "dim")
        else:
            self.os_autostart.set(not want)
            self.push_log("设置开机自启失败（可能被安全软件拦截）", "err")

    def _start_keep(self, reason=""):
        self.save_config()
        if not self.engine.start(self.selected.get()):
            return
        self._user_stopped = False
        self.set_button("running")
        text = f"开始维持「{self._label(self.selected.get())}」"
        if reason:
            text += f"（{reason}）"
        self.push_log(text, "info")

    def toggle(self):
        if self.engine.running():
            self.push_log("正在停止…", "dim")
            self._user_stopped = True
            self.engine.stop()
            self.set_button("stopping")
        else:
            self._start_keep()

    def _label(self, value):
        return next((l for k, l, _ in AVAILABILITY_OPTIONS if k == value), value)

    def set_button(self, state):
        if state == "running":
            self.main_btn.configure(text="停止", bg="#e05c5c")
        elif state == "stopping":
            self.main_btn.configure(text="正在停止…", bg="#5a6272")
        else:
            self.main_btn.configure(text="开始保持", bg=ACCENT)

    def on_state(self, state, text):
        """引擎线程回调：只入队，由主线程消费（Tk 不跨线程）。"""
        self.state_queue.put((state, text))

    def _apply_state(self, state, text):
        self.state = state
        color = {"running": OK, "waiting": WARN}.get(state, FG_DIM)
        self.dot.itemconfigure(self.dot_id, fill=color)
        self.status_label.configure(text=text, fg=color if state != "stopped" else FG_DIM)
        if state == "stopped":
            self.set_button("idle")
        self.stats_label.configure(
            text=f"已校正 {self.engine.stats['corrections']} 次" if state != "stopped" else "")

    def push_log(self, text, tag="info"):
        self.log_queue.put((text, tag))

    def drain_logs(self):
        try:
            while True:
                state, text = self.state_queue.get_nowait()
                self._apply_state(state, text)
        except queue.Empty:
            pass
        try:
            while True:
                text, tag = self.log_queue.get_nowait()
                stamp = time.strftime("%H:%M:%S")
                self.log.configure(state="normal")
                self.log.insert("end", f"{stamp}  ", "dim")
                self.log.insert("end", text + "\n", tag)
                self.log.see("end")
                self.log.configure(state="disabled")
        except queue.Empty:
            pass
        self.stats_label.configure(
            text=f"已校正 {self.engine.stats['corrections']} 次"
            if self.state != "stopped" else "")
        if self.state == "stopped":
            self._check_client_idle()
        self.root.after(120, self.drain_logs)

    def _check_client_idle(self):
        """未运行时轻量探测：既显示就绪状态，也负责“客户端启动即自动开始”。"""
        now = time.monotonic()
        if now - self._last_idle_check < 2.0:
            return
        self._last_idle_check = now

        found = get_lcu_credentials() is not None
        self._client_seen = found

        if not found:
            # 客户端关掉了：重置“手动停止”标记，下次启动仍可自动开始
            self._user_stopped = False
            self.dot.itemconfigure(self.dot_id, fill=FG_DIM)
            self.status_label.configure(text="等待客户端启动…", fg=FG_DIM)
            return

        if self.auto_start.get() and not self._user_stopped and not self.engine.running():
            self._start_keep("检测到客户端")
            return

        self.dot.itemconfigure(self.dot_id, fill=OK)
        self.status_label.configure(text="已检测到客户端，可以开始", fg=OK)

    def on_close(self):
        if self.engine.running():
            self.engine.stop()
        self.root.after(200, self.root.destroy)


def _icon_path():
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    candidate = os.path.join(base, "app.ico")
    return candidate if os.path.exists(candidate) else None


def _show_fatal(message):
    """打包成 exe 后没有控制台，出错时用弹窗提示，避免一闪而过。"""
    try:
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("LOL 保持离线 · 启动失败", message)
        root.destroy()
    except Exception:
        pass


def main():
    try:
        root = tk.Tk()
    except Exception as exc:
        _show_fatal(f"无法创建窗口: {exc!r}")
        return
    try:
        root.tk.call("tk", "scaling", 1.25)
    except tk.TclError:
        pass
    icon = _icon_path()
    if icon:
        try:
            root.iconbitmap(icon)
        except tk.TclError:
            pass
    try:
        App(root)
    except Exception:
        import traceback
        _show_fatal(traceback.format_exc())
        return
    root.mainloop()


if __name__ == "__main__":
    main()
