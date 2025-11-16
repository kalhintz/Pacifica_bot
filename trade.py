#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Pacifica 순차 전략 봇 + 실시간 상태 보드 (수정 버전)

주요 수정사항:
- 시작 시 포지션 체크 강화
- 카운트다운 버그 수정
- 재주문 간격 5초로 변경
- 포지션 감지 로직 개선
"""

import os, json, asyncio, uuid, time, logging, signal, random, shutil
from decimal import Decimal, getcontext, ROUND_FLOOR
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Tuple, List

import requests
import base58
from dotenv import load_dotenv
from solders.keypair import Keypair
import websockets

# ----- (선택) 화면용 rich -----
RICH_AVAILABLE = False
if (os.getenv("PF_STATUS_USE_RICH") or "1") == "1":
    try:
        from rich.live import Live
        from rich.table import Table
        from rich.console import Console
        RICH_AVAILABLE = True
    except Exception:
        RICH_AVAILABLE = False

# ---------- 로깅 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pacifica")
getcontext().prec = 28

# ---------- 상수 ----------
MAINNET_WS = "wss://ws.pacifica.fi/ws"
TESTNET_WS  = "wss://test-ws.pacifica.fi/ws"
MAINNET_REST = "https://api.pacifica.fi/api/v1"
TESTNET_REST = "https://test-api.pacifica.fi/api/v1"

JSON_PING_INTERVAL_SEC = 30
DEFAULT_EXPIRY_MS = 5_000
CANCEL_TIMEOUT_SEC = 2.0

# ---------- 데이터 구조 ----------
@dataclass
class MarketSpec:
    symbol: str
    tick_size: Decimal
    lot_size: Decimal
    min_order_size_usd: Decimal

@dataclass
class InflightOrder:
    cloid: str
    side: str                # "bid" | "ask"
    price: Decimal
    amount: Decimal
    reduce_only: bool
    tif: str                 # "GTC" | "ALO" | "IOC"
    order_id: Optional[int] = None
    filled: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    status: str = "open"     # open/partially_filled/filled/cancelled/rejected
    created_at: float = field(default_factory=time.time)
    last_update_at: float = field(default_factory=time.time)

# ---------- 봇 ----------
class PacificaSequentialBot:
    def __init__(self):
        load_dotenv()

        # 키/환경
        self.private_b58 = os.getenv("PF_PRIVATE_KEY_B58") or os.getenv("PF_PRIVATE_KEY")
        if not self.private_b58:
            raise RuntimeError("PF_PRIVATE_KEY_B58 필요")

        self.api_key = self._ascii_or_none(os.getenv("PF_API_KEY") or "")
        env = (os.getenv("PF_ENV") or "mainnet").lower()
        if env not in ("mainnet", "testnet"):
            raise RuntimeError("PF_ENV 는 mainnet|testnet")
        self.env = env
        self.symbol = (os.getenv("PF_SYMBOL") or "BTC").upper()

        self.notional = Decimal(os.getenv("PF_NOTIONAL_USDC") or "1000")
        self.target_roi_pct = Decimal(os.getenv("PF_TARGET_ROI_PCT") or "1.0")
        self.use_roi = (os.getenv("PF_USE_ROI") or "1") == "1"
        self.hold_min = int(os.getenv("PF_HOLD_MIN_SEC") or "300")
        self.hold_max = int(os.getenv("PF_HOLD_MAX_SEC") or "600")
        if self.hold_max < self.hold_min:
            self.hold_max = self.hold_min

        # 재주문 간격을 5초로 기본 설정
        self.reprice_sec = int(os.getenv("PF_REPRICE_SEC") or "5")

        self.flush_on_start = (os.getenv("PF_FLUSH_ON_START") or "1") == "1"
        self.flush_wait = int(os.getenv("PF_FLUSH_WAIT_SEC") or "120")
        self.reconnect_max_backoff = int(os.getenv("PF_RECONNECT_MAX_BACKOFF") or "30")

        # REST 쓰로틀/쿨다운 파라미터
        self.pos_rest_min = int(os.getenv("PF_POS_REST_MIN_SEC") or "30")
        self.pos_rest_429 = int(os.getenv("PF_POS_REST_429_SEC") or "180")
        self.pos_rest_err = int(os.getenv("PF_POS_REST_ERR_SEC") or "60")
        self.pos_ws_grace = int(os.getenv("PF_POS_WS_GRACE_SEC") or "3")  # 3초로 증가
        self._pos_rest_next_at: float = 0.0
        self._pos_last_source: str = "none"
        self._ws_pos_last_ts: float = 0.0

        # 상태 보드
        self.status_sec = max(1, int(os.getenv("PF_STATUS_SEC") or "1"))
        self.use_rich = RICH_AVAILABLE
        self._live: Optional[Live] = None
        self._console = Console() if self.use_rich else None
        self._status_task: Optional[asyncio.Task] = None

        # 엔드포인트
        self.ws_url = MAINNET_WS if self.env == "mainnet" else TESTNET_WS
        self.rest_base = MAINNET_REST if self.env == "mainnet" else TESTNET_REST

        # 계정/마켓
        self.keypair = Keypair.from_bytes(base58.b58decode(self.private_b58))
        self.account_addr = str(self.keypair.pubkey())
        self.market: Optional[MarketSpec] = None

        # WS 상태
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.recv_task: Optional[asyncio.Task] = None
        self.heartbeat_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

        # 오더북
        self.best_bid: Optional[Decimal] = None
        self.best_ask: Optional[Decimal] = None
        self.book_ready = asyncio.Event()

        # 주문/취소 대기
        self.orders_by_cloid: Dict[str, InflightOrder] = {}
        self.orders_by_oid: Dict[int, InflightOrder] = {}
        self.cancel_waiters: Dict[str, asyncio.Future] = {}

        # 포지션 캐시
        self.positions: Dict[str, Dict[str, Any]] = {}

        # 순차 상태
        self.state: str = "INIT"  # 시작 상태를 INIT로 변경
        self.entry_order: Optional[InflightOrder] = None
        self.exit_order: Optional[InflightOrder] = None
        self.entry_filled: Decimal = Decimal("0")
        self.entry_avg: Optional[Decimal] = None
        self.hold_until: float = 0.0
        self._last_reprice_ts: float = 0.0

        # FLUSH 트래킹
        self.flush_order: Optional[InflightOrder] = None
        self.flush_deadline: float = 0.0
        self._flush_last_reprice_ts: float = 0.0

        # 중복 진입 방지
        self.avoid_double_entry = (os.getenv("PF_AVOID_DOUBLE_ENTRY") or "1") == "1"

        if self.api_key:
            log.info("PF_API_KEY 적용: %s***%s", self.api_key[:4], self.api_key[-4:])
        log.info("websockets 버전: %s", getattr(websockets, "__version__", "unknown"))
        log.info("재주문 간격: %s초", self.reprice_sec)

    # ---------- 유틸 ----------
    @staticmethod
    def _ascii_or_none(val: str) -> Optional[str]:
        s = str(val).strip().strip('"').strip("'")
        if "#" in s:
            s = s.split("#", 1)[0].strip()
        s = s.replace("\ufeff", "").replace("\u200b", "").replace("\u200c", "").replace("\u200d", "")
        s2 = "".join(ch for ch in s if 0x20 <= ord(ch) <= 0x7E)
        return s2 if s2 and len(s2) >= 8 else None

    @staticmethod
    def _floor_step(x: Decimal, step: Decimal) -> Decimal:
        if step <= 0: return x
        q = (x / step).to_integral_value(rounding=ROUND_FLOOR)
        return q * step

    @staticmethod
    def _dec(v: Any) -> Decimal:
        return v if isinstance(v, Decimal) else Decimal(str(v))

    def _rest_headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.api_key: h["PF-API-KEY"] = self.api_key
        return h

    def _sign(self, op_type: str, op_data: Dict[str, Any]) -> Dict[str, Any]:
        ts = int(time.time() * 1000)
        header = {"timestamp": ts, "expiry_window": DEFAULT_EXPIRY_MS, "type": op_type}

        def sort_obj(x):
            if isinstance(x, dict):
                return {k: sort_obj(x[k]) for k in sorted(x.keys())}
            if isinstance(x, list):
                return [sort_obj(v) for v in x]
            return x

        payload = {**header, "data": op_data}
        msg = json.dumps(sort_obj(payload), separators=(",", ":")).encode("utf-8")
        sig = base58.b58encode(bytes(self.keypair.sign_message(msg))).decode("ascii")
        return {
            "account": self.account_addr,
            "agent_wallet": None,
            "signature": sig,
            "timestamp": header["timestamp"],
            "expiry_window": header["expiry_window"],
            **op_data,
        }

    # ---------- 포지션 정규화 ----------
    def _normalize_position(self, p: Dict[str, Any]) -> Tuple[str, str, Decimal]:
        # 심볼
        sym = str(p.get("symbol") or p.get("s") or "").upper()

        # side
        side = str(p.get("side") or p.get("d") or "").lower()

        # amount - 다양한 필드명 시도
        amt_raw = None
        for field in ["amount", "a", "size", "sz", "qty", "quantity"]:
            if field in p and p[field] is not None:
                try:
                    amt_raw = self._dec(p[field])
                    break
                except:
                    continue

        if amt_raw is None:
            amt_raw = Decimal("0")

        # side 자동 판별
        if side not in ("bid", "ask"):
            if amt_raw > 0:
                side = "bid"
            elif amt_raw < 0:
                side = "ask"

        amt = abs(amt_raw)

        if amt > 0 and sym:
            log.debug("[NORM] %s: side=%s, amt=%s (from raw=%s)", sym, side, amt, amt_raw)

        return sym, side, amt

    # ---------- REST ----------
    def _load_market_spec(self) -> MarketSpec:
        url = f"{self.rest_base}/info"
        r = requests.get(url, headers=self._rest_headers(), timeout=10)
        r.raise_for_status()
        raw = r.json()

        records = None
        if isinstance(raw, list):
            records = raw
        elif isinstance(raw, dict):
            records = raw.get("data") if isinstance(raw.get("data"), list) else \
                [{"symbol": k, **v} for k, v in raw.items() if isinstance(v, dict)]

        if not records:
            raise RuntimeError(f"/info 응답 형식 오류: sample={str(raw)[:200]}")

        sym_u = self.symbol.upper()
        tgt = None
        for it in records:
            if isinstance(it, dict) and str(it.get("symbol", "")).upper() == sym_u:
                tgt = it; break
        if not tgt:
            raise RuntimeError(f"심볼 {self.symbol} 미발견")

        try:
            tick = Decimal(str(tgt["tick_size"]))
            lot = Decimal(str(tgt["lot_size"]))
            minusd = Decimal(str(tgt.get("min_order_size", "0")))
        except Exception as e:
            raise RuntimeError(f"/info 파싱 실패: {e}; target={tgt}")
        log.info("[SPEC] %s tick=%s lot=%s minUSD=%s", self.symbol, tick, lot, minusd)
        return MarketSpec(self.symbol, tick, lot, minusd)

    def _fetch_positions_rest(self) -> Dict[str, Dict[str, Any]]:
        url = f"{self.rest_base}/positions"
        log.info("[REST] 포지션 조회: %s", url)
        r = requests.get(url, headers=self._rest_headers(),
                         params={"account": self.account_addr}, timeout=10)
        r.raise_for_status()
        raw = r.json()

        # 응답 구조 로깅
        if isinstance(raw, dict):
            log.info("[REST] 응답 키: %s", list(raw.keys())[:10])

        arr = raw.get("data") if isinstance(raw, dict) else raw
        log.info("[REST] 포지션 배열 길이: %s", len(arr) if isinstance(arr, list) else "not a list")

        out: Dict[str, Dict[str, Any]] = {}
        if isinstance(arr, list):
            for p in arr:
                # 원본 로깅
                if str(p.get("symbol") or p.get("s") or "").upper() == self.symbol:
                    log.info("[REST] %s 포지션 RAW: %s", self.symbol, p)

                sym, side, amt = self._normalize_position(p)
                if sym and amt > 0 and side in ("bid", "ask"):
                    out[sym] = {"side": side, "amount": amt}
                    log.info("[REST] 포지션 파싱: %s %s %s", sym, side, amt)
        return out

    # ---------- 포지션 조회 ----------
    def _position_for_symbol(self, allow_rest: bool = True) -> Optional[Dict[str, Any]]:
        # 1) WS 캐시 우선
        p = self.positions.get(self.symbol)
        if p and self._dec(p.get("amount") or "0") > 0 and p.get("side") in ("bid", "ask"):
            self._pos_last_source = "ws"
            return {"side": p["side"], "amount": self._dec(p["amount"])}

        # 2) REST Fallback
        now = time.time()
        if not allow_rest or now < self._pos_rest_next_at:
            self._pos_last_source = "cache" if p else "none"
            return {"side": p["side"], "amount": self._dec(p["amount"])} if p else None

        try:
            out = self._fetch_positions_rest()
            self._pos_rest_next_at = now + self.pos_rest_min
            # 캐시 업데이트
            for sym, pos_data in out.items():
                self.positions[sym] = pos_data
            pos = out.get(self.symbol)
            self._pos_last_source = "rest" if pos else "rest"
            return pos
        except requests.exceptions.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            cool = self.pos_rest_429 if status == 429 else self.pos_rest_err
            self._pos_rest_next_at = now + cool
            log.warning("positions REST 실패(%s) → %ss 쿨다운", status or "err", cool)
            self._pos_last_source = "cache" if p else "none"
            return {"side": p["side"], "amount": self._dec(p["amount"])} if p else None
        except Exception as e:
            self._pos_rest_next_at = now + self.pos_rest_err
            log.warning("positions REST 예외(%r) → %ss 쿨다운", e, self.pos_rest_err)
            self._pos_last_source = "cache" if p else "none"
            return {"side": p["side"], "amount": self._dec(p["amount"])} if p else None

    # ---------- WS 송수신 ----------
    async def _send(self, msg: Dict[str, Any]):
        assert self.ws is not None
        await self.ws.send(json.dumps(msg))

    async def _subscribe(self):
        await self._send({"method": "subscribe", "params": {"source": "book", "symbol": self.symbol, "agg_level": 1}})
        for src in ("account_orders", "account_order_updates", "account_trades", "account_positions"):
            await self._send({"method": "subscribe", "params": {"source": src, "account": self.account_addr}})

    async def _recv_loop(self):
        try:
            async for raw in self.ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    log.warning("WS 파싱 실패: %s", raw[:200]); continue

                ch = msg.get("channel")
                mtype = msg.get("type")

                # 오더북
                if ch == "book":
                    data = msg.get("data", {})
                    levels = data.get("l", [[], []])
                    bids = levels[0] if isinstance(levels, list) and len(levels) > 0 else []
                    asks = levels[1] if isinstance(levels, list) and len(levels) > 1 else []
                    if bids: self.best_bid = self._dec(bids[0].get("p"))
                    if asks: self.best_ask = self._dec(asks[0].get("p"))
                    if self.best_bid and self.best_ask:
                        self.book_ready.set()
                    continue

                # 포지션
                if ch == "account_positions":
                    arr = msg.get("data", []) or []
                    log.info("[WS] account_positions 수신 (%d개 포지션)", len(arr))
                    present = set()
                    for p in arr:
                        # 원본 데이터 로깅
                        if str(p.get("symbol") or p.get("s") or "").upper() == self.symbol:
                            log.info("[WS] 내 심볼 포지션 RAW: %s", p)

                        sym, side, amt = self._normalize_position(p)
                        if sym and amt > 0 and side in ("bid","ask"):
                            self.positions[sym] = {"side": side, "amount": amt}
                            present.add(sym)
                            log.info("[WS] 포지션 업데이트: %s %s %s", sym, side, amt)
                    # 스냅샷에 없으면 제거
                    if self.symbol not in present:
                        if self.symbol in self.positions:
                            log.info("[WS] %s 포지션 제거 (스냅샷에 없음)", self.symbol)
                        self.positions.pop(self.symbol, None)
                    self._ws_pos_last_ts = time.time()
                    continue

                # 계정 오더 스냅샷
                if ch == "account_orders":
                    arr = msg.get("data", []) or []
                    for o in arr:
                        if str(o.get("s","")).upper() != self.symbol:
                            continue
                        if str(o.get("os","")) != "open":
                            continue
                        side = str(o.get("d",""))
                        ro = bool(o.get("ro", False))
                        price = self._dec(o.get("ip") if o.get("ip") is not None else o.get("p"))
                        amt = self._dec(o.get("a","0"))
                        cloid = str(o.get("I") or uuid.uuid4())
                        oid = o.get("i")

                        io = InflightOrder(
                            cloid=cloid, side=side, price=price, amount=amt,
                            reduce_only=ro, tif="GTC", order_id=oid, status="open"
                        )
                        self.orders_by_cloid[cloid] = io
                        if oid is not None:
                            self.orders_by_oid[oid] = io

                        # 상태에 따라 복구
                        if ro and self.state == "FLUSH" and self.flush_order is None:
                            self.flush_order = io
                            log.info("[복구] FLUSH 주문 재구성: %s %s @ %s (oid=%s)", side, amt, price, oid)
                        elif ro and self.state == "EXIT_SHORT" and self.exit_order is None:
                            self.exit_order = io
                            log.info("[복구] EXIT 주문 재구성: %s %s @ %s (oid=%s)", side, amt, price, oid)
                        elif (not ro) and self.state == "ENTER_SHORT" and side == "ask" and self.entry_order is None:
                            self.entry_order = io
                            log.info("[복구] ENTRY 주문 재구성: %s %s @ %s (oid=%s)", side, amt, price, oid)
                    continue

                # 주문 생성 ACK
                if mtype == "create_order":
                    data = msg.get("data", {}) or {}
                    cloid = data.get("I"); oid = data.get("i")
                    io = self.orders_by_cloid.get(cloid)
                    if io:
                        io.order_id = oid
                        if oid is not None: self.orders_by_oid[oid] = io
                        log.info("[ACK] %s %s @ %s (ro=%s, tif=%s, oid=%s, cloid=%s)",
                                 io.side, io.amount, io.price, io.reduce_only, io.tif, oid, io.cloid)

                        # FLUSH 주문 ACK 처리 확인
                        if self.flush_order and io.cloid == self.flush_order.cloid:
                            log.info("[ACK] FLUSH 주문 확인됨 (state=%s)", self.state)
                    continue

                # 주문 업데이트
                if ch == "account_order_updates":
                    updates = msg.get("data", []) or []
                    for u in updates:
                        cloid = u.get("I"); oid = u.get("i")
                        status = u.get("os")
                        avgp = u.get("p"); filled = u.get("f")
                        io: Optional[InflightOrder] = None
                        if cloid and cloid in self.orders_by_cloid:
                            io = self.orders_by_cloid[cloid]
                        elif oid and oid in self.orders_by_oid:
                            io = self.orders_by_oid[oid]
                        if not io:
                            continue
                        if filled is not None:
                            try: io.filled = self._dec(filled)
                            except: pass
                        if avgp is not None:
                            try: io.avg_price = self._dec(avgp)
                            except: pass
                        if status:
                            io.status = status
                        io.last_update_at = time.time()

                        # 취소 대기자 깨우기
                        if status == "cancelled":
                            fut = self.cancel_waiters.get(io.cloid)
                            if fut and not fut.done(): fut.set_result(True)

                        # 로그
                        if status == "open":
                            log.info("[open] %s %s @ %s (oid=%s)", io.side, io.amount, io.price, io.order_id)
                        elif status == "partially_filled":
                            log.info("[partial] %s %s/%s @ %s", io.side, io.filled, io.amount, io.price)
                        elif status == "filled":
                            log.info("[filled] %s %s @ %s (avg=%s ro=%s)", io.side, io.filled, io.price, io.avg_price, io.reduce_only)
                        elif status in ("cancelled", "rejected"):
                            reason = u.get("oe") or u.get("e") or u.get("reason")
                            log.warning("[%s] %s 주문 %s @ %s (oid=%s, reason=%s)", status, io.side, io.amount, io.price, io.order_id, reason)

                        # 이벤트 기반 상태 전환
                        if status == "filled":
                            # 엔트리 체결 → HOLD
                            if io is self.entry_order and not io.reduce_only:
                                self.entry_filled = io.filled
                                self.entry_avg = io.avg_price or io.price
                                self.hold_until = time.time() + random.randint(self.hold_min, self.hold_max)
                                self.entry_order = None
                                self.state = "HOLD"
                                log.info("[EVT] ENTRY filled → HOLD (filled=%s, avg=%s, until=%ds)",
                                         self.entry_filled, self.entry_avg, int(self.hold_until - time.time()))
                            # 청산 체결 → ENTER_SHORT
                            elif io is self.exit_order and io.reduce_only:
                                self.exit_order = None
                                self.entry_order = None
                                self.entry_filled = Decimal("0")
                                self.entry_avg = None
                                self.state = "ENTER_SHORT"
                                log.info("[EVT] EXIT filled → ENTER_SHORT")
                            # 플러시 체결 → ENTER_SHORT
                            elif io is self.flush_order and io.reduce_only:
                                self.flush_order = None
                                self.state = "ENTER_SHORT"
                                log.info("[EVT] FLUSH filled → ENTER_SHORT")
                    continue

                if ch == "pong":
                    continue

        except websockets.exceptions.ConnectionClosed as e:
            log.warning("WS 연결 종료: %s", e)
        except Exception as e:
            log.exception("WS 수신 에러: %s", e)

    async def _heartbeat(self):
        try:
            while not self._stop.is_set():
                await asyncio.sleep(JSON_PING_INTERVAL_SEC)
                try:
                    await self._send({"method": "ping"})
                except Exception:
                    break
        except Exception as e:
            log.warning("Heartbeat 종료: %s", e)

    # ---------- 주문 API ----------
    async def _create_order(self, side: str, price: Decimal, amount: Decimal, reduce_only: bool, tif: str="GTC") -> InflightOrder:
        price = self._floor_step(price, self.market.tick_size)
        amount = self._floor_step(amount, self.market.lot_size)
        if amount <= 0:
            raise RuntimeError("amount <= 0")

        cloid = str(uuid.uuid4())
        op = {
            "symbol": self.symbol,
            "price": f"{price}",
            "reduce_only": bool(reduce_only),
            "amount": f"{amount}",
            "side": side,
            "tif": tif,
            "client_order_id": cloid,
        }
        signed = self._sign("create_order", op)
        req = {"id": str(uuid.uuid4()), "method": "trading", "params": {"create_order": signed}}
        await self._send(req)
        io = InflightOrder(cloid=cloid, side=side, price=price, amount=amount, reduce_only=reduce_only, tif=tif)
        self.orders_by_cloid[cloid] = io
        log.info("[%s] 게시: %s @ %s (ro=%s, tif=%s, cloid=%s)", side, amount, price, reduce_only, tif, cloid)
        return io

    async def _cancel_order(self, io: InflightOrder) -> bool:
        op = {"symbol": self.symbol, "client_order_id": io.cloid}
        signed = self._sign("cancel_order", op)
        req = {"id": str(uuid.uuid4()), "method": "trading", "params": {"cancel_order": signed}}
        fut = asyncio.get_running_loop().create_future()
        self.cancel_waiters[io.cloid] = fut
        await self._send(req)
        try:
            await asyncio.wait_for(fut, timeout=CANCEL_TIMEOUT_SEC)
            return True
        except asyncio.TimeoutError:
            log.warning("취소 대기 타임아웃: cloid=%s (강행 계속)", io.cloid)
            return False
        finally:
            self.cancel_waiters.pop(io.cloid, None)

    # ---------- 전략 유틸 ----------
    def _calc_amount_by_notional(self, price: Decimal) -> Decimal:
        amt = self.notional / price
        return self._floor_step(amt, self.market.lot_size)

    def _roi_short_pct(self, entry: Optional[Decimal]) -> Decimal:
        if entry is None or not self.best_ask:
            return Decimal("0")
        return (entry - self.best_ask) / entry * Decimal("100")

    async def _ensure_book_ready(self):
        await self.book_ready.wait()
        if self.market is None:
            self.market = self._load_market_spec()

    # ---------- FLUSH 단계 ----------
    async def _step_flush(self):
        await self._ensure_book_ready()

        if self.flush_deadline <= 0:
            self.flush_deadline = time.time() + max(1, self.flush_wait)
            log.info("[FLUSH] 시작, 최대 %ds 동안 정리 시도", self.flush_wait)

        # 포지션 확인
        p = self._position_for_symbol(allow_rest=False)  # 캐시만 사용
        if not p or self._dec(p.get("amount", "0")) <= 0:
            # 포지션 없음 → ENTER_SHORT로 전환
            log.info("[FLUSH] 포지션 없음 → ENTER_SHORT")
            if self.flush_order:
                await self._cancel_order(self.flush_order)
            self.flush_order = None
            self.state = "ENTER_SHORT"
            return

        side_now = p["side"]
        amt_now = self._floor_step(self._dec(p["amount"]), self.market.lot_size)

        # 반대 방향 계산 (정리를 위한)
        if side_now == "bid":
            opp = "ask"
            ref_px = self.best_ask
        else:  # ask
            opp = "bid"
            ref_px = self.best_bid

        if not ref_px:
            await asyncio.sleep(0.1)
            return

        # 초기 FLUSH 주문 생성
        if self.flush_order is None:
            self.flush_order = await self._create_order(opp, ref_px, amt_now, reduce_only=True, tif="GTC")
            self._flush_last_reprice_ts = time.time()
            log.info("[FLUSH] 정리 주문 생성: %s %s @ %s", opp, amt_now, ref_px)
            # return 제거 - 아래 로직 계속 실행

        # FLUSH 주문이 있는 경우에만 체크
        if self.flush_order:
            # 체결 확인
            if self.flush_order.status == "filled":
                log.info("[STATE] FLUSH 완료(체결) → ENTER_SHORT")
                self.flush_order = None
                self.state = "ENTER_SHORT"
                return

            # 타임아웃 확인
            remaining = max(0, int(self.flush_deadline - time.time()))
            if time.time() >= self.flush_deadline:
                log.info("[FLUSH] 대기 만료(%ds 경과) → ENTER_SHORT", self.flush_wait)
                await self._cancel_order(self.flush_order)
                self.flush_order = None
                self.state = "ENTER_SHORT"
                return

            # 5초마다 재주문
            now = time.time()
            elapsed = now - self._flush_last_reprice_ts

            if elapsed >= self.reprice_sec:
                log.info("[FLUSH] 재주문 시간 도달 (%ds 경과)", int(elapsed))

                # 최신 포지션 재확인
                p_update = self._position_for_symbol(allow_rest=False)
                if not p_update or self._dec(p_update.get("amount", "0")) <= 0:
                    log.info("[FLUSH] 포지션 사라짐 → ENTER_SHORT")
                    await self._cancel_order(self.flush_order)
                    self.flush_order = None
                    self.state = "ENTER_SHORT"
                    return

                qty_left = self._floor_step(self._dec(p_update["amount"]), self.market.lot_size)
                if qty_left > 0:
                    # 가격 업데이트
                    if p_update["side"] == "bid":
                        new_px = self.best_ask
                    else:
                        new_px = self.best_bid

                    if new_px:
                        # 가격이나 수량이 변경되었을 때만 재주문
                        if new_px != self.flush_order.price or qty_left != self.flush_order.amount:
                            log.info("[FLUSH] 취소하고 재주문 (기존: %s @ %s, 신규: %s @ %s)",
                                     self.flush_order.amount, self.flush_order.price, qty_left, new_px)
                            await self._cancel_order(self.flush_order)
                            # 새 주문 생성
                            opp_side = "ask" if p_update["side"] == "bid" else "bid"
                            self.flush_order = await self._create_order(
                                opp_side, new_px, qty_left, reduce_only=True, tif="GTC"
                            )
                            log.info("[FLUSH] 재주문 완료: %s %s @ %s (남은시간=%ds)",
                                     opp_side, qty_left, new_px, remaining)
                        else:
                            log.debug("[FLUSH] 가격/수량 동일, 재주문 불필요")

                self._flush_last_reprice_ts = time.time()
            else:
                # 다음 재주문까지 대기
                next_reprice = max(0, self.reprice_sec - int(elapsed))
                if next_reprice % 2 == 0:  # 2초마다 로그
                    log.debug("[FLUSH] 다음 재주문까지 %ds (남은시간=%ds)", next_reprice, remaining)

    # ---------- ENTER_SHORT ----------
    async def _step_enter_short(self):
        await self._ensure_book_ready()

        # 체결 확인
        eps = self.market.lot_size / Decimal("10")
        if self.entry_order and (
            self.entry_order.status == "filled" or
            (self.entry_order.amount - self.entry_order.filled) <= eps
        ):
            self.entry_filled = self.entry_order.filled
            self.entry_avg = self.entry_order.avg_price or self.entry_order.price
            self.hold_until = time.time() + random.randint(self.hold_min, self.hold_max)
            self.entry_order = None
            self.state = "HOLD"
            log.info("[STATE] HOLD 진입 (filled=%s avg=%s, until=%ds)",
                     self.entry_filled, self.entry_avg, int(self.hold_until - time.time()))
            return

        # 중복 진입 방지 - 초기 진입 시에만 체크 (이미 주문이 있으면 스킵)
        if self.avoid_double_entry and self.entry_order is None:
            p = self._position_for_symbol(allow_rest=False)  # WS 캐시만 확인
            if p and self._dec(p.get("amount","0")) > 0:
                log.info("[GUARD] 예상치 못한 포지션 감지(side=%s, amt=%s) → FLUSH 전환", p["side"], p["amount"])
                self.state = "FLUSH"
                self.flush_deadline = 0.0
                return

        # 신규 엔트리
        if self.entry_order is None:
            if not self.best_bid:
                await asyncio.sleep(0.1)
                return
            px = self.best_bid
            amt = self._calc_amount_by_notional(px)
            if (px * amt) < self.market.min_order_size_usd:
                await asyncio.sleep(0.3)
                return
            self.entry_order = await self._create_order("ask", px, amt, reduce_only=False, tif="GTC")
            self._last_reprice_ts = time.time()
            return

        # 5초마다 재주문
        now = time.time()
        if now - self._last_reprice_ts >= self.reprice_sec:
            remain = self.entry_order.amount - self.entry_order.filled
            if remain > 0 and self.best_bid:
                new_px = self._floor_step(self.best_bid, self.market.tick_size)
                if new_px != self.entry_order.price:
                    await self._cancel_order(self.entry_order)
                    self.entry_order = await self._create_order("ask", new_px, self._floor_step(remain, self.market.lot_size), False, "GTC")
                    log.info("[ENTER] 재주문: ask %s @ %s", self._floor_step(remain, self.market.lot_size), new_px)
            self._last_reprice_ts = now

    # ---------- HOLD ----------
    async def _step_hold(self):
        if self.entry_avg is None:
            log.warning("[HOLD] entry_avg가 None → ENTER_SHORT 전환")
            self.state = "ENTER_SHORT"
            return

        now = time.time()
        roi = self._roi_short_pct(self.entry_avg)
        trigger_time = now >= self.hold_until
        trigger_roi  = (roi >= self.target_roi_pct) if self.use_roi else False

        if trigger_time or trigger_roi:
            reason = "time" if trigger_time else "roi"
            log.info("[TRIGGER:%s] EXIT 조건 충족 (roi=%.4f%%, remain=%ds)",
                     reason, float(roi), max(0, int(self.hold_until - now)))
            self.state = "EXIT_SHORT"
        await asyncio.sleep(0.25)

    # ---------- EXIT_SHORT ----------
    async def _step_exit_short(self):
        await self._ensure_book_ready()

        pos = self._position_for_symbol()
        qty_remain = self._floor_step(self._dec((pos or {}).get("amount", self.entry_filled)), self.market.lot_size)
        if not qty_remain or qty_remain <= 0:
            log.info("[STATE] EXIT 완료 → ENTER_SHORT")
            self.exit_order = None
            self.entry_order = None
            self.entry_filled = Decimal("0")
            self.entry_avg = None
            self.state = "ENTER_SHORT"
            return

        if self.exit_order is None:
            px = self.best_ask
            if not px:
                await asyncio.sleep(0.1)
                return
            self.exit_order = await self._create_order("bid", px, qty_remain, reduce_only=True, tif="GTC")
            self._last_reprice_ts = time.time()
            return

        if self.exit_order.status == "filled":
            log.info("[STATE] EXIT 완료 → ENTER_SHORT")
            self.exit_order = None
            self.entry_order = None
            self.entry_filled = Decimal("0")
            self.entry_avg = None
            self.state = "ENTER_SHORT"
            return

        # 5초마다 재주문
        now = time.time()
        if now - self._last_reprice_ts >= self.reprice_sec:
            if self.best_ask:
                pos = self._position_for_symbol()
                qty_remain = self._floor_step(self._dec((pos or {}).get("amount", self.entry_filled)), self.market.lot_size)
                if qty_remain > 0:
                    new_px = self._floor_step(self.best_ask, self.market.tick_size)
                    if self.exit_order.price != new_px:
                        await self._cancel_order(self.exit_order)
                        self.exit_order = await self._create_order("bid", new_px, qty_remain, reduce_only=True, tif="GTC")
                        log.info("[EXIT] 재주문: bid %s @ %s", qty_remain, new_px)
            self._last_reprice_ts = now

    # ---------- 초기화 단계 ----------
    async def _step_init(self):
        """시작 시 포지션 체크하고 상태 결정"""
        await self._ensure_book_ready()

        log.info("[INIT] 포지션 확인 시작")

        # 1. REST로 즉시 확인 (WS는 신뢰할 수 없음)
        log.info("[INIT] REST API로 포지션 확인...")
        p = None
        try:
            self._pos_rest_next_at = 0  # 쿨다운 리셋
            rest_positions = self._fetch_positions_rest()
            log.info("[INIT] REST 포지션 조회 결과: %s", rest_positions)

            # 캐시 업데이트
            for sym, pos_data in rest_positions.items():
                self.positions[sym] = pos_data

            p = rest_positions.get(self.symbol)
            if p:
                log.info("[INIT] REST에서 포지션 발견: %s", p)
        except Exception as e:
            log.error("[INIT] REST 포지션 조회 실패: %s", e)

        # 2. WS 잠시 대기 (보조용)
        if p is None:
            log.info("[INIT] REST에서 포지션 없음, WS 대기 %ss", self.pos_ws_grace)
            t_end = time.time() + self.pos_ws_grace
            while time.time() < t_end:
                p = self._position_for_symbol(allow_rest=False)
                if p:
                    log.info("[INIT] WS에서 포지션 발견")
                    break
                await asyncio.sleep(0.2)

        # 3. 상태 결정 (한 번만 실행되도록)
        if p and self._dec(p.get("amount") or "0") > 0:
            log.info("[INIT] 기존 포지션 발견(side=%s, amt=%s) → FLUSH", p["side"], p["amount"])
            self.state = "FLUSH"
            self.flush_deadline = 0.0
            return  # 상태 전환 후 즉시 리턴
        else:
            log.info("[INIT] 포지션 없음 → ENTER_SHORT")
            self.state = "ENTER_SHORT"
            return  # 상태 전환 후 즉시 리턴

    # ---------- 상태 보드 ----------
    def _render_table(self):
        now = time.time()
        spread = (self.best_ask - self.best_bid) if (self.best_ask and self.best_bid) else None
        p = self._position_for_symbol(allow_rest=False)
        pos_side = p["side"] if p else "-"
        pos_amt  = self._dec(p["amount"]) if p else Decimal("0")

        entry_px = self.entry_avg or (self.entry_order.price if self.entry_order else None)
        roi = self._roi_short_pct(entry_px) if self.state in ("HOLD","EXIT_SHORT") else Decimal("0")

        if self.state == "HOLD":
            countdown = max(0, int(self.hold_until - now))
        elif self.state == "FLUSH":
            countdown = max(0, int(self.flush_deadline - now))
        else:
            countdown = 0

        if self.state == "FLUSH" and self._flush_last_reprice_ts > 0:
            next_reprice = max(0, self.reprice_sec - int(now - self._flush_last_reprice_ts))
        elif self._last_reprice_ts > 0:
            next_reprice = max(0, self.reprice_sec - int(now - self._last_reprice_ts))
        else:
            next_reprice = "-"

        rest_cd = max(0, int(self._pos_rest_next_at - now))
        pos_src = self._pos_last_source

        def fmt_order(io: Optional[InflightOrder]) -> str:
            if not io: return "-"
            return f"{io.side} {io.filled}/{io.amount} @ {io.price} ({io.status}{', RO' if io.reduce_only else ''})"

        exit_cell = fmt_order(self.flush_order) if self.state == "FLUSH" else fmt_order(self.exit_order)

        t = Table(title=f"Pacifica · {self.symbol} · {self.state}", expand=True)
        t.add_column("Position")
        t.add_column("Entry/ROI")
        t.add_column("Countdown")
        t.add_column("Orderbook")
        t.add_column("Entry Order")
        t.add_column("Exit/Flush Order")
        t.add_column("Next Reprice")

        t.add_row(
            f"{pos_side} {pos_amt} ({pos_src}, restCD={rest_cd}s)",
            f"entry={entry_px} | ROI={roi:.4f}%",
            f"{countdown}s" if countdown else "-",
            f"bid0={self.best_bid} | ask0={self.best_ask} | spr={spread}" if spread is not None else "-",
            fmt_order(self.entry_order),
            exit_cell,
            f"{next_reprice}s" if isinstance(next_reprice, int) else "-",
        )
        return t

    def _render_text(self) -> str:
        cols = shutil.get_terminal_size((100, 20)).columns
        now = time.strftime("%H:%M:%S")
        spread = (self.best_ask - self.best_bid) if (self.best_ask and self.best_bid) else None
        p = self._position_for_symbol(allow_rest=False)
        pos_side = p["side"] if p else "-"
        pos_amt  = self._dec(p["amount"]) if p else Decimal("0")
        entry_px = self.entry_avg or (self.entry_order.price if self.entry_order else None)
        roi = self._roi_short_pct(entry_px) if self.state in ("HOLD","EXIT_SHORT") else Decimal("0")

        if self.state == "HOLD":
            remain = f"{max(0,int(self.hold_until - time.time()))}s"
            next_reprice = max(0, self.reprice_sec - int(time.time() - self._last_reprice_ts)) if self._last_reprice_ts>0 else "-"
        elif self.state == "FLUSH":
            remain = f"{max(0,int(self.flush_deadline - time.time()))}s"
            next_reprice = max(0, self.reprice_sec - int(time.time() - self._flush_last_reprice_ts)) if self._flush_last_reprice_ts>0 else "-"
        else:
            remain = "-"
            next_reprice = max(0, self.reprice_sec - int(time.time() - self._last_reprice_ts)) if self._last_reprice_ts>0 else "-"

        rest_cd = max(0, int(self._pos_rest_next_at - time.time()))
        pos_src = self._pos_last_source

        def fmt_order(io: Optional[InflightOrder]) -> str:
            if not io: return "-"
            ro = ", RO" if io.reduce_only else ""
            return f"{io.side} {io.filled}/{io.amount} @ {io.price} ({io.status}{ro})"

        exit_cell = fmt_order(self.flush_order) if self.state == "FLUSH" else fmt_order(self.exit_order)

        lines = [
            "="*cols,
            f"[{now}] {self.symbol} | STATE={self.state}",
            f"POS: {pos_side} {pos_amt} ({pos_src}, restCD={rest_cd}s)",
            f"ENTRY: {entry_px} | ROI={roi:.4f}%",
            f"COUNTDOWN: {remain}",
            f"BOOK: bid0={self.best_bid} | ask0={self.best_ask} | spread={spread}",
            f"ENTRY ORDER: {fmt_order(self.entry_order)}",
            f"EXIT/FLUSH ORDER: {exit_cell}",
            f"NEXT REPRICE: {next_reprice}s",
            "="*cols,
        ]
        return "\n".join(lines)

    async def _status_loop(self):
        if self.use_rich:
            self._live = Live(self._render_table(), refresh_per_second=4, console=self._console)
            self._live.start()
        try:
            while not self._stop.is_set():
                if self.use_rich:
                    self._live.update(self._render_table())
                else:
                    print(self._render_text())
                await asyncio.sleep(self.status_sec)
        finally:
            if self._live:
                try: self._live.stop()
                except: pass

    # ---------- 연결/루프 ----------
    async def _open_ws(self, headers: Optional[Dict[str, str]]):
        try:
            return await websockets.connect(self.ws_url, extra_headers=headers or {})
        except TypeError:
            return await websockets.connect(self.ws_url)

    async def _connect_once(self):
        ws_headers = self._rest_headers()
        ws_headers.pop("Accept", None)
        if not self.api_key: ws_headers = {}

        log.info("WS 연결: %s (symbol=%s, notional=%s)", self.ws_url, self.symbol, self.notional)
        ws = await self._open_ws(ws_headers)
        self.ws = ws
        try:
            self.recv_task = asyncio.create_task(self._recv_loop())
            self.heartbeat_task = asyncio.create_task(self._heartbeat())
            self._status_task = asyncio.create_task(self._status_loop())
            await self._subscribe()

            # 오더북 준비
            await self._ensure_book_ready()
            log.info("오더북 준비: bid0=%s, ask0=%s", self.best_bid, self.best_ask)

            # FSM
            while not self._stop.is_set():
                prev_state = self.state  # 디버깅용

                if self.state == "INIT":
                    await self._step_init()
                elif self.state == "FLUSH":
                    await self._step_flush()
                elif self.state == "ENTER_SHORT":
                    await self._step_enter_short()
                elif self.state == "HOLD":
                    await self._step_hold()
                elif self.state == "EXIT_SHORT":
                    await self._step_exit_short()
                else:
                    log.warning("알 수 없는 상태: %s", self.state)
                    await asyncio.sleep(0.2)

                # 짧은 대기 (CPU 과부하 방지)
                await asyncio.sleep(0.01)

                # 상태 변경 로깅
                if prev_state != self.state:
                    log.info("[FSM] 상태 전환: %s → %s", prev_state, self.state)
        finally:
            try: await ws.close()
            except: pass

    async def run(self):
        self.market = self._load_market_spec()

        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._connect_once()
                backoff = 1.0
            except Exception as e:
                if self._stop.is_set(): break
                log.warning("연결 사이클 종료: %s → %ss 후 재연결", repr(e), int(backoff))
                try:
                    if self.recv_task: self.recv_task.cancel()
                    if self.heartbeat_task: self.heartbeat_task.cancel()
                    if self._status_task: self._status_task.cancel()
                except: pass
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, float(self.reconnect_max_backoff))

    def stop(self, *_):
        self._stop.set()

# ---------- 엔트리 ----------
async def _amain():
    bot = PacificaSequentialBot()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try: loop.add_signal_handler(sig, bot.stop)
        except NotImplementedError: pass
    await bot.run()

if __name__ == "__main__":
    asyncio.run(_amain())
