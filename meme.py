import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Dict, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn
import websockets

WS_URL = "wss://pumpdev.io/ws"

# ==========================================
# TIMEFRAME CONFIGURATION (5-MINUTE STRATEGY)
# ==========================================
C1_DURATION = 300     # 5 Minutes for Candle 1
C2_DURATION = 600     # 10 Minutes total (Candle 1 + Candle 2)
MAX_LIFESPAN = 900    # 15 Minutes maximum token monitoring window

# ==========================================
# DATA MODELS & STRATEGY ENGINE
# ==========================================
@dataclass
class Candle:
    open_p: float
    high_p: float
    low_p: float
    close_p: float

    @property
    def is_green(self) -> bool:
        return self.close_p > self.open_p

    @property
    def body_ratio(self) -> float:
        total_range = self.high_p - self.low_p
        if total_range == 0:
            return 0.0
        return abs(self.close_p - self.open_p) / total_range

    @property
    def is_doji(self) -> float:
        return self.body_ratio < 0.15 or not self.is_green


# ==========================================
# WEBSOCKET DASHBOARD CONNECTION MANAGER
# ==========================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)

manager = ConnectionManager()


# ==========================================
# ENHANCED SPOT BREAKOUT BOT (5M TIMEFRAME)
# ==========================================
class SpotBreakoutBot:
    def __init__(self, position_usd: float = 0.25):
        self.position_usd = position_usd
        self.active_queues: Dict[str, asyncio.Queue] = {}

    async def monitor_token_5m(self, symbol: str, mint: str, queue: asyncio.Queue):
        dex_url = f"https://dexscreener.com/solana/{mint}"

        await manager.broadcast({
            "type": "NEW_TRACKED",
            "mint": mint,
            "symbol": symbol,
            "dex_url": dex_url,
            "status": "Tracking C1 (5m)..."
        })

        start_time = asyncio.get_event_loop().time()
        c1_prices = []
        c1_buys = 0
        c1_sells = 0
        c1_candle: Optional[Candle] = None

        c2_prices = []
        c2_buys = 0
        c2_sells = 0
        c2_candle: Optional[Candle] = None

        target_breakout_price = 0.0
        stop_loss_price = 0.0
        base_rr = 2.0  # Conservative R:R ratio for 5m scalps

        bought = False
        entry_price = 0.0
        tokens_bought = 0.0
        tp_price = 0.0
        be_trigger = 0.0
        peak_price = 0.0
        last_high_time = 0.0

        while True:
            now = asyncio.get_event_loop().time()
            elapsed = now - start_time

            # 1. Safety removal after max lifespan (15 mins)
            if elapsed > MAX_LIFESPAN and not bought:
                await manager.broadcast({"type": "REMOVE_TRACKED", "mint": mint})
                break

            try:
                # Non-blocking queue reading
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    data = None

                if data and data.get("vSolInBondingCurve") and data.get("vTokensInBondingCurve"):
                    sol_res = data["vSolInBondingCurve"] / 1e9
                    tok_res = data["vTokensInBondingCurve"] / 1e6

                    # FILTER 1: Minimum Bonding Curve Liquidity (> 5.0 SOL)
                    if sol_res < 5.0 and not bought:
                        data = None

                    if data and tok_res > 0:
                        current_price = sol_res / tok_res
                        is_buy = data.get("isBuy", True)

                        if elapsed <= C1_DURATION:
                            c1_prices.append(current_price)
                            if is_buy:
                                c1_buys += 1
                            else:
                                c1_sells += 1
                        elif elapsed <= C2_DURATION and c1_candle and not (c1_candle.is_green and not c1_candle.is_doji):
                            c2_prices.append(current_price)
                            if is_buy:
                                c2_buys += 1
                            else:
                                c2_sells += 1

                # --- 2. FINALIZE 5-MINUTE C1 ---
                if elapsed > C1_DURATION and not c1_candle:
                    # FILTER 2: Needs at least 5 trade events & buyer dominance
                    if len(c1_prices) < 5 or c1_sells >= c1_buys:
                        await manager.broadcast({"type": "REMOVE_TRACKED", "mint": mint})
                        break

                    c1_candle = Candle(
                        open_p=c1_prices[0],
                        high_p=max(c1_prices),
                        low_p=min(c1_prices),
                        close_p=c1_prices[-1]
                    )

                    if c1_candle.is_green and not c1_candle.is_doji:
                        target_breakout_price = c1_candle.high_p
                        stop_loss_price = c1_candle.low_p
                        base_rr = 2.0
                        await manager.broadcast({
                            "type": "UPDATE_TRACKED",
                            "mint": mint,
                            "status": f"Waiting Breakout > ${target_breakout_price:.8f}"
                        })
                    else:
                        await manager.broadcast({
                            "type": "UPDATE_TRACKED",
                            "mint": mint,
                            "status": "Tracking C2 (5m)..."
                        })

                # --- 3. FINALIZE 5-MINUTE C2 (At 10m elapsed) ---
                if elapsed > C2_DURATION and c1_candle and not (c1_candle.is_green and not c1_candle.is_doji) and not c2_candle:
                    if len(c2_prices) < 5 or c2_sells >= c2_buys:
                        await manager.broadcast({"type": "REMOVE_TRACKED", "mint": mint})
                        break

                    c2_candle = Candle(
                        open_p=c2_prices[0],
                        high_p=max(c2_prices),
                        low_p=min(c2_prices),
                        close_p=c2_prices[-1]
                    )

                    if c2_candle.is_green and not c2_candle.is_doji:
                        target_breakout_price = c2_candle.high_p
                        stop_loss_price = min(c1_candle.low_p, c2_candle.low_p)
                        base_rr = 1.8
                        await manager.broadcast({
                            "type": "UPDATE_TRACKED",
                            "mint": mint,
                            "status": f"Waiting Breakout > ${target_breakout_price:.8f}"
                        })
                    else:
                        await manager.broadcast({"type": "REMOVE_TRACKED", "mint": mint})
                        break

                # --- 4. EXECUTE BREAKOUT BUY ---
                if target_breakout_price > 0 and not bought and 'current_price' in locals():
                    if current_price >= target_breakout_price:
                        risk = target_breakout_price - stop_loss_price
                        if risk <= 0 or (risk / target_breakout_price) > 0.25:  # Avoid entering if risk is > 25%
                            await manager.broadcast({"type": "REMOVE_TRACKED", "mint": mint})
                            break

                        bought = True
                        entry_price = current_price
                        tokens_bought = self.position_usd / entry_price
                        tp_price = entry_price + (risk * base_rr)
                        be_trigger = entry_price + (risk * 0.4)
                        peak_price = entry_price
                        last_high_time = now

                        await manager.broadcast({"type": "REMOVE_TRACKED", "mint": mint})
                        await manager.broadcast({
                            "type": "NEW_BOUGHT",
                            "mint": mint,
                            "symbol": symbol,
                            "dex_url": dex_url,
                            "entry": f"${entry_price:.8f}",
                            "current": f"${current_price:.8f}",
                            "sl": f"${stop_loss_price:.8f}",
                            "tp": f"${tp_price:.8f}"
                        })

                # --- 5. ACTIVE POSITION MANAGEMENT ---
                if bought and 'current_price' in locals():
                    unrealized_pnl = (tokens_bought * current_price) - self.position_usd
                    await manager.broadcast({
                        "type": "UPDATE_BOUGHT",
                        "mint": mint,
                        "current": f"${current_price:.8f}",
                        "pnl": f"{unrealized_pnl:+.4f}"
                    })

                    if current_price > peak_price:
                        peak_price = current_price
                        last_high_time = now

                    # Trailing Stop-Loss to Break-Even
                    if current_price >= be_trigger and stop_loss_price < entry_price:
                        stop_loss_price = entry_price

                    # STALL PROTECTION: Exit if no new high in 45s while in profit
                    if (now - last_high_time) > 45.0 and unrealized_pnl > 0:
                        profit = unrealized_pnl
                        await manager.broadcast({"type": "REMOVE_BOUGHT", "mint": mint})
                        await manager.broadcast({
                            "type": "NEW_CLOSED",
                            "symbol": symbol,
                            "dex_url": dex_url,
                            "entry": f"${entry_price:.8f}",
                            "exit": f"${current_price:.8f}",
                            "pnl": f"+${profit:.4f}",
                            "status": "STALL-PROFIT"
                        })
                        break

                    # TAKE PROFIT
                    if current_price >= tp_price:
                        profit = unrealized_pnl
                        await manager.broadcast({"type": "REMOVE_BOUGHT", "mint": mint})
                        await manager.broadcast({
                            "type": "NEW_CLOSED",
                            "symbol": symbol,
                            "dex_url": dex_url,
                            "entry": f"${entry_price:.8f}",
                            "exit": f"${current_price:.8f}",
                            "pnl": f"+${profit:.4f}",
                            "status": "PROFIT"
                        })
                        break

                    # STOP LOSS
                    if current_price <= stop_loss_price:
                        pnl = unrealized_pnl
                        status = "BREAK-EVEN" if stop_loss_price == entry_price else "LOSS"
                        await manager.broadcast({"type": "REMOVE_BOUGHT", "mint": mint})
                        await manager.broadcast({
                            "type": "NEW_CLOSED",
                            "symbol": symbol,
                            "dex_url": dex_url,
                            "entry": f"${entry_price:.8f}",
                            "exit": f"${current_price:.8f}",
                            "pnl": f"${pnl:+.4f}",
                            "status": status
                        })
                        break

                await asyncio.sleep(0.5)

            except Exception:
                continue

        self.active_queues.pop(mint, None)


bot = SpotBreakoutBot(position_usd=0.25)


# ==========================================
# BACKGROUND WEBSOCKET LISTENER
# ==========================================
async def solana_listener():
    while True:
        try:
            async with websockets.connect(WS_URL) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                async for msg in ws:
                    data = json.loads(msg)

                    if data.get("txType") == "create" and data.get("mint"):
                        symbol = data.get("symbol", "MEME")
                        mint = data["mint"]

                        queue = asyncio.Queue()
                        bot.active_queues[mint] = queue

                        await ws.send(json.dumps({"method": "subscribeTokenTrade", "keys": [mint]}))
                        asyncio.create_task(bot.monitor_token_5m(symbol, mint, queue))

                    mint = data.get("mint")
                    if mint and mint in bot.active_queues:
                        bot.active_queues[mint].put_nowait(data)
        except Exception:
            await asyncio.sleep(2)


# ==========================================
# FASTAPI LIFESPAN & ROUTING
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    listener_task = asyncio.create_task(solana_listener())
    yield
    listener_task.cancel()

app = FastAPI(lifespan=lifespan)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Meme Coin 5m Spot Bot Dashboard</title>
        <style>
            * { box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
            body { background: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
            h1 { text-align: center; margin-bottom: 20px; font-size: 24px; color: #38bdf8; }
            .grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; height: calc(100vh - 80px); }
            .col { background: #1e293b; border-radius: 8px; padding: 15px; display: flex; flex-direction: column; overflow: hidden; border: 1px solid #334155; }
            .col-title { font-size: 16px; font-weight: bold; padding-bottom: 10px; margin-bottom: 10px; border-bottom: 2px solid #334155; display: flex; justify-content: space-between; align-items: center; }
            .card-list { flex: 1; overflow-y: auto; display: flex; flex-direction: column; gap: 10px; }
            .card { background: #0f172a; border-radius: 6px; padding: 12px; border: 1px solid #334155; }
            .card a { color: #38bdf8; text-decoration: none; font-weight: bold; font-size: 16px; }
            .card a:hover { text-decoration: underline; }
            .meta { font-size: 12px; color: #94a3b8; margin-top: 5px; }
            .green { color: #4ade80; }
            .red { color: #f87171; }
            .badge { padding: 2px 6px; border-radius: 4px; font-size: 11px; font-weight: bold; background: #334155; }
        </style>
    </head>
    <body>
        <h1>🚀 Solana 5m Spot Breakout Dashboard</h1>
        <div class="grid">
            <div class="col">
                <div class="col-title" style="color: #38bdf8;">
                    <span>1. Tracked Coins</span>
                    <span id="count-tracked" class="badge">0</span>
                </div>
                <div id="list-tracked" class="card-list"></div>
            </div>

            <div class="col">
                <div class="col-title" style="color: #facc15;">
                    <span>2. Active Positions</span>
                    <span id="count-bought" class="badge">0</span>
                </div>
                <div id="list-bought" class="card-list"></div>
            </div>

            <div class="col">
                <div class="col-title" style="color: #4ade80;">
                    <span>3. Profit / Loss History</span>
                    <span id="count-closed" class="badge">0</span>
                </div>
                <div id="list-closed" class="card-list"></div>
            </div>
        </div>

        <script>
            const ws = new WebSocket(`ws://${location.host}/ws`);
            
            function updateCount(id) {
                const list = document.getElementById('list-' + id);
                document.getElementById('count-' + id).innerText = list.children.length;
            }

            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);

                if (data.type === 'NEW_TRACKED') {
                    if (document.getElementById(`tracked-${data.mint}`)) return;
                    const card = document.createElement('div');
                    card.className = 'card';
                    card.id = `tracked-${data.mint}`;
                    card.innerHTML = `
                        <a href="${data.dex_url}" target="_blank">📈 ${data.symbol}</a>
                        <div class="meta" id="status-${data.mint}">${data.status}</div>
                    `;
                    document.getElementById('list-tracked').prepend(card);
                    updateCount('tracked');
                }
                else if (data.type === 'UPDATE_TRACKED') {
                    const el = document.getElementById(`status-${data.mint}`);
                    if (el) el.innerText = data.status;
                }
                else if (data.type === 'REMOVE_TRACKED') {
                    const card = document.getElementById(`tracked-${data.mint}`);
                    if (card) card.remove();
                    updateCount('tracked');
                }
                else if (data.type === 'NEW_BOUGHT') {
                    const card = document.createElement('div');
                    card.className = 'card';
                    card.id = `bought-${data.mint}`;
                    card.style.borderColor = '#facc15';
                    card.innerHTML = `
                        <a href="${data.dex_url}" target="_blank">🛒 ${data.symbol}</a>
                        <div class="meta">Entry: ${data.entry} | SL: ${data.sl} | TP: ${data.tp}</div>
                        <div class="meta">Price: <span id="price-${data.mint}">${data.current}</span></div>
                        <div class="meta">PnL: <span id="pnl-${data.mint}">$0.0000</span></div>
                    `;
                    document.getElementById('list-bought').prepend(card);
                    updateCount('bought');
                }
                else if (data.type === 'UPDATE_BOUGHT') {
                    const priceEl = document.getElementById(`price-${data.mint}`);
                    const pnlEl = document.getElementById(`pnl-${data.mint}`);
                    if (priceEl) priceEl.innerText = data.current;
                    if (pnlEl) {
                        pnlEl.innerText = `$${data.pnl} USD`;
                        pnlEl.className = parseFloat(data.pnl) >= 0 ? 'green' : 'red';
                    }
                }
                else if (data.type === 'REMOVE_BOUGHT') {
                    const card = document.getElementById(`bought-${data.mint}`);
                    if (card) card.remove();
                    updateCount('bought');
                }
                else if (data.type === 'NEW_CLOSED') {
                    const card = document.createElement('div');
                    card.className = 'card';
                    const isProfit = data.pnl.includes('+');
                    card.style.borderColor = isProfit ? '#22c55e' : '#ef4444';
                    card.innerHTML = `
                        <a href="${data.dex_url}" target="_blank">🏁 ${data.symbol}</a>
                        <div class="meta">Entry: ${data.entry} | Exit: ${data.exit}</div>
                        <div class="meta">Status: <span class="badge">${data.status}</span></div>
                        <div style="font-weight: bold; margin-top: 5px;" class="${isProfit ? 'green' : 'red'}">
                            PnL: ${data.pnl} USD
                        </div>
                    `;
                    document.getElementById('list-closed').prepend(card);
                    updateCount('closed');
                }
            };
        </script>
    </body>
    </html>
    """


if __name__ == "__main__":
    uvicorn.run("meme:app", host="127.0.0.1", port=8000, reload=True)
