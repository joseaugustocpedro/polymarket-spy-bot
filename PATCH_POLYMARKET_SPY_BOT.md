# Mudanças no `polymarket-spy-bot/bot.py` para Render Free

Objetivo: impedir que o Spy Bot consuma banda/API sem limite. Essas mudanças mantêm o bot funcionando, mas com freios para o plano gratuito.

---

## 1) Substitua o bloco de configurações

Procure no começo do arquivo:

```python
ALERT_CHAT_ID = os.environ.get("ALERT_CHAT_ID", "").strip()
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))
MIN_ALERT_USD = float(os.environ.get("MIN_ALERT_USD", "1000"))
LARGE_ALERT_USD = float(os.environ.get("LARGE_ALERT_USD", "5000"))
WHALE_ALERT_USD = float(os.environ.get("WHALE_ALERT_USD", "20000"))
SETTLE_SECONDS = int(os.environ.get("SETTLE_SECONDS", "60"))
SMALL_BATCH_MAX_AGE_SECONDS = int(
    os.environ.get("SMALL_BATCH_MAX_AGE_SECONDS", "900")
)
FETCH_LIMIT = int(os.environ.get("FETCH_LIMIT", "100"))
```

Substitua por:

```python
ALERT_CHAT_ID = os.environ.get("ALERT_CHAT_ID", "").strip()


def env_bool(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in {
        "1", "true", "yes", "sim", "on"
    }


FREE_TIER_MODE = env_bool("FREE_TIER_MODE", "true")
MONITORING_ENABLED = env_bool("MONITORING_ENABLED", "true")

# No modo free, não permite polling agressivo mesmo se a env vier errada.
_raw_poll_interval = int(os.environ.get("POLL_INTERVAL", "1800" if FREE_TIER_MODE else "60"))
POLL_INTERVAL = max(900 if FREE_TIER_MODE else 1, _raw_poll_interval)

MIN_ALERT_USD = float(os.environ.get("MIN_ALERT_USD", "5000" if FREE_TIER_MODE else "1000"))
LARGE_ALERT_USD = float(os.environ.get("LARGE_ALERT_USD", "15000" if FREE_TIER_MODE else "5000"))
WHALE_ALERT_USD = float(os.environ.get("WHALE_ALERT_USD", "50000" if FREE_TIER_MODE else "20000"))
SETTLE_SECONDS = int(os.environ.get("SETTLE_SECONDS", "240" if FREE_TIER_MODE else "60"))
SMALL_BATCH_MAX_AGE_SECONDS = int(
    os.environ.get("SMALL_BATCH_MAX_AGE_SECONDS", "7200" if FREE_TIER_MODE else "900")
)

_raw_fetch_limit = int(os.environ.get("FETCH_LIMIT", "10" if FREE_TIER_MODE else "100"))
FETCH_LIMIT = min(max(1, _raw_fetch_limit), 15) if FREE_TIER_MODE else max(1, _raw_fetch_limit)

MAX_WALLETS_PER_CYCLE = int(os.environ.get("MAX_WALLETS_PER_CYCLE", "1" if FREE_TIER_MODE else "0"))
API_CALL_BUDGET_PER_DAY = int(os.environ.get("API_CALL_BUDGET_PER_DAY", "120" if FREE_TIER_MODE else "0"))
DISABLE_DAILY_RANKING = env_bool("DISABLE_DAILY_RANKING", "true" if FREE_TIER_MODE else "false")
ENABLE_API_RANKING_COMMAND = env_bool("ENABLE_API_RANKING_COMMAND", "false" if FREE_TIER_MODE else "true")
ENABLE_PREVIEW_COMMAND = env_bool("ENABLE_PREVIEW_COMMAND", "false" if FREE_TIER_MODE else "true")
```

---

## 2) Adicione orçamento diário de chamadas externas

Logo depois de:

```python
BOT_TZ = ZoneInfo(BOT_TIMEZONE_NAME)
```

adicione:

```python
_api_budget_day = datetime.now(BOT_TZ).date()
_api_calls_today = 0
_wallet_cycle_offset = 0


def reset_api_budget_if_needed() -> None:
    global _api_budget_day, _api_calls_today
    today = datetime.now(BOT_TZ).date()
    if today != _api_budget_day:
        _api_budget_day = today
        _api_calls_today = 0


def can_make_external_api_call() -> bool:
    reset_api_budget_if_needed()
    return API_CALL_BUDGET_PER_DAY <= 0 or _api_calls_today < API_CALL_BUDGET_PER_DAY


def register_external_api_call() -> None:
    global _api_calls_today
    reset_api_budget_if_needed()
    _api_calls_today += 1


def api_budget_status() -> str:
    reset_api_budget_if_needed()
    if API_CALL_BUDGET_PER_DAY <= 0:
        return f"{_api_calls_today}/sem limite"
    return f"{_api_calls_today}/{API_CALL_BUDGET_PER_DAY}"
```

---

## 3) Troque a função `request_json`

Procure:

```python
def request_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    response = HTTP.get(url, params=params or {}, timeout=25)
    response.raise_for_status()
    return response.json()
```

Substitua por:

```python
def request_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    if not can_make_external_api_call():
        raise RuntimeError(
            f"Orçamento diário de chamadas externas atingido: {api_budget_status()}. "
            "O monitoramento será retomado automaticamente quando virar o dia."
        )
    register_external_api_call()
    response = HTTP.get(url, params=params or {}, timeout=20)
    response.raise_for_status()
    return response.json()
```

---

## 4) Adicione rodízio de wallets

Depois da função `get_active_wallets`, adicione:

```python
def select_wallets_for_cycle(wallets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    global _wallet_cycle_offset
    if MAX_WALLETS_PER_CYCLE <= 0 or len(wallets) <= MAX_WALLETS_PER_CYCLE:
        return wallets
    start = _wallet_cycle_offset % len(wallets)
    selected = [wallets[(start + i) % len(wallets)] for i in range(MAX_WALLETS_PER_CYCLE)]
    _wallet_cycle_offset = (start + MAX_WALLETS_PER_CYCLE) % len(wallets)
    return selected
```

---

## 5) Altere `process_monitoring`

Procure:

```python
async def process_monitoring(application: Application) -> Dict[str, int]:
    stats = {
        "wallets": 0,
        "inserted": 0,
        "bootstrapped": 0,
        "batches": 0,
        "ignored": 0,
        "sent": 0,
    }
    for wallet_row in get_active_wallets():
```

Substitua por:

```python
async def process_monitoring(application: Application) -> Dict[str, int]:
    stats = {
        "wallets": 0,
        "wallets_total": 0,
        "inserted": 0,
        "bootstrapped": 0,
        "batches": 0,
        "ignored": 0,
        "sent": 0,
        "api_budget": api_budget_status(),
    }

    if not MONITORING_ENABLED:
        logger.info("Monitoramento da Polymarket pausado por MONITORING_ENABLED=false.")
        return stats

    all_wallets = get_active_wallets()
    wallets_to_process = select_wallets_for_cycle(all_wallets)
    stats["wallets_total"] = len(all_wallets)

    for wallet_row in wallets_to_process:
```

---

## 6) Proteja comandos manuais que gastam API

No começo da função `preview`, adicione:

```python
    if not ENABLE_PREVIEW_COMMAND:
        await update.message.reply_text(
            "Preview desativado no modo free para economizar banda. "
            "Ative ENABLE_PREVIEW_COMMAND=true só quando precisar testar."
        )
        return
```

No começo da função `ranking_command`, adicione:

```python
    if not ENABLE_API_RANKING_COMMAND:
        await update.message.reply_text(
            "Ranking por API desativado no modo free para economizar banda. "
            "Ative ENABLE_API_RANKING_COMMAND=true apenas temporariamente."
        )
        return
```

---

## 7) Ajuste o health check `/`

Na resposta do endpoint `/`, acrescente:

```python
"free_tier_mode": FREE_TIER_MODE,
"monitoramento_ativo": MONITORING_ENABLED,
"fetch_limit": FETCH_LIMIT,
"max_wallets_por_ciclo": MAX_WALLETS_PER_CYCLE,
"api_budget": api_budget_status(),
"ranking_api_ativo": ENABLE_API_RANKING_COMMAND,
"preview_ativo": ENABLE_PREVIEW_COMMAND,
```

---

## 8) Desative ranking diário automático

Na função `build_daily_summary_text`, o código que chama `get_monitored_ranking("DAY", "ROI")` deve ficar dentro de:

```python
    if not DISABLE_DAILY_RANKING:
        try:
            ranking = get_monitored_ranking("DAY", "ROI")[:3]
            lines.extend(["", "Top 3 do dia por ROI:"])
            for index, row in enumerate(ranking, start=1):
                lines.append(
                    f"{index}. {html.escape(row['name'])} — "
                    f"ROI {row['roi']:+.2f}% • Lucro {format_money(row['pnl'])}"
                )
        except Exception:
            logger.exception("Não foi possível incluir o ranking no resumo diário.")
    else:
        lines.extend(["", "Ranking diário desativado no modo free para economizar banda."])
```
