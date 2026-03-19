function currentFamily() {
  return window.location.pathname.startsWith("/weather") ? "weather" : "crypto";
}

async function loadDashboard() {
  const family = currentFamily();
  const response = await fetch(`/v1/dashboard?family=${family}`);
  if (!response.ok) {
    throw new Error(`dashboard request failed: ${response.status}`);
  }
  return response.json();
}

async function loadRuntime() {
  const response = await fetch("/v1/runtime");
  if (!response.ok) {
    throw new Error(`runtime request failed: ${response.status}`);
  }
  return response.json();
}

const AUTO_REFRESH_MS = 15000;
let refreshPromise = null;

async function postOperatorAction(action) {
  const response = await fetch("/v1/operator/action", {
    method: "POST",
    headers: {
      "content-type": "application/json",
    },
    body: JSON.stringify({ action }),
  });
  if (!response.ok) {
    throw new Error(`operator action failed: ${response.status}`);
  }
  return response.json();
}

function money(value) {
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(value ?? 0);
}

function dateTime(value) {
  if (!value) return "n/a";
  return new Date(value).toLocaleString();
}

function parseLaneKey(laneKey) {
  const [exchange, symbol, windowMinutes, side, strategyFamily, modelName] = String(laneKey || "").split(":");
  return {
    exchange,
    symbol: symbol || null,
    windowMinutes: Number(windowMinutes) || null,
    side: side || null,
    strategyFamily: strategyFamily || null,
    modelName: modelName || null,
  };
}

function kalshiTradeUrl(trade) {
  const parsed = parseLaneKey(trade?.lane_key);
  const symbol = (parsed.symbol || "").toLowerCase();
  const windowMinutes = parsed.windowMinutes;
  const ticker = String(trade?.market_ticker || "").toLowerCase();
  const marketFamily = String(trade?.market_family || "").toLowerCase();

  if (ticker) {
    const series = ticker.split("-")[0];
    if (marketFamily === "weather" && series) {
      return `https://kalshi.com/markets/${series}`;
    }
    if (series && symbol && windowMinutes) {
      return `https://kalshi.com/markets/${series}/${symbol}-${windowMinutes}-minute/${ticker}`;
    }
  }

  if (symbol && windowMinutes) {
    const series = `kx${symbol}${windowMinutes}m`;
    return `https://kalshi.com/markets/${series}/${symbol}-${windowMinutes}-minute`;
  }

  return "https://kalshi.com/markets";
}

function titleCase(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function normalizeDegreeGlyphs(value) {
  return String(value || "").replaceAll(
    `${String.fromCharCode(194)}${String.fromCharCode(176)}`,
    String.fromCharCode(176),
  );
}

function formatWeatherTemp(value) {
  if (value == null || Number.isNaN(Number(value))) return null;
  const numeric = Number(value);
  const rounded = Number.isInteger(numeric) ? String(numeric) : String(numeric).replace(/\.0$/, "");
  return `${rounded}°`;
}

function formatWeatherMarketDate(value) {
  if (!value) return null;
  const parsed = new Date(`${value}T12:00:00Z`);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function weatherBucketLabel(trade) {
  const strikeType = String(trade?.weather_strike_type || "").toLowerCase();
  const floor = Number(trade?.weather_floor_strike);
  const cap = Number(trade?.weather_cap_strike);

  if (strikeType === "greater" && Number.isFinite(floor)) {
    return `${formatWeatherTemp(floor + 1)} or above`;
  }
  if (strikeType === "less" && Number.isFinite(cap)) {
    return `${formatWeatherTemp(cap - 1)} or below`;
  }
  if (strikeType === "between" && Number.isFinite(floor) && Number.isFinite(cap)) {
    return `${formatWeatherTemp(floor)} to ${formatWeatherTemp(cap)}`;
  }
  return null;
}

function weatherContractSummary(trade) {
  const parsed = parseLaneKey(trade?.lane_key);
  const city = trade?.weather_city || parsed.symbol || "Weather";
  const contractKind = String(trade?.weather_contract_kind || "").toLowerCase();
  const kindLabel = contractKind === "daily_low_temperature"
    ? "low temp"
    : contractKind === "daily_high_temperature"
      ? "high temp"
      : null;
  const bucket = weatherBucketLabel(trade);
  const marketDate = formatWeatherMarketDate(trade?.weather_market_date);
  return normalizeDegreeGlyphs(cleanMarketTitle([titleCase(city), kindLabel, bucket, marketDate].filter(Boolean).join(" · ")));
}

function cleanMarketTitle(value) {
  return String(value || "")
    .replaceAll("**", "")
    .replaceAll("Â°", "°")
    .replace(/\s+/g, " ")
    .trim();
}

function tradeHeadline(trade) {
  if (trade?.market_family === "weather") {
    return weatherContractSummary(trade) || cleanMarketTitle(trade?.market_title || "");
  }
  const parsed = parseLaneKey(trade?.lane_key);
  const symbol = (parsed.symbol || "market").toUpperCase();
  const side = String(parsed.side || "").toLowerCase();
  if (side === "buy_yes") return `${symbol} YES`;
  if (side === "buy_no") return `${symbol} NO`;
  return symbol;
}

function tradeSubhead(trade) {
  const parsed = parseLaneKey(trade?.lane_key);
  if (trade?.market_family === "weather") {
    const direction = tradeDirectionLabel(trade);
    const bucket = weatherBucketLabel(trade);
    return normalizeDegreeGlyphs([direction, bucket, titleCase(parsed.strategyFamily || trade?.strategy_family)].filter(Boolean).join(" · "));
  }
  const windowText = parsed.windowMinutes ? `${parsed.windowMinutes}m` : "short horizon";
  const strategyText = titleCase(parsed.strategyFamily || trade?.strategy_family);
  return `${windowText} ${strategyText}`;
}

function tradeDirectionTone(trade) {
  const side = String(parseLaneKey(trade?.lane_key).side || "").toLowerCase();
  if (side === "buy_yes") return "good";
  if (side === "buy_no") return "warn";
  return "";
}

function tradeDirectionLabel(trade) {
  const side = String(parseLaneKey(trade?.lane_key).side || "").toLowerCase();
  if (side === "buy_yes") return "Buy YES";
  if (side === "buy_no") return "Buy NO";
  return titleCase(side || "Unknown");
}

function relativeAge(value) {
  if (!value) return "n/a";
  const seconds = Math.max(0, Math.round((Date.now() - new Date(value).getTime()) / 1000));
  if (seconds < 60) return `${seconds}s open`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m open`;
  const hours = Math.floor(minutes / 60);
  const remMinutes = minutes % 60;
  if (hours < 24) return remMinutes ? `${hours}h ${remMinutes}m open` : `${hours}h open`;
  const days = Math.floor(hours / 24);
  return `${days}d open`;
}

function familyLabel(value) {
  if (value === "all") return "Total account";
  if (value === "weather") return "Weather";
  return "Crypto";
}

function statusTone(status) {
  const normalized = String(status || "").toLowerCase();
  if (normalized.includes("healthy") || normalized.includes("ok") || normalized.includes("armed")) return "good";
  if (
    normalized.includes("warn") ||
    normalized.includes("candidate") ||
    normalized.includes("shadow") ||
    normalized.includes("safe")
  ) return "warn";
  if (
    normalized.includes("critical") ||
    normalized.includes("attention") ||
    normalized.includes("quarantined") ||
    normalized.includes("failed") ||
    normalized.includes("disabled")
  ) return "bad";
  return "";
}

function opportunityLabel(item) {
  if (!item.approved) return "Blocked";
  if (item.execution_note === "existing_open_trade_for_lane") return "Held";
  if (item.execution_status === "opened") return "Executed";
  if (item.execution_status === "pending") return "Queued";
  if (item.execution_status === "submitted" || item.execution_status === "acknowledged") return "Routing";
  if (item.execution_status) return titleCase(item.execution_status);
  return "Approved";
}

function opportunityTone(item) {
  if (!item.approved) return "warn";
  if (item.execution_note === "existing_open_trade_for_lane") return "warn";
  if (item.execution_status === "pending" || item.execution_status === "submitted" || item.execution_status === "acknowledged") return "warn";
  if (item.execution_status === "cancelled" || item.execution_status === "rejected" || item.execution_status === "orphaned") return "bad";
  return "good";
}

function opportunityExecutionSummary(item) {
  if (item.execution_note === "existing_open_trade_for_lane") {
    return "Execution held because this lane already had an open trade when the decision was approved.";
  }
  if (item.execution_status === "opened") {
    return "Execution opened a trade from this decision.";
  }
  if (item.execution_status === "pending") {
    return "Execution intent created and waiting for the execution worker.";
  }
  if (item.execution_status === "submitted" || item.execution_status === "acknowledged") {
    return `Execution intent is currently ${item.execution_status}.`;
  }
  if (item.execution_status === "cancelled" || item.execution_status === "rejected" || item.execution_status === "orphaned") {
    return `Execution ended as ${titleCase(item.execution_status)}${item.execution_note ? `: ${item.execution_note}` : "."}`;
  }
  if (item.execution_note) {
    return `Execution note: ${item.execution_note}`;
  }
  if (item.approved) {
    return "Approved by decisioning.";
  }
  return "";
}

function bankrollFreshness(item) {
  const ageSeconds = Math.max(0, (Date.now() - new Date(item.as_of).getTime()) / 1000);
  if (item.mode === "live" && ageSeconds > 300) {
    return { label: "Live stale", tone: "warn" };
  }
  if (item.mode === "paper" && ageSeconds > 120) {
    return { label: "Paper stale", tone: "warn" };
  }
  return { label: titleCase(item.mode), tone: "" };
}

function badgeTone(value, fallback = "neutral") {
  const tone = statusTone(value);
  return tone || fallback;
}

function sumExposure(items, mode) {
  return (items || [])
    .filter((item) => item.mode === mode)
    .reduce((total, item) => total + (Number(item.open_exposure) || 0), 0);
}

function liveActionState(payload, runtime, readiness) {
  const exceptions = payload?.live_exceptions || {};
  const urgentCount =
    (exceptions.trade_exceptions || []).length +
    (exceptions.orders || []).length +
    (runtime?.alarms || []).length;
  if (urgentCount > 0) {
    return { label: "Action needed", tone: "bad", detail: `${urgentCount} urgent item${urgentCount === 1 ? "" : "s"}` };
  }
  if ((readiness?.quarantined_count || 0) > 0) {
    return {
      label: "Watch lanes",
      tone: "warn",
      detail: `${readiness.quarantined_count} quarantined lane${readiness.quarantined_count === 1 ? "" : "s"}`,
    };
  }
  return { label: "No urgent action", tone: "good", detail: "No live exceptions or runtime alarms" };
}

function executionTruthLabel(summary) {
  if (!summary) {
    return { label: "Unavailable", tone: "warn", detail: "No execution truth payload" };
  }
  if (!summary.live_sample_sufficient) {
    return {
      label: "Diagnostic Only",
      tone: "neutral",
      detail: `${summary.recent_live_terminal_intent_count || 0} terminal live intents`,
    };
  }
  const tone = summary.live_sample_sufficient ? executionQualityTone(summary) : "neutral";
  if (tone === "good") return { label: "Actionable", tone: "good", detail: "Live sample threshold met" };
  if (tone === "bad") return { label: "Needs caution", tone: "bad", detail: "Live truth diverges from forecast" };
  return { label: "Watch closely", tone: "warn", detail: "Evidence exists but confidence is mixed" };
}

function renderDecisionBar(payload, runtime, readiness) {
  const root = document.getElementById("decision-grid");
  const liveEnabled = runtime?.live_trading_enabled && runtime?.live_order_placement_enabled;
  const exchangeStatus = payload?.live_sync?.status || "unavailable";
  const executionStatus = executionTruthLabel(payload?.execution_quality || runtime?.execution_quality || null);
  const actionState = liveActionState(payload, runtime, readiness);
  const liveExposure = sumExposure(payload?.bankrolls || [], "live");
  const exposureLabel = liveExposure > 0 ? money(liveExposure) : currentFamily() === "weather" ? "Paper only" : "No live exposure";

  root.innerHTML = [
    {
      eyebrow: "Live routing",
      value: liveEnabled ? "Enabled" : "Disabled",
      tone: liveEnabled ? "warn" : "neutral",
      detail: liveEnabled ? "Orders can route live right now" : "Live placement is off",
    },
    {
      eyebrow: "Exchange truth",
      value: titleCase(exchangeStatus),
      tone: badgeTone(exchangeStatus, "warn"),
      detail: payload?.live_sync?.issues?.length
        ? payload.live_sync.issues.join(", ")
        : "Bankroll, positions, orders, and fills agree",
    },
    {
      eyebrow: "Execution truth",
      value: executionStatus.label,
      tone: executionStatus.tone,
      detail: executionStatus.detail,
    },
    {
      eyebrow: "Live capital at risk",
      value: exposureLabel,
      tone: liveExposure > 0 ? "warn" : "neutral",
      detail: liveExposure > 0 ? "Open live exposure on the book" : "No live dollars currently at risk",
    },
    {
      eyebrow: "Action state",
      value: actionState.label,
      tone: actionState.tone,
      detail: actionState.detail,
    },
  ].map((item) => `
    <section class="decision-tile">
      <p class="eyebrow">${item.eyebrow}</p>
      <strong class="${item.tone}">${item.value}</strong>
      <p class="muted">${item.detail}</p>
    </section>
  `).join("");
}

function renderBankrolls(items) {
  const root = document.getElementById("bankroll-list");
  if (!items.length) {
    root.innerHTML = `<div class="empty">No bankroll snapshots yet. Once execution writes bankroll data, it will appear here.</div>`;
    return;
  }

  const grouped = items.reduce((acc, item) => {
    const key = `${item.mode}:${item.market_family || "all"}`;
    if (!acc[key]) {
      acc[key] = {
        mode: item.mode,
        market_family: item.market_family || "all",
        bankroll: 0,
        deployable_balance: 0,
        open_exposure: 0,
        realized_pnl: 0,
        unrealized_pnl: 0,
        as_of: item.as_of,
      };
    }
    acc[key].bankroll += item.bankroll ?? 0;
    acc[key].deployable_balance += item.deployable_balance ?? 0;
    acc[key].open_exposure += item.open_exposure ?? 0;
    acc[key].realized_pnl += item.realized_pnl ?? 0;
    acc[key].unrealized_pnl += item.unrealized_pnl ?? 0;
    if (new Date(item.as_of) > new Date(acc[key].as_of)) {
      acc[key].as_of = item.as_of;
    }
    return acc;
  }, {});

  root.innerHTML = Object.values(grouped).map((item) => `
    <section class="metric-card">
      <header>
        <div>
          <p class="eyebrow">${familyLabel(item.market_family)} · ${titleCase(item.mode)} bankroll</p>
          <h3>${money(item.bankroll)}</h3>
        </div>
        <div class="chip ${bankrollFreshness(item).tone}">${bankrollFreshness(item).label}</div>
      </header>
      <dl class="metric-grid">
        <div class="metric"><dt>Deployable</dt><dd>${money(item.deployable_balance)}</dd></div>
        <div class="metric"><dt>Open exposure</dt><dd>${money(item.open_exposure)}</dd></div>
        <div class="metric"><dt>Realized PnL</dt><dd class="${item.realized_pnl >= 0 ? "good" : "bad"}">${money(item.realized_pnl)}</dd></div>
        <div class="metric"><dt>Unrealized PnL</dt><dd class="${item.unrealized_pnl >= 0 ? "good" : "bad"}">${money(item.unrealized_pnl)}</dd></div>
      </dl>
      <p class="muted">As of ${dateTime(item.as_of)}</p>
    </section>
  `).join("");
}

function renderReadiness(readiness) {
  document.getElementById("readiness-badge").textContent = titleCase(readiness.overall_status);
  document.getElementById("readiness-badge").className = `readiness-badge ${statusTone(readiness.overall_status)}`;
  document.getElementById("hero-status").textContent = titleCase(readiness.overall_status);
  document.getElementById("hero-status").className = `hero-status ${statusTone(readiness.overall_status)}`;

  const states = [
    ["Shadow", readiness.shadow_count],
    ["Paper", readiness.paper_active_count],
    ["Live Micro", readiness.live_micro_count],
    ["Live Scaled", readiness.live_scaled_count],
    ["Quarantined", readiness.quarantined_count],
  ];
  document.getElementById("state-strip").innerHTML = states.map(([label, count]) => `
    <div class="state-pill">
      <span class="eyebrow">${label}</span>
      <strong>${count}</strong>
    </div>
  `).join("");

  const root = document.getElementById("lane-list");
  if (!readiness.lanes.length) {
    root.innerHTML = `<div class="empty">No lane states yet. Training and decision workers will populate readiness here.</div>`;
    return;
  }
  root.innerHTML = readiness.lanes.map((lane) => `
    <section class="lane-card">
      <header>
        <div>
          <p class="eyebrow">${titleCase(lane.promotion_state)}</p>
          <strong>${lane.lane_key}</strong>
        </div>
        <div class="chip">${lane.current_champion_model || "No champion"}</div>
      </header>
      <dl class="metric-grid">
        <div class="metric"><dt>Recent PnL</dt><dd class="${lane.recent_pnl >= 0 ? "good" : "bad"}">${money(lane.recent_pnl)}</dd></div>
        <div class="metric"><dt>Replay EV</dt><dd class="${lane.recent_replay_expectancy >= 0 ? "good" : "bad"}">${money(lane.recent_replay_expectancy)}</dd></div>
        <div class="metric"><dt>Brier</dt><dd class="mono">${lane.recent_brier.toFixed(3)}</dd></div>
        <div class="metric"><dt>Execution</dt><dd class="mono">${lane.recent_execution_quality.toFixed(3)}</dd></div>
      </dl>
      ${lane.quarantine_reason ? `<p class="bad">Quarantine: ${lane.quarantine_reason}</p>` : ""}
    </section>
  `).join("");
}

function renderLiveSync(sync) {
  const badge = document.getElementById("live-sync-badge");
  const strip = document.getElementById("live-sync-strip");
  const root = document.getElementById("live-sync-list");
  if (!sync) {
    badge.textContent = "Unavailable";
    badge.className = "readiness-badge warn";
    strip.innerHTML = "";
    root.innerHTML = `<div class="empty">No live exchange sync yet. Ingest will populate exchange truth from Kalshi positions, orders, and fills.</div>`;
    return;
  }

  badge.textContent = titleCase(sync.status);
  badge.className = `readiness-badge ${statusTone(sync.status)}`;
  strip.innerHTML = `
    <div class="state-pill"><span class="eyebrow">Positions</span><strong>${sync.positions_count}</strong></div>
    <div class="state-pill"><span class="eyebrow">Resting Orders</span><strong>${sync.resting_orders_count}</strong></div>
    <div class="state-pill"><span class="eyebrow">Recent Fills</span><strong>${sync.recent_fills_count}</strong></div>
    <div class="state-pill"><span class="eyebrow">Local Live Trades</span><strong>${sync.local_open_live_trades_count}</strong></div>
  `;
  root.innerHTML = `
    <section class="lane-card">
      <header>
        <div>
          <p class="eyebrow">Synced</p>
          <strong>${dateTime(sync.synced_at)}</strong>
        </div>
        <div class="chip ${sync.issues?.length ? "warn" : "good"}">${sync.issues?.length ? "Attention" : "Aligned"}</div>
      </header>
      ${
        sync.issues?.length
          ? `<p class="bad">${sync.issues.join(", ")}</p>`
          : `<p class="muted">Live bankroll, positions, and order state are being checked together instead of trusting bankroll alone.</p>`
      }
    </section>
  `;
}

function pct(value) {
  return `${((Number(value) || 0) * 100).toFixed(1)}%`;
}

function ratio(value) {
  return (Number(value) || 0).toFixed(2);
}

function executionQualityTone(summary) {
  if (!summary) return "warn";
  if (!summary.live_sample_sufficient) return "warn";
  const gap = Math.abs(
    (summary.recent_live_predicted_fill_probability_mean || 0) -
      (summary.recent_live_filled_quantity_ratio || 0)
  );
  const replayEdge = summary.replay_sample_sufficient
    ? (summary.replay_trade_weighted_edge_realization_ratio_diag || 0)
    : null;
  if ((replayEdge != null && replayEdge < 0) || gap > 0.30) return "bad";
  if ((replayEdge != null && replayEdge < 0.5) || gap > 0.15) return "warn";
  return "good";
}

function renderExecutionQuality(summary) {
  const badge = document.getElementById("execution-quality-badge");
  const strip = document.getElementById("execution-quality-strip");
  const root = document.getElementById("execution-quality-list");
  if (!summary) {
    badge.textContent = "Unavailable";
    badge.className = "readiness-badge warn";
    strip.innerHTML = "";
    root.innerHTML = `<div class="empty">Execution truth is not available yet. Once typed live fill outcomes exist, this panel will move out of diagnostic mode.</div>`;
    return;
  }

  const tone = executionQualityTone(summary);
  const fillGap =
    (summary.recent_live_filled_quantity_ratio || 0) -
    (summary.recent_live_predicted_fill_probability_mean || 0);
  badge.textContent = summary.live_sample_sufficient
    ? titleCase(tone === "good" ? "Actionable" : tone === "warn" ? "Watch" : "Drift")
    : "Insufficient Sample";
  badge.className = `readiness-badge ${tone}`;
  strip.innerHTML = `
    <div class="state-pill"><span class="eyebrow">Live sample</span><strong>${summary.live_sample_sufficient ? "Trustworthy" : "Insufficient"}</strong></div>
    <div class="state-pill"><span class="eyebrow">Replay sample</span><strong>${summary.replay_sample_sufficient ? "Diagnostic ready" : "Weak"}</strong></div>
    <div class="state-pill"><span class="eyebrow">Predicted fill</span><strong>${summary.recent_live_predicted_fill_probability_mean == null ? "Diagnostic Only" : pct(summary.recent_live_predicted_fill_probability_mean)}</strong></div>
    <div class="state-pill"><span class="eyebrow">Filled qty ratio</span><strong>${summary.recent_live_filled_quantity_ratio == null ? "Diagnostic Only" : pct(summary.recent_live_filled_quantity_ratio)}</strong></div>
  `;
  root.innerHTML = `
    <section class="lane-card">
      <header>
        <div>
          <p class="eyebrow">Execution truth</p>
          <strong>${summary.live_sample_sufficient ? "Measured live evidence is usable" : "Stay humble with this panel"}</strong>
        </div>
        <div class="chip ${summary.live_sample_sufficient ? tone : "neutral"}">${summary.live_sample_sufficient ? `As of ${dateTime(summary.as_of)}` : "Diagnostic Only"}</div>
      </header>
      <dl class="metric-grid">
        <div class="metric"><dt>Terminal live intents</dt><dd class="mono">${summary.recent_live_terminal_intent_count}</dd></div>
        <div class="metric"><dt>Predicted live sample</dt><dd class="mono">${summary.recent_live_predicted_fill_sample_count}</dd></div>
        <div class="metric"><dt>Actual fill hit rate</dt><dd>${summary.recent_live_actual_fill_hit_rate == null ? "Diagnostic Only" : pct(summary.recent_live_actual_fill_hit_rate)}</dd></div>
        <div class="metric"><dt>Replay edge diag</dt><dd class="${(summary.replay_trade_weighted_edge_realization_ratio_diag || 0) >= 1 ? "good" : (summary.replay_trade_weighted_edge_realization_ratio_diag || 0) < 0 ? "bad" : ""}">${summary.replay_trade_weighted_edge_realization_ratio_diag == null ? "Diagnostic Only" : ratio(summary.replay_trade_weighted_edge_realization_ratio_diag)}</dd></div>
        <div class="metric"><dt>Replay fill rate</dt><dd>${summary.replay_trade_weighted_fill_rate_diag == null ? "Diagnostic Only" : pct(summary.replay_trade_weighted_fill_rate_diag)}</dd></div>
        <div class="metric"><dt>Replay slippage</dt><dd class="mono">${summary.replay_trade_weighted_slippage_bps_diag == null ? "Diagnostic Only" : `${summary.replay_trade_weighted_slippage_bps_diag.toFixed(1)} bps`}</dd></div>
      </dl>
      ${
        summary.live_sample_sufficient && summary.recent_live_predicted_fill_probability_mean != null && summary.recent_live_filled_quantity_ratio != null
          ? `<p class="${Math.abs(fillGap) > 0.15 ? "bad" : "muted"}">Predicted fill ${pct(summary.recent_live_predicted_fill_probability_mean)} versus realized filled quantity ${pct(summary.recent_live_filled_quantity_ratio)} (${fillGap >= 0 ? "+" : ""}${pct(fillGap)} gap).</p>`
          : `<p class="diagnostic-copy">Diagnostic only. Do not treat this panel as a green light until live samples are sufficient.</p>`
      }
      ${!summary.replay_sample_sufficient ? `<p class="muted">Replay remains diagnostic only until trade-weighted replay has enough trades to be comparable.</p>` : ""}
    </section>
  `;
}

function executionTruthStatusTone(status) {
  switch (status) {
    case "good":
      return "good";
    case "watch":
      return "warn";
    case "quarantine_candidate":
      return "bad";
    default:
      return "warn";
  }
}

function formatLaneTruthLabel(laneKey) {
  const parsed = parseLaneKey(laneKey);
  const pieces = [];
  if (parsed.symbol) pieces.push(String(parsed.symbol).toUpperCase());
  if (parsed.side) pieces.push(titleCase(parsed.side));
  if (parsed.windowMinutes) pieces.push(`${parsed.windowMinutes}m`);
  return pieces.join(" · ") || laneKey;
}

function renderMoneyView(payload) {
  const badge = document.getElementById("money-view-badge");
  const familyStrip = document.getElementById("money-view-family");
  const root = document.getElementById("money-view-list");
  const laneRows = payload?.lane_execution_truth || [];
  const familyRows = payload?.family_execution_truth || [];
  if (!laneRows.length && !familyRows.length) {
    badge.textContent = "Unavailable";
    badge.className = "readiness-badge warn";
    familyStrip.innerHTML = "";
    root.innerHTML = `<div class="empty">Lane-level live execution truth is not available yet.</div>`;
    return;
  }

  const worstLane = laneRows[0];
  const worstTone = executionTruthStatusTone(worstLane?.status || familyRows[0]?.status || "insufficient_sample");
  badge.textContent = worstLane?.live_sample_sufficient ? titleCase(worstLane?.status || "watch") : "Diagnostic Only";
  badge.className = `readiness-badge ${worstTone}`;

  familyStrip.innerHTML = familyRows.map((row) => `
    <div class="state-pill">
      <span class="eyebrow">${familyLabel(row.market_family)} · ${titleCase(row.mode)}</span>
      <strong class="${executionTruthStatusTone(row.status)}">${row.live_sample_sufficient ? titleCase(row.status) : "Diagnostic Only"}</strong>
    </div>
  `).join("");

  root.innerHTML = laneRows.map((row) => {
    const predicted = row.recent_live_predicted_fill_probability_mean;
    const filled = row.recent_live_filled_quantity_ratio;
    const actual = row.recent_live_actual_fill_hit_rate;
    const replay = row.replay_trade_weighted_edge_realization_ratio_diag;
    const recommendation = row.manual_reenable_required
      ? "Manual Re-enable"
      : row.block_promotion_recommended
        ? "Quarantine Candidate"
        : row.live_sample_sufficient
          ? titleCase(row.status)
          : "Insufficient Sample";
    const recommendationTone = row.manual_reenable_required
      ? "bad"
      : row.block_promotion_recommended
        ? "warn"
        : executionTruthStatusTone(row.status);

    return `
      <article class="triage-card">
        <div class="triage-head">
          <div>
            <p class="eyebrow">${row.mode ? `${titleCase(row.mode)} · ` : ""}${row.promotion_state ? titleCase(row.promotion_state) : "Lane"}</p>
            <strong>${formatLaneTruthLabel(row.lane_key)}</strong>
            <p class="muted">${row.lane_key}</p>
          </div>
          <div class="triage-meta">
            <span class="triage-chip ${row.live_sample_sufficient ? executionTruthStatusTone(row.status) : "neutral"}">${row.live_sample_sufficient ? titleCase(row.status) : "Diagnostic Only"}</span>
            <span class="triage-chip ${recommendationTone}">${recommendation}</span>
          </div>
        </div>
        <div class="triage-grid">
          <div class="triage-metric"><dt>Live sample</dt><dd>${row.recent_live_terminal_intent_count} terminal</dd></div>
          <div class="triage-metric"><dt>Predicted fill</dt><dd>${predicted == null ? "Diagnostic Only" : pct(predicted)}</dd></div>
          <div class="triage-metric"><dt>Filled qty ratio</dt><dd>${filled == null ? "Diagnostic Only" : pct(filled)}</dd></div>
          <div class="triage-metric"><dt>Actual fill hit</dt><dd>${actual == null ? "Diagnostic Only" : pct(actual)}</dd></div>
          <div class="triage-metric"><dt>Replay diagnostic</dt><dd>${replay == null ? "Diagnostic Only" : ratio(replay)}</dd></div>
          <div class="triage-metric"><dt>Live vs replay gap</dt><dd>${row.live_vs_replay_fill_gap == null ? "Diagnostic Only" : `${row.live_vs_replay_fill_gap >= 0 ? "+" : ""}${pct(row.live_vs_replay_fill_gap)}`}</dd></div>
        </div>
        <p class="${row.live_sample_sufficient ? "muted" : "diagnostic-copy"}">${
          row.live_sample_sufficient
            ? (row.recommendation_reason
              ? `${titleCase(row.recommendation_reason)}${row.recommended_size_multiplier != null && row.recommended_size_multiplier < 1 ? ` · size cap ${pct(row.recommended_size_multiplier)}` : ""}`
              : "No degradation signal from recent live truth.")
            : "Diagnostic only. This lane has not earned enough live evidence to deserve capital automatically."
        }</p>
      </article>
    `;
  }).join("");
}

function freshnessLabel(seconds, staleAfter) {
  if (seconds == null) return { label: "Unknown", tone: "warn" };
  if (seconds > staleAfter) return { label: `Stale ${seconds}s`, tone: "bad" };
  if (seconds > Math.floor(staleAfter / 2)) return { label: `${seconds}s old`, tone: "warn" };
  return { label: `${seconds}s old`, tone: "good" };
}

function renderOperatorState(runtime, readiness) {
  const badge = document.getElementById("ops-badge");
  const strip = document.getElementById("ops-strip");
  const root = document.getElementById("ops-list");
  if (!runtime) {
    badge.textContent = "Unavailable";
    badge.className = "readiness-badge warn";
    strip.innerHTML = "";
    root.innerHTML = `<div class="empty">Runtime state is unavailable.</div>`;
    return;
  }

  const marketFreshness = freshnessLabel(runtime.market_feed_age_seconds, 45);
  const referenceFreshness = freshnessLabel(runtime.reference_feed_age_seconds, 45);
  const bankrollFreshnessState = freshnessLabel(runtime.live_bankroll_age_seconds, 180);
  const exchangeFreshness = freshnessLabel(runtime.live_exchange_sync_age_seconds, 45);
  const intentCounts = Object.fromEntries(runtime.intent_status_counts || []);
  const pendingCount = intentCounts.pending || 0;
  const rejectedCount = intentCounts.rejected || 0;
  const acknowledgedCount = intentCounts.acknowledged || 0;
  const quarantined = (readiness?.lanes || []).filter((lane) => lane.promotion_state === "quarantined");
  const attention = buildAttentionNeeded(readiness);
  const acceptanceGates = runtime.acceptance_gates || [];

  badge.textContent = runtime.live_order_placement_enabled ? "Armed" : "Safe";
  badge.className = `readiness-badge ${runtime.live_order_placement_enabled ? "warn" : "good"}`;
  strip.innerHTML = `
    <div class="state-pill"><span class="eyebrow">Market feed</span><strong class="${marketFreshness.tone}">${marketFreshness.label}</strong></div>
    <div class="state-pill"><span class="eyebrow">Reference feed</span><strong class="${referenceFreshness.tone}">${referenceFreshness.label}</strong></div>
    <div class="state-pill"><span class="eyebrow">Live bankroll</span><strong class="${bankrollFreshnessState.tone}">${bankrollFreshnessState.label}</strong></div>
    <div class="state-pill"><span class="eyebrow">Exchange sync</span><strong class="${exchangeFreshness.tone}">${exchangeFreshness.label}</strong></div>
    <div class="state-pill"><span class="eyebrow">Pending intents</span><strong class="${pendingCount ? "warn" : "good"}">${pendingCount}</strong></div>
    <div class="state-pill"><span class="eyebrow">Ack intents</span><strong class="${acknowledgedCount ? "warn" : ""}">${acknowledgedCount}</strong></div>
    <div class="state-pill"><span class="eyebrow">Rejected intents</span><strong class="${rejectedCount ? "warn" : ""}">${rejectedCount}</strong></div>
  `;

  root.innerHTML = `
    <section class="lane-card">
      <header>
        <div>
          <p class="eyebrow">Trading mode</p>
          <strong>${runtime.live_trading_enabled ? "Live capable" : "Paper only"}</strong>
        </div>
        <div class="chip ${runtime.live_order_placement_enabled ? "warn" : "good"}">${runtime.reference_price_source}</div>
      </header>
      <p class="muted">Live placement is ${runtime.live_order_placement_enabled ? "enabled" : "disabled"} and the operator surface now includes feed alarms, exchange exceptions, and manual controls.</p>
      ${
        runtime.alarms?.length
          ? `<div class="issue-list">${runtime.alarms
              .map(
                (alarm) => `
                  <div class="issue-item">
                    <strong class="${statusTone(alarm.severity)}">${titleCase(alarm.severity)} · ${titleCase(alarm.code)}</strong>
                    <span>${alarm.message}</span>
                  </div>
                `
              )
              .join("")}</div>`
          : `<p class="muted">No active runtime alarms.</p>`
      }
      ${
        acceptanceGates.length
          ? `
            <section class="lane-card">
              <header>
                <div>
                  <p class="eyebrow">Acceptance gates</p>
                  <strong>Prod readiness proof</strong>
                </div>
              </header>
              <div class="issue-list">${acceptanceGates
                .map(
                  (gate) => `
                    <div class="issue-item">
                      <strong class="${gate.status === "passed" ? "good" : "warn"}">Gate ${gate.gate} · ${titleCase(gate.status)}</strong>
                      <span>${gate.message}</span>
                    </div>
                  `
                )
                .join("")}</div>
            </section>
          `
          : ""
      }
      ${
        attention
          ? `
            <section class="lane-card attention-card">
              <header>
                <div>
                  <p class="eyebrow">Attention Needed</p>
                  <strong>${attention.laneKey}</strong>
                </div>
                <div class="chip ${attention.tone}">${attention.label}</div>
              </header>
              <dl class="metric-grid">
                <div class="metric"><dt>Reason</dt><dd>${attention.reason}</dd></div>
                <div class="metric"><dt>Recent PnL</dt><dd class="${attention.pnl >= 0 ? "good" : "bad"}">${money(attention.pnl)}</dd></div>
                <div class="metric"><dt>Replay EV</dt><dd class="${attention.replay >= 0 ? "good" : "bad"}">${money(attention.replay)}</dd></div>
                <div class="metric"><dt>Brier</dt><dd class="mono">${attention.brier.toFixed(3)}</dd></div>
              </dl>
              <p class="muted">Next criterion: ${attention.nextCriterion}</p>
            </section>
          `
          : ""
      }
      ${
        quarantined.length
          ? `<div class="issue-list">${quarantined
              .map(
                (lane) =>
                  `<div class="issue-item"><strong>${lane.lane_key}</strong><span>${lane.quarantine_reason || "quarantined"}</span></div>`
              )
              .join("")}</div>`
          : ``
      }
    </section>
  `;
}

function buildAttentionNeeded(readiness) {
  const lanes = readiness?.lanes || [];
  const target =
    lanes.find((lane) => lane.promotion_state === "quarantined") ||
    lanes.find((lane) => lane.promotion_state === "shadow");
  if (!target) return null;

  let nextCriterion = "needs stronger lane evidence";
  if (target.promotion_state === "quarantined") {
    if ((target.quarantine_reason || "").includes("paper_pnl")) {
      nextCriterion = "recent paper PnL must recover above the quarantine floor";
    } else if (target.recent_replay_expectancy < 0) {
      nextCriterion = "replay expectancy must turn positive before the lane can re-enter paper";
    } else {
      nextCriterion = "lane must clear quarantine and requalify for paper-active";
    }
  } else if (target.promotion_state === "shadow") {
    if (target.recent_replay_expectancy < 0) {
      nextCriterion = "replay expectancy must turn positive for paper promotion";
    } else if (target.recent_brier > 0.20) {
      nextCriterion = "forecast Brier must improve before paper promotion";
    } else {
      nextCriterion = "paper gate requires acceptable replay and execution quality";
    }
  }

  return {
    laneKey: target.lane_key,
    label: titleCase(target.promotion_state),
    tone: statusTone(target.promotion_state),
    reason: titleCase(target.promotion_reason || target.quarantine_reason || target.promotion_state),
    pnl: target.recent_pnl ?? 0,
    replay: target.recent_replay_expectancy ?? 0,
    brier: target.recent_brier ?? 0,
    nextCriterion,
  };
}

function renderLiveExceptions(payload) {
  const badge = document.getElementById("exceptions-badge");
  const strip = document.getElementById("exceptions-strip");
  const root = document.getElementById("exceptions-list");
  const feedback = document.getElementById("control-feedback");
  const exceptions = payload.live_exceptions || {
    operator_control: null,
    positions: [],
    orders: [],
    recent_fills: [],
    trade_exceptions: [],
    live_intents: [],
  };

  const control = exceptions.operator_control;
  const openOrderCount = (exceptions.orders || []).length;
  const tradeExceptionCount = (exceptions.trade_exceptions || []).length;
  const liveIntentCount = (exceptions.live_intents || []).length;
  const hasAttention = tradeExceptionCount > 0 || openOrderCount > 0 || liveIntentCount > 0;

  badge.textContent = control?.live_order_placement_enabled ? "Enabled" : "Disabled";
  badge.className = `readiness-badge ${control?.live_order_placement_enabled ? "warn" : "good"}`;
  strip.innerHTML = `
    <div class="state-pill"><span class="eyebrow">Positions</span><strong>${exceptions.positions.length}</strong></div>
    <div class="state-pill"><span class="eyebrow">Resting Orders</span><strong>${openOrderCount}</strong></div>
    <div class="state-pill"><span class="eyebrow">Recent Fills</span><strong>${exceptions.recent_fills.length}</strong></div>
    <div class="state-pill"><span class="eyebrow">Trade Exceptions</span><strong class="${tradeExceptionCount ? "bad" : "good"}">${tradeExceptionCount}</strong></div>
    <div class="state-pill"><span class="eyebrow">Live Intents</span><strong class="${liveIntentCount ? "warn" : "good"}">${liveIntentCount}</strong></div>
  `;

  feedback.textContent = control
    ? `Manual control last changed ${dateTime(control.updated_at)}${control.note ? ` · ${control.note}` : ""}`
    : "Operator actions will report here.";

  const sections = [];
  if (exceptions.trade_exceptions.length) {
    sections.push(`
      <section class="lane-card">
        <header><div><p class="eyebrow">Trade exceptions</p><strong>Open live trades needing attention</strong></div><div class="chip bad">${exceptions.trade_exceptions.length}</div></header>
        <div class="issue-list">
          ${exceptions.trade_exceptions
            .map(
              (item) => `
                <div class="issue-item">
                  <strong>#${item.trade_id} · ${item.market_ticker}</strong>
                  <span>${item.issue}</span>
                  <span class="muted">Position: ${item.has_position ? "yes" : "no"} · Resting order: ${item.has_resting_order ? "yes" : "no"} · Matched exit fill: ${item.matched_exit_fill_quantity.toFixed(2)}</span>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
    `);
  }

  if (exceptions.orders.length) {
    sections.push(`
      <section class="lane-card">
        <header><div><p class="eyebrow">Resting orders</p><strong>Exchange-side open or pending orders</strong></div></header>
        <div class="stack-list">
          ${exceptions.orders
            .map(
              (item) => `
                <div class="stack-row">
                  <div>
                    <strong>${item.market_ticker || item.order_id}</strong>
                    <p class="muted">${titleCase(item.action)} · ${titleCase(item.side)} · ${item.order_id}</p>
                  </div>
                  <div class="stack-meta">
                    <span class="chip ${statusTone(item.status)}">${titleCase(item.status || "unknown")}</span>
                    <span class="mono">${item.fill_count.toFixed(0)}/${item.count.toFixed(0)}</span>
                  </div>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
    `);
  }

  if (exceptions.live_intents.length) {
    sections.push(`
      <section class="lane-card">
        <header><div><p class="eyebrow">Live intents</p><strong>Intent state machine</strong></div></header>
        <div class="stack-list">
          ${exceptions.live_intents
            .map(
              (item) => `
                <div class="stack-row">
                  <div>
                    <strong>${item.lane_key}</strong>
                    <p class="muted">${item.market_ticker || "No ticker"} · ${item.client_order_id || "No client id"}</p>
                    ${item.last_error ? `<p class="bad">${item.last_error}</p>` : ""}
                  </div>
                  <div class="stack-meta">
                    <span class="chip ${statusTone(item.status)}">${titleCase(item.status)}</span>
                    <span class="muted">${dateTime(item.last_transition_at)}</span>
                  </div>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
    `);
  }

  if (exceptions.positions.length) {
    sections.push(`
      <section class="lane-card">
        <header><div><p class="eyebrow">Positions</p><strong>Live position snapshot</strong></div></header>
        <div class="stack-list">
          ${exceptions.positions
            .map(
              (item) => `
                <div class="stack-row">
                  <div>
                    <strong>${item.market_ticker}</strong>
                    <p class="muted">Position ${item.position_count.toFixed(2)} · Resting ${item.resting_order_count}</p>
                  </div>
                  <div class="stack-meta">
                    <span class="mono">${money(item.market_exposure)}</span>
                    <span class="${item.realized_pnl >= 0 ? "good" : "bad"}">${money(item.realized_pnl)}</span>
                  </div>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
    `);
  }

  if (exceptions.recent_fills.length) {
    sections.push(`
      <section class="lane-card">
        <header><div><p class="eyebrow">Recent fills</p><strong>Latest exchange fills</strong></div></header>
        <div class="stack-list">
          ${exceptions.recent_fills
            .map(
              (item) => `
                <div class="stack-row">
                  <div>
                    <strong>${item.market_ticker || item.fill_id}</strong>
                    <p class="muted">${titleCase(item.action)} · ${titleCase(item.side)} · ${dateTime(item.created_time)}</p>
                  </div>
                  <div class="stack-meta">
                    <span class="mono">${item.count.toFixed(0)}</span>
                    <span class="bad">${money(item.fee_paid)}</span>
                  </div>
                </div>
              `
            )
            .join("")}
        </div>
      </section>
    `);
  }

  root.innerHTML = sections.length
    ? sections.join("")
    : `<div class="empty">${hasAttention ? "Live exceptions exist but could not be rendered." : "No live exceptions right now. Orders, positions, fills, and local live intent states will appear here when live flow becomes active."}</div>`;
}

function renderTrades(items) {
  const root = document.getElementById("trade-list");
  if (!items.length) {
    root.innerHTML = `<div class="empty">No exposure right now. Open live or paper inventory will stay visible here without paging.</div>`;
    return;
  }
  root.innerHTML = items.map((trade) => `
    <article class="trade-card">
      <header>
        <div>
          <p class="eyebrow">${titleCase(trade.mode)} · ${titleCase(trade.strategy_family)}</p>
          <div class="trade-heading-row">
            <p class="eyebrow">${titleCase(trade.mode)} · ${tradeSubhead(trade)}</p>
            <span class="trade-direction ${tradeDirectionTone(trade)}">${tradeDirectionLabel(trade)}</span>
          </div>
          <strong class="trade-headline">${tradeHeadline(trade)}</strong>
          <p class="trade-subtitle">${trade.market_ticker || "Kalshi market link ready"}</p>
        </div>
        <div class="chip">${titleCase(trade.status)}</div>
      </header>
      <dl class="trade-grid">
        <div class="mini"><dt>Mode</dt><dd>${titleCase(trade.mode)}</dd></div>
        <div class="mini"><dt>Quantity</dt><dd>${trade.quantity}</dd></div>
        <div class="mini"><dt>Entry</dt><dd>${money(trade.entry_price)}</dd></div>
        <div class="mini"><dt>Age</dt><dd>${relativeAge(trade.created_at)}</dd></div>
        <div class="mini"><dt>Placed</dt><dd>${dateTime(trade.created_at)}</dd></div>
      </dl>
      <div class="trade-actions">
        <a class="action-link" href="${kalshiTradeUrl(trade)}" target="_blank" rel="noopener noreferrer">${trade.market_ticker ? "Open on Kalshi" : "Open Kalshi series"}</a>
      </div>
      <details>
        <summary>More detail</summary>
        ${trade.market_title ? `<p class="muted">${cleanMarketTitle(trade.market_title)}</p>` : ""}
        ${trade.market_ticker ? `<p class="muted">Ticker ${trade.market_ticker}</p>` : ""}
        <p class="muted">${tradeHeadline(trade)} on ${tradeSubhead(trade)}</p>
        <p class="muted">Lane ${trade.lane_key}</p>
        <p class="mono">Trade ID ${trade.trade_id}</p>
      </details>
    </article>
  `).join("");
}

function renderOpportunities(items) {
  const root = document.getElementById("opportunity-list");
  if (!items.length) {
    const family = currentFamily();
    const message = family === "crypto"
      ? "No fresh crypto decisions right now. If Kalshi does not have a near-expiry 15m contract in range, this section will stay empty instead of showing stale decisions."
      : "No fresh weather decisions right now. Once ingest, features, and decisioning update again, the latest weather flow will appear here.";
    root.innerHTML = `<div class="empty">${message}</div>`;
    return;
  }
  root.innerHTML = items.map((item) => `
    <article class="trade-card">
      <header>
        <div>
          <p class="eyebrow">${titleCase(item.strategy_family)} · ${titleCase(item.side)}</p>
          <strong>${item.lane_key}</strong>
        </div>
        <div class="chip ${opportunityTone(item)}">${opportunityLabel(item)}</div>
      </header>
      <dl class="trade-grid">
        <div class="mini"><dt>Edge</dt><dd class="${item.edge >= 0 ? "good" : "bad"}">${(item.edge * 100).toFixed(1)}%</dd></div>
        <div class="mini"><dt>Confidence</dt><dd>${(item.confidence * 100).toFixed(1)}%</dd></div>
      </dl>
      <details>
        <summary>Reasons</summary>
        <p class="muted">${item.reasons?.length ? item.reasons.join(", ") : "No blocking reasons."}</p>
        ${opportunityExecutionSummary(item) ? `<p class="muted">${opportunityExecutionSummary(item)}</p>` : ""}
        <p class="muted">As of ${dateTime(item.as_of)}</p>
      </details>
    </article>
  `).join("");
}

function bindOperatorControls(refresh) {
  document.querySelectorAll("[data-action]").forEach((button) => {
    button.onclick = async () => {
      const feedback = document.getElementById("control-feedback");
      const action = button.dataset.action;
      const originalText = button.textContent;
      button.disabled = true;
      button.textContent = "Working...";
      feedback.textContent = `Running ${titleCase(action)}...`;
      try {
        const result = await postOperatorAction(action);
        if (!result.ok) {
          throw new Error(result.error || "operator action failed");
        }
        feedback.textContent = `${titleCase(action)} completed at ${new Date().toLocaleTimeString()}.`;
        await refresh();
      } catch (error) {
        feedback.textContent = `${titleCase(action)} failed: ${error.message}`;
        feedback.className = "control-feedback bad";
      } finally {
        button.disabled = false;
        button.textContent = originalText;
      }
    };
  });
}

async function refreshScreen() {
  if (refreshPromise) {
    return refreshPromise;
  }

  refreshPromise = (async () => {
    const [payload, runtime] = await Promise.all([loadDashboard(), loadRuntime()]);
    const family = currentFamily();
    document.body.dataset.family = family;
    document.getElementById("hero-title").textContent = family === "weather"
      ? "Weather paper cockpit"
      : "Crypto live cockpit";
    document.getElementById("hero-copy").textContent = family === "weather"
      ? "Research-first. Paper evidence before confidence."
      : "Live risk first. Lane truth before optimism.";
    document.getElementById("family-link-crypto").classList.toggle("active", family === "crypto");
    document.getElementById("family-link-weather").classList.toggle("active", family === "weather");
    const readiness = payload.readiness || {
      overall_status: "shadow_only",
      shadow_count: 0,
      paper_active_count: 0,
      live_micro_count: 0,
      live_scaled_count: 0,
      quarantined_count: 0,
      lanes: [],
    };
    renderDecisionBar(payload, runtime, readiness);
    renderBankrolls(payload.bankrolls || []);
    renderReadiness(readiness);
    renderLiveSync(payload.live_sync || null);
    renderOperatorState(runtime, readiness);
    renderExecutionQuality(payload.execution_quality || runtime?.execution_quality || null);
    renderMoneyView(payload);
    renderLiveExceptions(payload);
    renderTrades(payload.open_trades || []);
    renderOpportunities(payload.opportunities || []);
  })();

  try {
    await refreshPromise;
  } finally {
    refreshPromise = null;
  }
}

refreshScreen()
  .then(() => bindOperatorControls(refreshScreen))
  .catch((error) => {
    document.getElementById("hero-status").textContent = "Unavailable";
    document.getElementById("decision-grid").innerHTML = "";
    document.getElementById("bankroll-list").innerHTML = `<div class="empty">Dashboard load failed: ${error.message}</div>`;
    document.getElementById("money-view-family").innerHTML = "";
    document.getElementById("money-view-list").innerHTML = "";
    document.getElementById("lane-list").innerHTML = "";
    document.getElementById("live-sync-list").innerHTML = "";
    document.getElementById("live-sync-strip").innerHTML = "";
    document.getElementById("ops-list").innerHTML = "";
    document.getElementById("ops-strip").innerHTML = "";
    document.getElementById("exceptions-list").innerHTML = "";
    document.getElementById("exceptions-strip").innerHTML = "";
    document.getElementById("trade-list").innerHTML = "";
    document.getElementById("opportunity-list").innerHTML = "";
  });

window.setInterval(() => {
  refreshScreen().catch((error) => {
    console.error("auto refresh failed", error);
  });
}, AUTO_REFRESH_MS);
