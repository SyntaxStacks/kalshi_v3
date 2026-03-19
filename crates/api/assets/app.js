async function loadDashboard() {
  const response = await fetch("/v1/dashboard");
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

function titleCase(value) {
  return String(value || "")
    .replaceAll("_", " ")
    .replace(/\b\w/g, (char) => char.toUpperCase());
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

function renderBankrolls(items) {
  const root = document.getElementById("bankroll-list");
  if (!items.length) {
    root.innerHTML = `<div class="empty">No bankroll snapshots yet. Once execution writes bankroll data, it will appear here.</div>`;
    return;
  }

  const grouped = items.reduce((acc, item) => {
    const key = item.mode;
    if (!acc[key]) {
      acc[key] = {
        mode: item.mode,
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
          <p class="eyebrow">${titleCase(item.mode)} bankroll</p>
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
    root.innerHTML = `<div class="empty">No open trades right now. When risk is live, open inventory will stay visible here without paging.</div>`;
    return;
  }
  root.innerHTML = items.map((trade) => `
    <article class="trade-card">
      <header>
        <div>
          <p class="eyebrow">${titleCase(trade.mode)} · ${titleCase(trade.strategy_family)}</p>
          <strong>${trade.lane_key}</strong>
        </div>
        <div class="chip">${titleCase(trade.status)}</div>
      </header>
      <dl class="trade-grid">
        <div class="mini"><dt>Quantity</dt><dd>${trade.quantity}</dd></div>
        <div class="mini"><dt>Entry</dt><dd>${money(trade.entry_price)}</dd></div>
      </dl>
      <details>
        <summary>More detail</summary>
        <p class="muted">Opened ${dateTime(trade.created_at)}</p>
        <p class="mono">Trade ID ${trade.trade_id}</p>
      </details>
    </article>
  `).join("");
}

function renderOpportunities(items) {
  const root = document.getElementById("opportunity-list");
  if (!items.length) {
    root.innerHTML = `<div class="empty">No opportunities yet. Once ingest, feature, and decision workers are flowing, the latest decisions will appear here.</div>`;
    return;
  }
  root.innerHTML = items.map((item) => `
    <article class="trade-card">
      <header>
        <div>
          <p class="eyebrow">${titleCase(item.strategy_family)} · ${titleCase(item.side)}</p>
          <strong>${item.lane_key}</strong>
        </div>
        <div class="chip ${item.approved ? "good" : "warn"}">${item.approved ? "Approved" : "Blocked"}</div>
      </header>
      <dl class="trade-grid">
        <div class="mini"><dt>Edge</dt><dd class="${item.edge >= 0 ? "good" : "bad"}">${(item.edge * 100).toFixed(1)}%</dd></div>
        <div class="mini"><dt>Confidence</dt><dd>${(item.confidence * 100).toFixed(1)}%</dd></div>
      </dl>
      <details>
        <summary>Reasons</summary>
        <p class="muted">${item.reasons?.length ? item.reasons.join(", ") : "No blocking reasons."}</p>
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
  const [payload, runtime] = await Promise.all([loadDashboard(), loadRuntime()]);
  const readiness = payload.readiness || {
    overall_status: "shadow_only",
    shadow_count: 0,
    paper_active_count: 0,
    live_micro_count: 0,
    live_scaled_count: 0,
    quarantined_count: 0,
    lanes: [],
  };
  renderBankrolls(payload.bankrolls || []);
  renderReadiness(readiness);
  renderLiveSync(payload.live_sync || null);
  renderOperatorState(runtime, readiness);
  renderLiveExceptions(payload);
  renderTrades(payload.open_trades || []);
  renderOpportunities(payload.opportunities || []);
}

refreshScreen()
  .then(() => bindOperatorControls(refreshScreen))
  .catch((error) => {
    document.getElementById("hero-status").textContent = "Unavailable";
    document.getElementById("bankroll-list").innerHTML = `<div class="empty">Dashboard load failed: ${error.message}</div>`;
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
