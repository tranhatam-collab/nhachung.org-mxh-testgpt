// apps/worker/src/trust.ts
export type TrustKind = "base" | "rs" | "cs" | "bonus" | "risk";
export type Actor = "system" | "admin" | `member:${string}`;

export type TrustEventRow = {
  id: string;
  member_id: string;
  project_id?: string | null;
  event_type: string;
  delta: number;
  ref_id?: string | null;
  note?: string | null;
  created_at: number;     // unix ms
  created_by: string;     // system|admin|member:<id>
};

export type TrustSummary = {
  member_id: string;
  score: number;                 // TS
  level: number;                 // 1..4
  base: number;
  rs: number;
  cs: number;
  is: number;                    // integrity (0..150)
  completed_commitments: number;
  failed_commitments: number;
  late_commitments_90d: number;
  total_contrib_hours_90d: number;
  total_contrib_capital_million_90d: number;
  total_contrib_asset_points_180d: number;
  total_contrib_knowledge_points_90d: number;
  disputes_open: number;
  last_positive_at?: number | null;
  last_event_at?: number | null;
  risk_flags: string[];          // ["R2_delay_pattern", ...]
  updated_at: number;
};

type Rules = any;

const clamp = (min: number, max: number, v: number) => Math.max(min, Math.min(max, v));
const isPositive = (delta: number) => delta > 0;

function startOfMonthUTC(ms: number): number {
  const d = new Date(ms);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), 1, 0, 0, 0, 0);
}

function daysBetween(aMs: number, bMs: number): number {
  return Math.floor(Math.abs(bMs - aMs) / (24 * 60 * 60 * 1000));
}

function ln(x: number): number {
  // Math.log is natural log
  return Math.log(x);
}

/**
 * Capital points formula:
 * round(2 * ln(1 + amount_million_vnd))
 */
function capitalPoints(amountMillion: number): number {
  const raw = 2 * ln(1 + Math.max(0, amountMillion));
  return Math.round(raw);
}

/**
 * Commitment done on time with difficulty:
 * base 15 * (1 + 0.1*(difficulty-1)), difficulty 1..5
 */
function commitmentOnTimePoints(base: number, difficulty: number, stepBonus: number): number {
  const d = clamp(1, 5, difficulty);
  const mult = 1 + stepBonus * (d - 1);
  return Math.round(base * mult);
}

function commitmentLatePoints(tiers: Array<[number, number, number]>, lateDays: number): number {
  const d = Math.max(0, lateDays);
  for (const [minD, maxD, pts] of tiers) {
    if (d >= minD && d <= maxD) return pts;
  }
  return 1;
}

/**
 * Apply caps for month by components and overall.
 * We compute monthly totals of positive deltas by component bucket.
 */
type MonthlyCapsState = {
  monthStart: number;
  totals: Record<string, number>;
  totalAll: number;
};

function initCapsState(monthStart: number): MonthlyCapsState {
  return { monthStart, totals: Object.create(null), totalAll: 0 };
}

function addCapped(
  state: MonthlyCapsState,
  component: string,
  delta: number,
  rules: Rules
): number {
  if (delta <= 0) return delta; // negatives are not capped; they apply fully

  const capTotal = rules.caps?.monthly?.total ?? 140;
  const capByComponent = rules.caps?.components?.[component];
  const capByMajor =
    component === "base" ? rules.base?.max
    : component === "rs" ? rules.caps?.monthly?.rs
    : component === "cs" ? rules.caps?.monthly?.cs
    : component === "bonus" ? rules.caps?.monthly?.bonus
    : undefined;

  // calculate remaining caps
  const currentComponent = state.totals[component] ?? 0;
  let remainingComponent = typeof capByComponent === "number" ? (capByComponent - currentComponent) : Infinity;

  // major caps (cs/rs/bonus) track them by synthetic bucket
  let majorBucket = component;
  if (component === "labor" || component === "capital" || component === "asset" || component === "knowledge" || component === "milestone") majorBucket = "cs";
  if (component === "peer" || component === "invited") majorBucket = "bonus";

  const currentMajor = state.totals[majorBucket] ?? 0;
  let remainingMajor = typeof capByMajor === "number" ? (capByMajor - currentMajor) : Infinity;

  let remainingTotal = capTotal - state.totalAll;

  const allowed = Math.max(0, Math.min(delta, remainingComponent, remainingMajor, remainingTotal));
  // update state with allowed
  state.totals[component] = currentComponent + allowed;
  state.totals[majorBucket] = currentMajor + allowed;
  state.totalAll += allowed;
  return allowed;
}

/**
 * Decay: if no positive events for >= threshold days,
 * apply monthly_percent to TS, capped by monthly_max_points.
 */
function applyDecay(ts: number, lastPositiveAt: number | null | undefined, now: number, rules: Rules): number {
  const decay = rules.decay;
  if (!decay?.enabled) return ts;

  const thresholdDays = decay.no_positive_event_days_threshold ?? 30;
  if (!lastPositiveAt) return ts; // if unknown, skip decay to avoid unfairness

  const gapDays = daysBetween(lastPositiveAt, now);
  if (gapDays < thresholdDays) return ts;

  const percent = (decay.monthly_percent ?? 2) / 100;
  const maxPoints = decay.monthly_max_points ?? 10;

  const computed = Math.round(ts * percent);
  const drop = clamp(0, maxPoints, computed);
  return Math.max(0, ts - drop);
}

/**
 * Compute RS from commitment stats (last 180d, 90d late).
 * For MVP, we compute RS approximately from events:
 * - commitment_done_on_time => quality 1.0
 * - commitment_done_late => quality depends on lateDays tiers (stored in note JSON ideally)
 * - commitment_failed/abandoned/harm => quality 0 and penalties handled in risk
 *
 * If you later store commitment metadata (due_at, verified_at), RS can be exact.
 */
function computeRSFromEvents(events180d: TrustEventRow[], rules: Rules): { rs: number; completed: number; failed: number; late90d: number } {
  let completed = 0;
  let failed = 0;
  let qualitySum = 0;

  let late90d = 0;
  const now = Date.now();
  const win90 = now - 90 * 24 * 60 * 60 * 1000;

  // streak approximation: count consecutive on_time events in recent order
  let streak = 0;
  let bestStreak = 0;

  // sort ascending by created_at
  const sorted = events180d.slice().sort((a, b) => a.created_at - b.created_at);

  for (const e of sorted) {
    if (e.event_type === "commitment_done_on_time") {
      completed++;
      qualitySum += 1.0;
      streak++;
      bestStreak = Math.max(bestStreak, streak);
    } else if (e.event_type === "commitment_done_late") {
      completed++;
      // default quality (we can improve if note includes lateDays)
      let q = 0.6;
      if (e.note) {
        try {
          const obj = JSON.parse(e.note);
          const lateDays = Number(obj.lateDays ?? 0);
          const tiers = rules.events?.commitment_done_late?.params?.late_days_tiers ?? [[1,3,0.6],[4,7,0.3],[8,9999,0.05]];
          // tiers format in rules.json is points; for quality we map:
          if (lateDays >= 1 && lateDays <= 3) q = 0.6;
          else if (lateDays >= 4 && lateDays <= 7) q = 0.3;
          else if (lateDays >= 8) q = 0.05;
          // count late90d
          if (e.created_at >= win90) late90d++;
        } catch {}
      } else {
        if (e.created_at >= win90) late90d++;
      }
      qualitySum += q;
      streak = 0;
    } else if (e.event_type === "commitment_failed" || e.event_type === "commitment_abandoned" || e.event_type === "commitment_harm") {
      failed++;
      streak = 0;
    }
  }

  const n = Math.max(1, completed + failed);
  const CQ = 100 * (qualitySum / n);

  // streak bonus ladder
  let SB = 0;
  if (bestStreak >= 20) SB = 60;
  else if (bestStreak >= 12) SB = 30;
  else if (bestStreak >= 7) SB = 15;
  else if (bestStreak >= 3) SB = 5;

  // delay penalty from late count (approx): 2 points per late in 90d
  const DP = clamp(0, 60, late90d * 2);

  const rs = clamp(0, 300, Math.round(CQ + SB - DP));
  return { rs, completed, failed, late90d };
}

function computeCSFromEvents(events: TrustEventRow[], rules: Rules): {
  cs: number;
  hours90: number;
  capital90_million: number;
  asset180: number;
  knowledge90: number;
  capitalPoints90: number;
  diversityBonus: number;
} {
  const now = Date.now();
  const win90 = now - 90 * 24 * 60 * 60 * 1000;
  const win180 = now - 180 * 24 * 60 * 60 * 1000;

  let hours90 = 0;
  let capital90 = 0;
  let asset180 = 0;
  let knowledge90 = 0;

  let hasLabor = false;
  let hasCapital = false;
  let hasAsset = false;
  let hasKnowledge = false;

  for (const e of events) {
    if (e.event_type === "contrib_labor_verified" && e.created_at >= win90) {
      hasLabor = true;
      // delta is per hour already; note may include hours or role multiplier; we trust delta
      hours90 += Math.max(0, e.delta);
    }
    if (e.event_type === "contrib_capital_verified" && e.created_at >= win90) {
      hasCapital = true;
      // amount million should be in note JSON: {amountMillion: 100}
      if (e.note) {
        try { capital90 += Math.max(0, Number(JSON.parse(e.note).amountMillion ?? 0)); } catch {}
      }
    }
    if (e.event_type === "contrib_asset_verified" && e.created_at >= win180) {
      hasAsset = true;
      asset180 += Math.max(0, e.delta);
    }
    if (e.event_type === "contrib_knowledge_verified" && e.created_at >= win90) {
      hasKnowledge = true;
      knowledge90 += Math.max(0, e.delta);
    }
  }

  // caps by component
  const LP = clamp(0, 120, hours90); // already contains role multiplier effect
  const capPts = capitalPoints(capital90);
  const CP = clamp(0, 120, capPts);

  const AP = clamp(0, 80, asset180);
  const KP = clamp(0, 60, knowledge90);

  const typesCount =
    (hasLabor ? 1 : 0) + (hasCapital ? 1 : 0) + (hasAsset ? 1 : 0) + (hasKnowledge ? 1 : 0);
  let DB = 0;
  if (typesCount >= 3) DB = 20;
  else if (typesCount >= 2) DB = 10;

  const cs = clamp(0, 300, Math.round(LP + CP + AP + KP + DB));
  return { cs, hours90, capital90_million: capital90, asset180, knowledge90, capitalPoints90: CP, diversityBonus: DB };
}

function computeISFromRiskEvents(events365d: TrustEventRow[], rules: Rules): { is: number; disputesOpen: number; majorDisputeLastAt?: number | null } {
  const max = rules.integrity?.max ?? 150;
  let is = rules.integrity?.start ?? 150;

  let disputesOpen = 0;
  let majorDisputeLastAt: number | null = null;

  for (const e of events365d) {
    if (e.event_type === "dispute_opened_against") disputesOpen++;
    if (e.event_type === "dispute_resolved_major") majorDisputeLastAt = Math.max(majorDisputeLastAt ?? 0, e.created_at);

    // negative deltas affect integrity
    if (e.event_type.startsWith("dispute_") || e.event_type === "toxic_confirmed" || e.event_type === "fraud_confirmed" || e.event_type === "score_gaming") {
      is += e.delta; // delta is negative
    }
  }

  is = clamp(0, max, is);
  return { is, disputesOpen, majorDisputeLastAt };
}

function computeLevel(summary: TrustSummary, rules: Rules): number {
  // evaluate highest level that satisfies both score and requirements
  const levels = rules.levels;
  const candidates = [4, 3, 2, 1];
  for (const lvl of candidates) {
    const conf = levels[String(lvl)];
    if (!conf) continue;

    if (summary.score < (conf.min_ts ?? 0)) continue;

    const req = conf.requirements ?? {};
    if (summary.rs < (req.min_rs ?? 0)) continue;
    if (summary.completed_commitments < (req.min_completed_commitments ?? 0)) continue;

    const total = Math.max(1, summary.completed_commitments + summary.failed_commitments);
    const failureRate = summary.failed_commitments / total;
    if (failureRate > (req.max_failure_rate ?? 1.0)) continue;

    if (req.no_open_disputes === true && summary.disputes_open > 0) continue;

    // major dispute constraint is checked via flags or stored separately; we keep it minimal here
    // In practice, store majorDisputeLastAt in trust_summary or compute from events and compare days.
    // If not available, don't block upgrade (MVP) or block conservatively (your choice).
    return lvl;
  }
  return 1;
}

function computeFlags(summary: TrustSummary, events: TrustEventRow[], rules: Rules): string[] {
  const flags: string[] = [];
  const now = Date.now();
  const win90 = now - 90 * 24 * 60 * 60 * 1000;
  const win60 = now - 60 * 24 * 60 * 60 * 1000;

  // R2 delay pattern: late_commitments_90d >= threshold
  const r2 = rules.flags?.R2_delay_pattern;
  if (r2 && summary.late_commitments_90d >= (r2.late_count_threshold ?? 3)) flags.push("R2_delay_pattern");

  // R3 dispute exposure
  const r3 = rules.flags?.R3_dispute_exposure;
  if (r3 && summary.disputes_open >= (r3.open_disputes_threshold ?? 1)) flags.push("R3_dispute_exposure");

  // R5 capital dominance: capital share of CS > threshold (approx)
  const r5 = rules.flags?.R5_capital_dominance;
  if (r5) {
    const capShare = summary.cs > 0 ? (summary.total_contrib_capital_million_90d > 0 ? (summary.cs > 0 ? (summary.total_contrib_capital_million_90d / (summary.total_contrib_capital_million_90d + summary.total_contrib_hours_90d + 1)) : 0) : 0) : 0;
    // This is a rough proxy; better compute by points share when you store component points in summary.
    if (capShare >= (r5.capital_share_threshold ?? 0.7)) flags.push("R5_capital_dominance");
  }

  // R4 ring behavior (rough): count peer_helpful between same pairs in 60d (needs pair tracking)
  const r4 = rules.flags?.R4_ring_behavior;
  if (r4) {
    let peer60 = 0;
    for (const e of events) {
      if (e.event_type === "peer_helpful" && e.created_at >= win60) peer60++;
    }
    if (peer60 >= (r4.pair_events_threshold ?? 3) * 2) flags.push("R4_ring_behavior"); // rough
  }

  // R1 unverified heavy: needs self-claim data; MVP skip unless you store self-claim events
  return flags;
}

/**
 * Recompute summary from events (authoritative).
 * You call this in:
 * - POST /trust/recalc?member_id=
 * - after inserting a trust_event (fast-path: update incremental later)
 */
export function recomputeTrustSummary(
  memberId: string,
  eventsAll: TrustEventRow[],
  rules: Rules,
  now: number = Date.now()
): TrustSummary {
  const win180 = now - 180 * 24 * 60 * 60 * 1000;
  const win365 = now - 365 * 24 * 60 * 60 * 1000;

  const events180 = eventsAll.filter(e => e.created_at >= win180);
  const events365 = eventsAll.filter(e => e.created_at >= win365);

  // Base = sum base events but capped
  let base = 0;
  for (const e of eventsAll) {
    const cfg = rules.events?.[e.event_type];
    if (cfg?.kind === "base") base += Math.max(0, cfg.delta ?? e.delta ?? 0);
  }
  base = clamp(0, rules.base?.max ?? 25, base);

  const rsRes = computeRSFromEvents(events180, rules);
  const csRes = computeCSFromEvents(eventsAll, rules);
  const isRes = computeISFromRiskEvents(events365, rules);

  let ts = Math.round(
    base +
    (rules.weights?.rs ?? 1.0) * rsRes.rs +
    (rules.weights?.cs ?? 0.8) * csRes.cs +
    (rules.weights?.is ?? 1.2) * isRes.is
  );

  // last positive event time
  let lastPositiveAt: number | null = null;
  let lastEventAt: number | null = null;
  for (const e of eventsAll) {
    if (!lastEventAt || e.created_at > lastEventAt) lastEventAt = e.created_at;
    if (e.delta > 0) {
      if (!lastPositiveAt || e.created_at > lastPositiveAt) lastPositiveAt = e.created_at;
    }
  }

  ts = applyDecay(ts, lastPositiveAt, now, rules);

  const summary: TrustSummary = {
    member_id: memberId,
    score: Math.max(0, ts),
    level: 1,
    base,
    rs: rsRes.rs,
    cs: csRes.cs,
    is: isRes.is,
    completed_commitments: rsRes.completed,
    failed_commitments: rsRes.failed,
    late_commitments_90d: rsRes.late90d,
    total_contrib_hours_90d: csRes.hours90,
    total_contrib_capital_million_90d: csRes.capital90_million,
    total_contrib_asset_points_180d: csRes.asset180,
    total_contrib_knowledge_points_90d: csRes.knowledge90,
    disputes_open: isRes.disputesOpen,
    last_positive_at: lastPositiveAt,
    last_event_at: lastEventAt,
    risk_flags: [],
    updated_at: now
  };

  summary.risk_flags = computeFlags(summary, eventsAll, rules);
  summary.level = computeLevel(summary, rules);
  return summary;
}

/**
 * Create a trust delta for an incoming event request.
 * This converts params (difficulty, lateDays, capital amount) into delta.
 * You still must enforce verification rules at API layer.
 */
export function computeDeltaForEvent(
  eventType: string,
  params: Record<string, any>,
  rules: Rules
): { delta: number; note?: string } {
  const cfg = rules.events?.[eventType];
  if (!cfg) return { delta: 0 };

  if (eventType === "contrib_capital_verified") {
    const amountMillion = Number(params.amountMillion ?? 0);
    const pts = capitalPoints(amountMillion);
    return { delta: pts, note: JSON.stringify({ amountMillion }) };
  }

  if (eventType === "contrib_labor_verified") {
    const hours = Number(params.hours ?? 0);
    const role = String(params.role ?? "member");
    const mult = Number(cfg.params?.role_multipliers?.[role] ?? 1.0);
    const perHour = Number(cfg.params?.per_hour ?? 1);
    const pts = Math.round(Math.max(0, hours) * perHour * mult);
    return { delta: pts, note: JSON.stringify({ hours, role, mult }) };
  }

  if (eventType === "commitment_done_on_time") {
    const difficulty = Number(params.difficulty ?? 1);
    const base = Number(cfg.delta ?? 15);
    const step = Number(cfg.params?.difficulty_step_bonus ?? 0.1);
    const pts = commitmentOnTimePoints(base, difficulty, step);
    return { delta: pts, note: JSON.stringify({ difficulty }) };
  }

  if (eventType === "commitment_done_late") {
    const lateDays = Number(params.lateDays ?? 1);
    const tiers = cfg.params?.late_days_tiers ?? [[1, 3, 8], [4, 7, 4], [8, 9999, 1]];
    const pts = commitmentLatePoints(tiers, lateDays);
    return { delta: pts, note: JSON.stringify({ lateDays }) };
  }

  if (eventType === "contrib_knowledge_verified" || eventType === "contrib_asset_verified") {
    const v = Number(params.value ?? cfg.delta ?? 0);
    const min = Number(cfg.params?.min ?? 0);
    const max = Number(cfg.params?.max ?? 9999);
    const pts = clamp(min, max, v);
    return { delta: pts, note: params.note ? String(params.note) : undefined };
  }

  // default: fixed delta
  return { delta: Number(cfg.delta ?? 0), note: params.note ? String(params.note) : undefined };
}
