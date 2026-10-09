/**
 * ============================================================
 * CONDENSE PIPELINE - AEROSPACE MANUFACTURING
 * APP 1: Aerospace Shop-Floor Simulator
 * ============================================================
 * Role     : Custom Input Connector on Condense
 * Function : Simulates the machines a tier-1 aerostructures / engine-component
 *            shop runs: 5-axis CNC machining centres (Ti / Inconel / Al),
 *            CMM inspection cells, and composite-cure autoclaves.
 * Output   : Kafka topic → aerospace.raw.telemetry
 *
 * DESIGN NOTE — raw data is deliberately "machine-shaped":
 *   Machines know their own ID, the NC program loaded and a work-order number.
 *   They do NOT know plant / site, aircraft programme, customer, part number,
 *   AS9100 certification scope or calibration status. That context is added
 *   downstream by the prebuilt Enrich Utility, joining on:
 *       asset_id      → machine master data        (Enrich A ← aerospace-machine-master-cdc)
 *       process_key   → part / programme / recipe master  (Enrich B ← aerospace-process-master-cdc)
 *
 * REAL-WORLD ALTERNATIVE:
 *   CNC controllers expose this over MTConnect / OPC-UA (Siemens 840D, Fanuc
 *   FOCAS, Heidenhain). Swap this connector for the prebuilt MQTT / OPC-UA
 *   input — field names below follow MTConnect-style semantics.
 * ============================================================
 */

const { Kafka } = require('kafkajs');
const express   = require('express');
require('dotenv').config();

const KAFKA_BROKERS    = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const KAFKA_TOPIC_OUT  = process.env.KAFKA_TOPIC_OUT || 'aerospace.raw.telemetry';
const ASSET_TYPES      = (process.env.ASSET_TYPES || 'aero_cnc,aero_cmm,aero_autoclave').split(',').map(s => s.trim());
const COUNTS           = {
  aero_cnc      : parseInt(process.env.CNC_COUNT       || '6'),
  aero_cmm      : parseInt(process.env.CMM_COUNT       || '2'),
  aero_autoclave: parseInt(process.env.AUTOCLAVE_COUNT || '3'),
};
const EMIT_INTERVAL_MS = parseInt(process.env.EMIT_INTERVAL_MS || '3000');
const SIM_MIN_PER_TICK = parseFloat(process.env.SIM_MIN_PER_TICK || '2');
const PORT             = parseInt(process.env.PORT || '3000');
const SIMULATE_ANOMALY = process.env.SIMULATE_ANOMALY !== 'false';

const kafka    = new Kafka({ clientId: 'aerospace-simulator', brokers: KAFKA_BROKERS, retry: { retries: 5 } });
const producer = kafka.producer();

function rand(min, max, dec = 2) { return parseFloat((Math.random() * (max - min) + min).toFixed(dec)); }
function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function gauss(sigma) { // Box-Muller
  const u = 1 - Math.random(), v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v) * sigma;
}
function pad(n, w) { return String(n).padStart(w, '0'); }

const machineState = {};
function getState(id, def) {
  if (!machineState[id]) machineState[id] = typeof def === 'function' ? def() : { ...def };
  return machineState[id];
}

// ── Material cutting characteristics ──────────────────────────────────────────
// sce = specific cutting energy (J/mm³). Titanium & Inconel are why aerospace
// machining is hard: high sce, low thermal conductivity, rapid tool wear.
const MATERIALS = {
  aluminium: { sce: 0.8, rpm: [10000, 18000], vf: [3000, 6000], ap: [5, 12], ae: [15, 30], vib: 0.8, wear: 0.12 },
  titanium : { sce: 4.5, rpm: [1500, 4500],   vf: [300, 700],   ap: [4, 10], ae: [10, 20], vib: 1.6, wear: 0.40 },
  inconel  : { sce: 6.5, rpm: [800, 3000],    vf: [150, 450],   ap: [2, 6],  ae: [6, 14],  vib: 2.0, wear: 0.65 },
};

// NC program → what is being cut. This is what the machine itself knows.
// (Part number, aircraft programme, customer … are NOT here — see Enrich Utility.)
// process_key = NC program (CNC/CMM) or cure recipe (autoclave): one uniform join key for the process master.
const PROGRAMS = {
  PROG_WR_4471 : { material: 'Al 7075-T6',   family: 'aluminium', stock_kg: 85,  part_kg: 9.5,  tol_um: 50 }, // wing rib
  PROG_FR_3305 : { material: 'Al 2024-T3',   family: 'aluminium', stock_kg: 60,  part_kg: 7.8,  tol_um: 60 }, // fuselage frame
  PROG_BK_1180 : { material: 'Ti-6Al-4V',    family: 'titanium',  stock_kg: 22,  part_kg: 2.4,  tol_um: 40 }, // titanium bracket
  PROG_LG_9032 : { material: 'Ti-5553',      family: 'titanium',  stock_kg: 140, part_kg: 18.0, tol_um: 40 }, // landing-gear lug
  PROG_TB_2210 : { material: 'Inconel 718',  family: 'inconel',   stock_kg: 12,  part_kg: 1.6,  tol_um: 25 }, // turbine blade/disc
  PROG_ENG_5521: { material: 'Inconel 718',  family: 'inconel',   stock_kg: 48,  part_kg: 6.1,  tol_um: 30 }, // engine casing
};
const PROGRAM_IDS = Object.keys(PROGRAMS);

// ── Asset generators ──────────────────────────────────────────────────────────
const ASSET_GENERATORS = {

  // ── 5-axis CNC machining centre ────────────────────────────────────────────
  aero_cnc: (id) => {
    const s = getState(`acnc_${id}`, () => ({
      program: PROGRAM_IDS[(id - 1) % PROGRAM_IDS.length],
      tool_wear: rand(0, 50, 1), tool_number: Math.floor(rand(1, 24, 0)), thermal_um: rand(2, 8, 1),
      status: 'running', status_ticks: 0, parts_today: 0, scrap_today: 0, cycle_ticks: 0,
      chatter_ticks: 0, work_order: 4000 + id * 17, ideal_cycle_s: rand(120, 240, 0),
    }));
    const prog = PROGRAMS[s.program];
    const mat  = MATERIALS[prog.family];

    // Occasionally move to the next NC program (new work order)
    s.cycle_ticks++;
    if (s.cycle_ticks > 90 && Math.random() < 0.05) {
      s.program = pick(PROGRAM_IDS); s.cycle_ticks = 0; s.work_order++;
      s.tool_wear = rand(0, 20, 1); s.ideal_cycle_s = rand(120, 240, 0);
    }

    // Status machine: running → (tool change / setup / fault) → running
    if (s.status !== 'running') {
      s.status_ticks--;
      if (s.status_ticks <= 0) s.status = 'running';
    } else if (Math.random() < 0.015) {
      s.status = pick(['idle', 'setup']); s.status_ticks = Math.floor(rand(2, 5, 0));
    } else if (SIMULATE_ANOMALY && Math.random() < 0.008) {
      s.status = 'fault'; s.status_ticks = Math.floor(rand(5, 12, 0));
    }
    const run = s.status === 'running';

    // Tool wear — Ti/Inconel wear far faster than Al. Replace at 100 %.
    if (run) s.tool_wear += mat.wear * rand(0.6, 1.4);
    if (s.tool_wear >= 100) { s.tool_wear = 0; s.tool_number = Math.floor(rand(1, 24, 0)); }

    // Chatter bursts (regenerative vibration) — more likely with worn tools
    if (SIMULATE_ANOMALY && run && s.chatter_ticks === 0 && Math.random() < 0.02 + s.tool_wear / 4000) s.chatter_ticks = Math.floor(rand(4, 9, 0));
    const chatter = s.chatter_ticks > 0; if (chatter) s.chatter_ticks--;

    const rpm  = run ? Math.round(rand(mat.rpm[0], mat.rpm[1], 0)) : 0;
    const vf   = run ? rand(mat.vf[0], mat.vf[1], 0) : 0;
    const ap   = run ? rand(mat.ap[0], mat.ap[1]) : 0;
    const ae   = run ? rand(mat.ae[0], mat.ae[1]) : 0;
    const mrr  = ap * ae * vf / 60;                                  // mm³/s
    const cutKw = run ? parseFloat(((mat.sce * mrr * rand(0.9, 1.15)) / 1000 + 0.4).toFixed(2)) : 0;
    const loadPct = run ? Math.min(98, parseFloat((cutKw / 40 * 100 + 8 + rand(0, 6)).toFixed(1))) : 0;

    // Spindle thermal growth — warms toward a load-dependent equilibrium
    const thermalTarget = run ? 4 + loadPct * 0.22 + (SIMULATE_ANOMALY && s.cycle_ticks % 140 > 100 ? 8 : 0) : 2;
    s.thermal_um += (thermalTarget - s.thermal_um) * 0.06;

    // Vibration — base + load + wear; chatter adds a large narrow-band burst
    const vib = run ? parseFloat((mat.vib * rand(0.6, 1.2) + loadPct / 60 + s.tool_wear / 60 + (chatter ? rand(3, 5.5) : 0)).toFixed(2)) : rand(0, 0.2);

    // In-process probing of a critical feature (µm from nominal).
    // Causal story for the demo: tool wear + thermal growth bias the cut → Cpk falls.
    const probe = run ? parseFloat((s.thermal_um * 0.35 + (s.tool_wear / 100) * prog.tol_um * 0.45 + gauss(prog.tol_um * 0.1)).toFixed(1)) : null;

    if (run && Math.random() < 0.28) { s.parts_today++; if (probe != null && Math.abs(probe) > prog.tol_um) s.scrap_today++; }

    return {
      asset_id            : `ACNC_${pad(id, 3)}`,
      asset_type          : 'aero_cnc',
      timestamp           : new Date().toISOString(),
      status              : s.status,
      program_name        : s.program,
      process_key         : s.program,           // Enrich Utility join key (process master)
      work_order_id       : `WO-${new Date().getFullYear()}-${s.work_order}`,
      operator_id         : `OP-${pad(1000 + ((id * 7 + Math.floor(s.work_order / 3)) % 40), 4)}`,
      material            : prog.material,
      operation           : pick(['Roughing', 'Semi-finishing', 'Finishing', 'Pocketing', 'Contouring']),
      spindle_rpm         : rpm,
      feed_rate_mmpm      : vf,
      depth_of_cut_mm     : ap,
      width_of_cut_mm     : ae,
      spindle_load_pct    : loadPct,
      cutting_power_kw    : cutKw,
      spindle_temp_c      : run ? rand(38, 70) + s.thermal_um * 0.3 : rand(24, 34),
      coolant_temp_c      : rand(18, 28),
      coolant_flow_lpm    : run ? rand(12, 30) : 0,
      coolant_pressure_bar: run ? (prog.family === 'aluminium' ? rand(10, 20) : rand(40, 70)) : 0, // HPC for Ti/Inconel
      vibration_g         : vib,
      dominant_freq_hz    : run ? (chatter ? rand(900, 2400, 0) : parseFloat((rpm / 60 * 4).toFixed(0))) : 0, // chatter vs 4-flute tooth-pass
      tool_number         : s.tool_number,
      tool_wear_pct       : parseFloat(s.tool_wear.toFixed(1)),
      thermal_growth_um   : parseFloat(s.thermal_um.toFixed(1)),
      probe_deviation_um  : probe,
      tolerance_um        : prog.tol_um,
      surface_roughness_ra_um: run ? parseFloat((0.4 + s.tool_wear / 100 * 0.9 + (chatter ? 0.8 : 0) + rand(0, 0.15)).toFixed(2)) : null,
      cycle_time_s        : run ? Math.round(s.ideal_cycle_s * rand(0.95, 1.35)) : 0,
      ideal_cycle_time_s  : s.ideal_cycle_s,
      parts_produced_today: s.parts_today,
      scrap_count_today   : s.scrap_today,
      stock_weight_kg     : prog.stock_kg,
      part_weight_kg      : prog.part_kg,
      axes_positions      : { x: rand(-800, 800), y: rand(-600, 600), z: rand(-400, 100), a: rand(-120, 30), c: rand(-180, 180) },
      alarm_code          : s.status === 'fault' ? pick(['SPN_OVERLOAD', 'TOOL_BREAK', 'AXIS_FOLLOW_ERR', 'COOLANT_LOW']) : null,
    };
  },

  // ── Coordinate Measuring Machine (inspection cell) ─────────────────────────
  aero_cmm: (id) => {
    const s = getState(`acmm_${id}`, () => ({
      program: PROGRAM_IDS[(id * 2) % PROGRAM_IDS.length], part_n: 1000 * id, probe_age_h: rand(0, 40, 1),
      drift_um: 0, status: 'running', status_ticks: 0, temp_exc_ticks: 0,
    }));
    const prog = PROGRAMS[s.program];
    s.part_n++;
    if (s.part_n % 30 === 0) s.program = pick(PROGRAM_IDS);
    s.probe_age_h += 0.03;
    if (s.probe_age_h > 168) s.probe_age_h = 0; // re-qualified

    if (s.status !== 'running') { if (--s.status_ticks <= 0) s.status = 'running'; }
    else if (SIMULATE_ANOMALY && Math.random() < 0.01) { s.status = 'fault'; s.status_ticks = Math.floor(rand(3, 8, 0)); }
    else if (Math.random() < 0.03) { s.status = 'idle'; s.status_ticks = Math.floor(rand(1, 3, 0)); }
    const meas = s.status === 'running'; // 'running' = actively measuring a part

    // Upstream process drift is visible in the CMM data as a mean shift
    if (SIMULATE_ANOMALY && Math.random() < 0.02) s.drift_um = rand(0, prog.tol_um * 0.5);
    s.drift_um *= 0.97;

    // CMMs must stay at 20 ± 1 °C; excursions invalidate measurement
    if (SIMULATE_ANOMALY && s.temp_exc_ticks === 0 && Math.random() < 0.012) s.temp_exc_ticks = Math.floor(rand(3, 7, 0));
    const tempExc = s.temp_exc_ticks > 0; if (tempExc) s.temp_exc_ticks--;

    const nFeat = meas ? Math.floor(rand(40, 120, 0)) : 0;
    const critDev = meas ? parseFloat((s.drift_um + gauss(prog.tol_um * 0.12)).toFixed(1)) : null;
    const nOOT = meas ? (Math.abs(critDev) > prog.tol_um ? 1 + (Math.random() < 0.3 ? 1 : 0) : (Math.random() < 0.015 ? 1 : 0)) : 0;

    return {
      asset_id            : `ACMM_${pad(id, 3)}`,
      asset_type          : 'aero_cmm',
      timestamp           : new Date().toISOString(),
      status              : s.status,
      program_name        : s.program,
      process_key         : s.program,           // Enrich Utility join key (process master)
      part_serial         : `SN-${pad(s.part_n, 6)}`,
      source_machine_id   : `ACNC_${pad(1 + (s.part_n % Math.max(1, COUNTS.aero_cnc)), 3)}`,
      is_first_article    : meas && s.part_n % 25 === 0,
      features_measured   : nFeat,
      features_out_of_tol : nOOT,
      critical_feature_id : meas ? pick(['KC-BORE-01', 'KC-FLANGE-03', 'KC-WEBTHK-02', 'KC-PROFILE-07']) : null,
      critical_dev_um     : critDev,
      tolerance_um        : prog.tol_um,
      chamber_temp_c      : parseFloat((20 + gauss(0.12) + (tempExc ? rand(1.2, 2.6) : 0)).toFixed(2)),
      measurement_uncertainty_um: parseFloat((1.6 + s.probe_age_h / 168 * 2.4 + rand(0, 0.4)).toFixed(2)),
      probe_calibration_age_h   : parseFloat(s.probe_age_h.toFixed(1)),
      inspection_time_s   : meas ? Math.round(rand(240, 900, 0)) : 0,
      alarm_code          : s.status === 'fault' ? pick(['PROBE_COLLISION', 'AIR_BEARING_LOW', 'SCALE_ERR']) : null,
    };
  },

  // ── Composite-cure autoclave ───────────────────────────────────────────────
  // Each tick advances the cure cycle clock by SIM_MIN_PER_TICK simulated minutes
  // so a full 4-hour cure plays out in a few real minutes.
  aero_autoclave: (id) => {
    const RECIPES = {
      'CFRP-180C-HP': { dwell_c: 180, pressure_bar: 7.0, dwell_min: 120, ramp_cpm: 2.0 }, // structural prepreg
      'CFRP-120C':    { dwell_c: 120, pressure_bar: 6.0, dwell_min: 90,  ramp_cpm: 2.0 }, // repair / secondary structure
    };
    const s = getState(`aaut_${id}`, () => ({
      recipe: 'CFRP-180C-HP', phase: 'idle', phase_ticks: 2 + id * 12, // stagger cycle starts
      air_c: 22, part_c: 22, pressure: 0.1, elapsed_min: 0, dwell_min: 0, batch_n: 100 * id, leak: 0, cold_tc: 0,
    }));
    const r = RECIPES[s.recipe];
    const dt = SIM_MIN_PER_TICK;
    let rampRate = 0;

    if (s.phase === 'idle') {
      if (--s.phase_ticks <= 0) {
        s.phase = 'ramp'; s.recipe = pick(Object.keys(RECIPES)); s.batch_n++; s.elapsed_min = 0; s.dwell_min = 0; s.leak = 0; s.cold_tc = 0;
      }
    } else if (s.phase === 'ramp') {
      rampRate = r.ramp_cpm + gauss(0.18) + (SIMULATE_ANOMALY && Math.random() < 0.04 ? rand(1.4, 2.5) : 0);
      s.air_c += rampRate * dt * 1.08;
      s.part_c += rampRate * dt * 0.97;                // composite lags the air
      s.pressure = Math.min(r.pressure_bar, s.pressure + r.pressure_bar / ((r.dwell_c - 22) / (r.ramp_cpm * dt)));
      if (s.part_c >= r.dwell_c - 2) s.phase = 'dwell';
    } else if (s.phase === 'dwell') {
      s.air_c = r.dwell_c + gauss(0.8); s.part_c = r.dwell_c + gauss(1.2); s.pressure = r.pressure_bar + gauss(0.05);
      s.dwell_min += dt;
      if (s.dwell_min >= r.dwell_min) s.phase = 'cool';
    } else if (s.phase === 'cool') {
      rampRate = -2.5 + gauss(0.2);
      s.air_c += rampRate * dt; s.part_c += rampRate * dt * 0.9;
      s.pressure = Math.max(0.1, s.pressure - 0.35);
      if (s.part_c <= 60) { s.phase = 'idle'; s.phase_ticks = 8; s.air_c = 25; s.part_c = 25; s.pressure = 0.1; }
    }
    if (s.phase !== 'idle') s.elapsed_min += dt;

    // Anomalies: bag leak (vacuum loss) and a cold thermocouple (blocked airflow / tooling mass)
    if (SIMULATE_ANOMALY && s.phase !== 'idle') {
      if (s.leak === 0 && Math.random() < 0.0015) s.leak = 1;
      if (s.cold_tc === 0 && Math.random() < 0.0015) s.cold_tc = rand(5, 12);
    }
    const active = s.phase !== 'idle';
    const spread = active ? parseFloat((1.4 + Math.abs(rampRate) * 0.8 + s.cold_tc + rand(0, 0.8)).toFixed(1)) : 0.5;
    const tcMax = parseFloat((s.part_c + spread * 0.4).toFixed(1));
    const tcMin = parseFloat((s.part_c + spread * 0.4 - spread).toFixed(1));

    return {
      asset_id            : `AAUT_${pad(id, 3)}`,
      asset_type          : 'aero_autoclave',
      timestamp           : new Date().toISOString(),
      status              : active ? 'running' : 'idle',
      cycle_phase         : s.phase,                                    // idle | ramp | dwell | cool
      cure_recipe         : s.recipe,
      process_key         : s.recipe,            // Enrich Utility join key (process master)
      batch_id            : `BATCH-${pad(s.batch_n, 5)}`,
      cycle_elapsed_min   : s.elapsed_min,
      dwell_elapsed_min   : s.dwell_min,
      sim_minutes_per_tick: dt,
      setpoint_temp_c     : active ? r.dwell_c : 0,
      setpoint_pressure_bar: active ? r.pressure_bar : 0,
      required_dwell_min  : r.dwell_min,
      air_temp_c          : parseFloat(s.air_c.toFixed(1)),
      part_temp_c         : parseFloat(s.part_c.toFixed(1)),
      tc_min_c            : tcMin,
      tc_max_c            : tcMax,
      thermocouple_count  : 12,
      pressure_bar        : parseFloat(s.pressure.toFixed(2)),
      vacuum_mbar         : active ? parseFloat((18 + gauss(3) + (s.leak ? rand(60, 140) : 0)).toFixed(1)) : 1013,
      ramp_rate_cpm       : parseFloat(rampRate.toFixed(2)),
      circulation_fan_rpm : active ? Math.round(rand(1380, 1460, 0)) : 0,
      power_kw            : active ? rand(180, 420) : rand(2, 6),
      alarm_code          : s.leak && Math.random() < 0.3 ? 'VAC_BAG_LEAK' : null,
    };
  },
};

// ── HTTP control plane ────────────────────────────────────────────────────────
const app = express();
app.use(express.json());
let tick = 0, running = true, interval = null;
app.get('/health', (_, res) => res.json({ status: 'ok', asset_types: ASSET_TYPES, counts: COUNTS }));
app.post('/control', (req, res) => {
  const { action } = req.body;
  if (action === 'pause')  { running = false; return res.json({ status: 'paused' }); }
  if (action === 'resume') { running = true;  return res.json({ status: 'resumed' }); }
  res.json({ status: running ? 'running' : 'paused' });
});

async function start() {
  const types = ASSET_TYPES.filter(t => ASSET_GENERATORS[t]);
  await producer.connect();
  console.log(`[Aero-Simulator] ${types.map(t => `${t}x${COUNTS[t]}`).join(', ')} → ${KAFKA_TOPIC_OUT}`);

  interval = setInterval(async () => {
    if (!running) return;
    tick++;
    const messages = [];
    for (const type of types) {
      for (let i = 1; i <= COUNTS[type]; i++) {
        const payload = ASSET_GENERATORS[type](i);
        messages.push({ key: payload.asset_id, value: JSON.stringify(payload), headers: { industry: 'aerospace', tick: String(tick) } });
      }
    }
    try {
      await producer.send({ topic: KAFKA_TOPIC_OUT, messages });
      console.log(`[Aero-Simulator] tick=${tick} sent ${messages.length} msgs`);
    } catch (err) { console.error('[Aero-Simulator] Kafka error:', err.message); }
  }, EMIT_INTERVAL_MS);
}

async function shutdown() { clearInterval(interval); await producer.disconnect(); process.exit(0); }
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

module.exports = { ASSET_GENERATORS, PROGRAMS };
if (require.main === module) {
  app.listen(PORT, () => console.log(`[Aero-Simulator] HTTP on :${PORT}`));
  start().catch(err => { console.error('[Aero-Simulator] Fatal:', err); process.exit(1); });
}
