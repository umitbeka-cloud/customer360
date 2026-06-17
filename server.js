/**
 * Omnidesk · AI Analytics — Local Proxy Server
 * ─────────────────────────────────────────────
 * Routes:
 *   POST /api/claude          → Anthropic API (streaming & non-streaming)
 *   GET  /omnidesk.ru/api/*      → Omnidesk REST API
 *   GET  /                    → serves the dashboard HTML
 *
 * Usage:
 *   node server.js
 *   Then open http://localhost:3000
 */

const http       = require('http');
const https      = require('https');
const fs         = require('fs');
const path       = require('path');
const { URL }    = require('url');

// PostgreSQL — run once: npm install pg
let Pool;
try { Pool = require('pg').Pool; }
catch(e) { console.warn('⚠️  pg not found. Run: npm install pg'); }

// One pool PER database key — different DBs (tariffs, TrustMe) must not evict
// each other. Evicting a pool mid-request caused "Cannot use a pool after end".
const pgPools = new Map();
function getPgPool(host, port, database, user, password, ssl) {
  const key = `${host}:${port}:${database}:${user}`;
  let pool = pgPools.get(key);
  if (pool) return pool;
  pool = new Pool({
    host, port: parseInt(port)||5432, database, user, password,
    ssl: ssl === 'true' ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: 8000, max: 3,
    // search_path so 'contract' (TrustMe NPS/CSAT) and 'public' (tariffs) both resolve
    options: '--search_path=contract,public',
  });
  pool.on('error', (e) => console.error('[PG pool]', key, e.message));
  pgPools.set(key, pool);
  return pool;
}

const PORT = process.env.PORT || 3000;

// ─── helpers ───────────────────────────────────────────────
function json(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(body);
}

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,x-api-key,anthropic-version,x-omnidesk-domain,x-omnidesk-email,x-omnidesk-key,x-pg-host,x-pg-port,x-pg-database,x-pg-user,x-pg-password,x-pg-ssl,x-bx-portal,x-bx-token,x-bx-category');
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', chunk => data += chunk);
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// Raw HTTPS request — returns { status, body }
function httpsRequest(options, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, upstream => {
      let data = '';
      upstream.on('data', c => data += c);
      upstream.on('end', () => resolve({ status: upstream.statusCode, body: data, headers: upstream.headers }));
      upstream.on('error', reject);
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

// Proxy with auto-retry on 429 (up to 5 attempts, exponential backoff)
async function proxyWithRetry(options, body, res, isStream, attempt = 1) {
  const MAX_ATTEMPTS = 5;

  // Streaming: pipe directly (no retry support needed — only used for AI)
  if (isStream) {
    return new Promise((resolve, reject) => {
      const req = https.request(options, upstream => {
        res.writeHead(upstream.statusCode, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          'Access-Control-Allow-Origin': '*',
        });
        upstream.pipe(res);
        upstream.on('end', resolve);
        upstream.on('error', reject);
      });
      req.on('error', reject);
      if (body) req.write(body);
      req.end();
    });
  }

  const { status, body: respBody } = await httpsRequest(options, body);

  // 429 — rate limited: wait and retry
  if (status === 429 && attempt <= MAX_ATTEMPTS) {
    const waitMs = Math.min(1000 * Math.pow(2, attempt - 1), 16000); // 1s, 2s, 4s, 8s, 16s
    console.warn(`[Rate limit] 429 received. Waiting ${waitMs}ms before retry ${attempt}/${MAX_ATTEMPTS}...`);
    await sleep(waitMs);
    return proxyWithRetry(options, body, res, false, attempt + 1);
  }

  // 503 — temporary server error: retry after short wait
  if (status === 503 && attempt <= 3) {
    await sleep(2000 * attempt);
    return proxyWithRetry(options, body, res, false, attempt + 1);
  }

  res.writeHead(status, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(respBody);
}

// Keep old name for compatibility
function proxyHttps(options, body, res, isStream) {
  return proxyWithRetry(options, body, res, isStream, 1);
}

// ─── server ────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  cors(res);

  // Preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const { pathname } = new URL(req.url, `http://localhost:${PORT}`);

  // ── Serve dashboard HTML ──────────────────────────────────
  if (req.method === 'GET' && pathname === '/') {
    const htmlPath = path.join(__dirname, 'dashboard.html');
    if (!fs.existsSync(htmlPath)) {
      res.writeHead(404, { 'Content-Type': 'text/plain' });
      res.end('dashboard.html not found — place it next to server.js');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    fs.createReadStream(htmlPath).pipe(res);
    return;
  }

  // ── Claude proxy ──────────────────────────────────────────
  if (req.method === 'POST' && pathname === '/api/claude') {
    let rawBody;
    try { rawBody = await readBody(req); } catch (e) {
      return json(res, 400, { error: 'Bad request body' });
    }

    let parsed;
    try { parsed = JSON.parse(rawBody); } catch (e) {
      return json(res, 400, { error: 'Invalid JSON' });
    }

    const apiKey = req.headers['x-api-key'];
    if (!apiKey) return json(res, 400, { error: 'Missing x-api-key header' });

    const isStream = !!parsed.stream;

    const options = {
      hostname: 'openrouter.ai',
      path: '/api/v1/chat/completions',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + apiKey,
        'HTTP-Referer': 'http://localhost:' + PORT,  // recommended by OpenRouter
        'X-Title': 'Omnidesk AI Analytics',
      },
    };

    try {
      await proxyHttps(options, rawBody, res, isStream);
    } catch (e) {
      if (!res.headersSent) json(res, 502, { error: e.message });
    }
    return;
  }

  // ── Omnidesk proxy ────────────────────────────────────────
  // Omnidesk auth: Basic Auth with staff_email:api_key
  // URL format:    https://[domain].omnidesk.ru/api/[endpoint].json
  if (req.method === 'GET' && pathname.startsWith('/omnidesk.ru/api')) {
    const domain = req.headers['x-omnidesk-domain'];  // e.g. "yourcompany.omnidesk.ru"
    const email  = req.headers['x-omnidesk-email'];   // staff email (not admin)
    const key    = req.headers['x-omnidesk-key'];     // API key from Settings → API

    if (!domain || !email || !key) {
      return json(res, 400, { error: 'Missing x-omnidesk-domain, x-omnidesk-email or x-omnidesk-key header' });
    }

    // Basic Auth: base64(staff_email:api_key)
    const credentials = Buffer.from(email + ':' + key).toString('base64');

    // Strip our proxy prefix → /api/[endpoint].json
    // e.g. /omnidesk.ru/api/cases → /api/cases.json
    let odEndpoint = pathname.replace('/omnidesk.ru/api', '');
    if (!odEndpoint.endsWith('.json')) odEndpoint = odEndpoint + '.json';

    const queryString = req.url.includes('?') ? req.url.slice(req.url.indexOf('?')) : '';
    const cleanDomain = domain.replace(/^https?:\/\//, '').replace(/\/$/, '');

    const options = {
      hostname: cleanDomain,
      path: '/api' + odEndpoint + queryString,
      method: 'GET',
      headers: {
        'Authorization': 'Basic ' + credentials,
        'Content-Type': 'application/json',
      },
    };

    console.log('[Omnidesk] →', options.hostname + options.path);

    try {
      await proxyHttps(options, null, res, false);
    } catch (e) {
      if (!res.headersSent) json(res, 502, { error: e.message });
    }
    return;
  }

  // ── PostgreSQL: tariff history ───────────────────────────
  if (req.method === 'GET' && pathname === '/api/pg/tariffs') {
    if (!Pool) return json(res, 503, { error: 'pg not installed. Run: npm install pg' });
    const h = req.headers;
    if (!h['x-pg-host'] || !h['x-pg-database'] || !h['x-pg-user'])
      return json(res, 400, { error: 'Missing PostgreSQL credentials' });

    const reqUrl = new URL(req.url, `http://localhost:${PORT}`);
    const companyBin  = (reqUrl.searchParams.get('bin')     || '').replace(/\D/g, '');
    const companyName = (reqUrl.searchParams.get('company') || '').trim();

    try {
      const pool = getPgPool(
        h['x-pg-host'], h['x-pg-port']||'5432',
        h['x-pg-database'], h['x-pg-user'],
        h['x-pg-password']||'', h['x-pg-ssl']||'false'
      );

      // Нет параметров = тест подключения
      if (!companyBin && !companyName) {
        await pool.query('SELECT 1 FROM sign_subscriptions LIMIT 1');
        return json(res, 200, { rows: [], test: true });
      }

      let result;

      // Попытка 1: по БИН (точное совпадение) — самый надёжный
      if (companyBin) {
        result = await pool.query(
          `SELECT DISTINCT ON (s.id)
             s.id, s.tarif_name, s.prise, s.sign_count,
             s.is_active, s.is_frozen, s.is_deleted,
             s.expiration_date, s.date_add,
             s.created_at, s.updated_at,
             s.frozen_date, s.cause, s.order_number, s.charge_id,
             c.company_name, c.company_bin
           FROM sign_subscriptions s
           JOIN charges c ON c.company_id = s.company_id
           WHERE c.company_bin = $1
           ORDER BY s.id, s.created_at DESC
           LIMIT 100`,
          [companyBin]
        );
        console.log(`[PG] БИН ${companyBin} → ${result.rows.length} строк`);
      }

      // Попытка 2: нечёткий матч по названию — убираем юр. форму, пробелы, спецсимволы
      if ((!result || result.rows.length === 0) && companyName) {
        result = await pool.query(
          `SELECT DISTINCT ON (s.id)
             s.id, s.tarif_name, s.prise, s.sign_count,
             s.is_active, s.is_frozen, s.is_deleted,
             s.expiration_date, s.date_add,
             s.created_at, s.updated_at,
             s.frozen_date, s.cause, s.order_number, s.charge_id,
             c.company_name, c.company_bin
           FROM sign_subscriptions s
           JOIN charges c ON c.company_id = s.company_id
           WHERE regexp_replace(lower(c.company_name), '[^a-zа-яёa-z0-9]', '', 'g')
               = regexp_replace(lower($1),             '[^a-zа-яёa-z0-9]', '', 'g')
           ORDER BY s.id, s.created_at DESC
           LIMIT 100`,
          [companyName]
        );
        console.log(`[PG] Точное (нормализованное) имя "${companyName}" → ${result.rows.length} строк`);
      }

      // Попытка 3: LIKE по ключевому слову — берём самое длинное слово из названия (>3 букв)
      if ((!result || result.rows.length === 0) && companyName) {
        // Выбираем самое длинное слово как наиболее уникальное
        const words = companyName
          .replace(/ТОО|ООО|ОАО|ЗАО|АО|ИП|LLP|LLC|JSC/gi, '')
          .split(/\s+/)
          .map(w => w.replace(/[^a-zа-яёa-z0-9]/gi, ''))
          .filter(w => w.length > 3)
          .sort((a, b) => b.length - a.length);

        if (words.length > 0) {
          const keyword = words[0];
          result = await pool.query(
            `SELECT DISTINCT ON (s.id)
               s.id, s.tarif_name, s.prise, s.sign_count,
               s.is_active, s.is_frozen, s.is_deleted,
               s.expiration_date, s.date_add,
               s.created_at, s.updated_at,
               s.frozen_date, s.cause, s.order_number, s.charge_id,
               c.company_name, c.company_bin
             FROM sign_subscriptions s
             JOIN charges c ON c.company_id = s.company_id
             WHERE lower(c.company_name) LIKE lower($1)
             ORDER BY s.id, s.created_at DESC
             LIMIT 100`,
            [`%${keyword}%`]
          );
          console.log(`[PG] LIKE "%${keyword}%" → ${result.rows.length} строк`);
        }
      }

      json(res, 200, { rows: result?.rows || [], matched_by: result?.rows?.length ? 'fuzzy' : 'none' });
    } catch(e) {
      console.error('[PG]', e.message);
      json(res, 500, { error: e.message });
    }
    return;
  }

  // ── TrustMe: NPS data ────────────────────────────────────
  if (req.method === 'GET' && pathname === '/api/pg/nps') {
    if (!Pool) return json(res, 503, { error: 'pg not installed. Run: npm install pg' });
    const h = req.headers;
    if (!h['x-pg-host'] || !h['x-pg-database'] || !h['x-pg-user'])
      return json(res, 400, { error: 'Missing PostgreSQL credentials' });
    try {
      const pool = getPgPool(h['x-pg-host'], h['x-pg-port']||'5432', h['x-pg-database'], h['x-pg-user'], h['x-pg-password']||'', h['x-pg-ssl']||'false');
      const u = new URL(req.url, `http://localhost:${PORT}`);
      const dateFrom    = u.searchParams.get('date_from') || '';
      const dateTo      = u.searchParams.get('date_to')   || '';
      const category    = u.searchParams.get('category')  || '';
      const companyName = u.searchParams.get('company')   || '';

      const conditions = [];
      const params = [];
      if (dateFrom)    { params.push(dateFrom);    conditions.push(`DATE(created_at_utc) >= $${params.length}`); }
      if (dateTo)      { params.push(dateTo);      conditions.push(`DATE(created_at_utc) <= $${params.length}`); }
      if (category)    { params.push(category.toLowerCase()); conditions.push(`LOWER(category) = $${params.length}`); }
      if (companyName) { params.push(`%${companyName}%`);     conditions.push(`LOWER(company_name) LIKE LOWER($${params.length})`); }

      const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

      const [rows, statsResult, distResult] = await Promise.all([
        pool.query(`SELECT id, created_at_utc, company_name, phone, user_name, score, comment, category FROM contract.nps ${where} ORDER BY created_at_utc DESC LIMIT 1000`, params),
        pool.query(`SELECT
            COUNT(*)                                                  AS total,
            COUNT(*) FILTER (WHERE LOWER(category)='promoter')       AS promoters,
            COUNT(*) FILTER (WHERE LOWER(category)='detractor')      AS detractors,
            COUNT(*) FILTER (WHERE LOWER(category)='passive')        AS passives,
            ROUND(AVG(score::numeric), 2)                             AS avg_score
          FROM contract.nps ${where}`, params),
        pool.query(`SELECT score, COUNT(*) AS cnt FROM contract.nps ${where} GROUP BY score ORDER BY score DESC`, params),
      ]);

      const s = statsResult.rows[0] || {};
      const total = parseInt(s.total) || 0;
      return json(res, 200, {
        rows:  rows.rows,
        total,
        promoters:  parseInt(s.promoters)  || 0,
        detractors: parseInt(s.detractors) || 0,
        passives:   parseInt(s.passives)   || 0,
        avg_score:  parseFloat(s.avg_score) || 0,
        nps: total ? Math.round(((parseInt(s.promoters)||0) - (parseInt(s.detractors)||0)) / total * 100) : 0,
        score_dist: distResult.rows,
      });
    } catch(e) {
      console.error('[PG/NPS]', e.message);
      return json(res, 500, { error: e.message });
    }
  }

  // ── TrustMe: CSAT data ───────────────────────────────────
  if (req.method === 'GET' && pathname === '/api/pg/csat') {
    if (!Pool) return json(res, 503, { error: 'pg not installed. Run: npm install pg' });
    const h = req.headers;
    if (!h['x-pg-host'] || !h['x-pg-database'] || !h['x-pg-user'])
      return json(res, 400, { error: 'Missing PostgreSQL credentials' });
    try {
      const pool = getPgPool(h['x-pg-host'], h['x-pg-port']||'5432', h['x-pg-database'], h['x-pg-user'], h['x-pg-password']||'', h['x-pg-ssl']||'false');
      const u = new URL(req.url, `http://localhost:${PORT}`);
      const dateFrom    = u.searchParams.get('date_from') || '';
      const dateTo      = u.searchParams.get('date_to')   || '';
      const category    = u.searchParams.get('category')  || '';
      const companyName = u.searchParams.get('company')   || '';

      const conditions = [];
      const params = [];
      if (dateFrom)    { params.push(dateFrom);    conditions.push(`DATE(created_at_utc) >= $${params.length}`); }
      if (dateTo)      { params.push(dateTo);      conditions.push(`DATE(created_at_utc) <= $${params.length}`); }
      if (category)    { params.push(category.toLowerCase()); conditions.push(`LOWER(category) = $${params.length}`); }
      if (companyName) { params.push(`%${companyName}%`);     conditions.push(`LOWER(company_name) LIKE LOWER($${params.length})`); }

      const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

      const [rows, distResult] = await Promise.all([
        pool.query(`SELECT id, created_at_utc, company_name, phone, user_name, score, comment, category FROM contract.csat ${where} ORDER BY created_at_utc DESC LIMIT 1000`, params),
        pool.query(`SELECT score, COUNT(*) AS cnt FROM contract.csat ${where} GROUP BY score ORDER BY score DESC`, params),
      ]);

      const statsResult = await pool.query(
        `SELECT COUNT(*) AS total,
           COUNT(*) FILTER (WHERE LOWER(category)='satisfied')    AS satisfied,
           COUNT(*) FILTER (WHERE LOWER(category)='dissatisfied') AS dissatisfied,
           COUNT(*) FILTER (WHERE LOWER(category)='neutral')      AS neutral,
           ROUND(AVG(score)::numeric, 2) AS avg_score
         FROM contract.csat ${where}`, params
      );

      const s = statsResult.rows[0] || {};
      const total = parseInt(s.total) || 0;
      return json(res, 200, {
        rows: rows.rows,
        total,
        satisfied:    parseInt(s.satisfied)    || 0,
        dissatisfied: parseInt(s.dissatisfied) || 0,
        neutral:      parseInt(s.neutral)      || 0,
        avg_score:    parseFloat(s.avg_score)  || 0,
        csat: total ? Math.round((parseInt(s.satisfied)||0) / total * 100) : 0,
        score_dist: distResult.rows,
      });
    } catch(e) {
      console.error('[PG/CSAT]', e.message);
      return json(res, 500, { error: e.message });
    }
  }

  // ── TrustMe: Подписание_TrustContract (sign_history помесячно) ──
  if (req.method === 'GET' && pathname === '/api/pg/sign-history') {
    if (!Pool) return json(res, 503, { error: 'pg not installed. Run: npm install pg' });
    const h = req.headers;
    if (!h['x-pg-host'] || !h['x-pg-database'] || !h['x-pg-user'])
      return json(res, 400, { error: 'Missing PostgreSQL credentials' });

    try {
      // Используем ту же БД что для тарифов (sign_subscriptions) — pg-* поля
      const pool = getPgPool(
        h['x-pg-host'], h['x-pg-port']||'5432',
        h['x-pg-database'], h['x-pg-user'],
        h['x-pg-password']||'', h['x-pg-ssl']||'false'
      );

      const u = new URL(req.url, `http://localhost:${PORT}`);
      const dateFrom = u.searchParams.get('date_from') || '';
      const dateTo   = u.searchParams.get('date_to')   || '';

      const conditions = [];
      const params = [];
      if (dateFrom) { params.push(dateFrom); conditions.push(`sh.create_at >= $${params.length}`); }
      if (dateTo)   { params.push(dateTo + ' 23:59:59'); conditions.push(`sh.create_at <= $${params.length}`); }
      const where = conditions.length ? 'WHERE ' + conditions.join(' AND ') : '';

      // Помесячная сводка: строки = месяц, колонки = сумма count по каждой причине (dic_sign_reasons.comment)
      const result = await pool.query(
        `SELECT
           to_char(sh.create_at, 'YYYY-MM') AS month,
           dr.comment                       AS reason,
           SUM(sh.count)                    AS total
         FROM sign_history sh
         JOIN dic_sign_reasons dr ON dr.id = sh.cause
         ${where}
         GROUP BY month, reason
         ORDER BY month ASC`,
        params
      );

      return json(res, 200, { rows: result.rows });
    } catch(e) {
      console.error('[PG/SignHistory]', e.message);
      return json(res, 500, { error: e.message });
    }
  }

  // ── Risk zones: risk_scores (public) ───────────────────────
  //  score < 0.3        → green
  //  0.3 <= score < 0.6 → yellow
  //  score >= 0.6       → red
  //  Optional ?assigned=<bitrix_id> reserved for future per-MOP filtering.
  if (req.method === 'GET' && pathname === '/api/pg/risk-zones') {
    if (!Pool) return json(res, 503, { error: 'pg not installed. Run: npm install pg' });
    const h = req.headers;
    if (!h['x-pg-host'] || !h['x-pg-database'] || !h['x-pg-user'])
      return json(res, 400, { error: 'Missing PostgreSQL credentials' });
    try {
      const pool = getPgPool(
        h['x-pg-host'], h['x-pg-port']||'5432',
        h['x-pg-database'], h['x-pg-user'],
        h['x-pg-password']||'', h['x-pg-ssl']||'false'
      );

      // Aggregate counts per zone. Each company may have multiple rows over time,
      // so take the latest score per company_id via DISTINCT ON.
      const aggResult = await pool.query(`
        WITH latest AS (
          SELECT DISTINCT ON (company_id) company_id, score
          FROM risk_scores
          ORDER BY company_id, updated_at DESC NULLS LAST, created_at DESC NULLS LAST
        )
        SELECT
          COUNT(*) FILTER (WHERE score < 0.3)                  AS green,
          COUNT(*) FILTER (WHERE score >= 0.3 AND score < 0.6) AS yellow,
          COUNT(*) FILTER (WHERE score >= 0.6)                 AS red,
          COUNT(*)                                             AS total
        FROM latest
      `);

      // Top risky companies (highest score) for the "Топ рисков" block
      const topResult = await pool.query(`
        WITH latest AS (
          SELECT DISTINCT ON (company_id) company_id, score, updated_at
          FROM risk_scores
          ORDER BY company_id, updated_at DESC NULLS LAST, created_at DESC NULLS LAST
        )
        SELECT company_id, score
        FROM latest
        ORDER BY score DESC
        LIMIT 10
      `);

      const a = aggResult.rows[0] || {};
      return json(res, 200, {
        green:  parseInt(a.green)  || 0,
        yellow: parseInt(a.yellow) || 0,
        red:    parseInt(a.red)    || 0,
        total:  parseInt(a.total)  || 0,
        top:    topResult.rows.map(r => ({ company_id: r.company_id, score: parseFloat(r.score) })),
      });
    } catch(e) {
      console.error('[PG/RiskZones]', e.message);
      return json(res, 500, { error: e.message });
    }
  }

  // ── GET /api/pg/subscription?company_id=<uuid> ───────────
  //  Active/last subscription for a company from sign_subscriptions.
  //  Lives in tariffs DB → uses pg-* headers. No mapping table needed.
  if (req.method === 'GET' && pathname === '/api/pg/subscription') {
    if (!Pool) return json(res, 503, { error: 'pg not installed. Run: npm install pg' });
    const h = req.headers;
    if (!h['x-pg-host'] || !h['x-pg-database'] || !h['x-pg-user'])
      return json(res, 400, { error: 'Missing PostgreSQL credentials' });

    const u = new URL(req.url, `http://localhost:${PORT}`);
    const companyId = (u.searchParams.get('company_id') || '').trim();
    if (!companyId) return json(res, 400, { error: 'company_id (uuid) required' });

    try {
      const pool = getPgPool(
        h['x-pg-host'], h['x-pg-port']||'5432',
        h['x-pg-database'], h['x-pg-user'],
        h['x-pg-password']||'', h['x-pg-ssl']||'false'
      );
      // prefer active, non-deleted; fall back to most recent
      const result = await pool.query(`
        SELECT tarif_name, prise, sign_count, expiration_date, is_active,
               is_frozen, frozen_date, date_add, created_at
        FROM sign_subscriptions
        WHERE company_id = $1 AND (is_deleted IS NULL OR is_deleted = false)
        ORDER BY is_active DESC NULLS LAST, expiration_date DESC NULLS LAST
        LIMIT 1
      `, [companyId]);

      if (!result.rows.length) {
        return json(res, 200, { found: false, company_id: companyId });
      }
      const r = result.rows[0];
      // days left until expiration
      let daysLeft = null;
      if (r.expiration_date) {
        const exp = new Date(r.expiration_date);
        daysLeft = Math.ceil((exp - new Date()) / (1000*60*60*24));
      }
      return json(res, 200, {
        found: true,
        company_id: companyId,
        tarif_name: r.tarif_name,
        price: r.prise ? parseInt(r.prise) : null,
        sign_count: r.sign_count ? parseInt(r.sign_count) : null,
        expiration_date: r.expiration_date,
        days_left: daysLeft,
        is_active: r.is_active,
        is_frozen: r.is_frozen,
        date_add: r.date_add,
      });
    } catch(e) {
      console.error('[PG/Subscription]', e.message);
      return json(res, 500, { error: e.message });
    }
  }

  // ── GET /api/features ────────────────────────────────────
  //  Reads trustme_features.csv (next to server.js) — catalog of all
  //  product features/feature-flags. UTF-8, comma-separated.
  if (req.method === 'GET' && pathname === '/api/features') {
    const csvPath = path.join(__dirname, 'trustme_features.csv');
    try {
      if (!fs.existsSync(csvPath)) {
        return json(res, 404, { error: 'trustme_features.csv не найден рядом с server.js' });
      }
      let text = fs.readFileSync(csvPath).toString('utf8').replace(/^\uFEFF/, '');
      function parseCsvLine(line) {
        const out = []; let cur = ''; let inQ = false;
        for (let i = 0; i < line.length; i++) {
          const ch = line[i];
          if (ch === '"') { if (inQ && line[i+1] === '"') { cur += '"'; i++; } else inQ = !inQ; }
          else if (ch === ',' && !inQ) { out.push(cur); cur = ''; }
          else cur += ch;
        }
        out.push(cur); return out;
      }
      const lines = text.split(/\r?\n/).filter(l => l.trim());
      const rows = [];
      for (let i = 1; i < lines.length; i++) {
        const p = parseCsvLine(lines[i]);
        rows.push({ name: (p[0]||'').trim(), type: (p[1]||'').trim(), desc: (p[2]||'').trim(), code: (p[3]||'').trim() });
      }
      return json(res, 200, { count: rows.length, features: rows });
    } catch(e) {
      console.error('[Features]', e.message);
      return json(res, 500, { error: e.message });
    }
  }

  // ── GET /api/mop/plan?assigned=<id> ──────────────────────
  //  Reads plan_mop_june.csv (next to server.js), format:
  //  Имя;id_bitrix;План;Факт   (semicolon-separated, windows-1251)
  //  Returns plan/fact for a given manager id, or the whole list.
  if (req.method === 'GET' && pathname === '/api/mop/plan') {
    const u = new URL(req.url, `http://localhost:${PORT}`);
    const assigned = (u.searchParams.get('assigned') || '').trim();
    const csvPath = path.join(__dirname, 'plan_mop_june.csv');

    try {
      if (!fs.existsSync(csvPath)) {
        return json(res, 404, { error: 'plan_mop_june.csv не найден рядом с server.js' });
      }
      const buf = fs.readFileSync(csvPath);
      // file is windows-1251 encoded
      const text = new TextDecoder('windows-1251').decode(buf);
      const lines = text.split(/\r?\n/).filter(l => l.trim());

      // skip header; parse rows: name ; id ; plan ; fact
      const rows = [];
      for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(';');
        if (parts.length < 4) continue;
        const name = (parts[0] || '').trim();
        const id   = (parts[1] || '').trim();
        // strip spaces used as thousand separators, keep digits only
        const plan = parseInt((parts[2] || '').replace(/\D/g, ''), 10) || 0;
        const fact = parseInt((parts[3] || '').replace(/\D/g, ''), 10) || 0;
        if (!id) continue;
        rows.push({ name, id, plan, fact });
      }

      if (assigned) {
        const row = rows.find(r => r.id === assigned);
        if (!row) return json(res, 200, { found: false, assigned, plan: 0, fact: 0 });
        return json(res, 200, {
          found: true,
          assigned,
          name: row.name,
          plan: row.plan,
          fact: row.fact,
          percent: row.plan ? Math.round(row.fact / row.plan * 100) : 0,
        });
      }
      return json(res, 200, { rows });
    } catch(e) {
      console.error('[MOP/Plan]', e.message);
      return json(res, 500, { error: e.message });
    }
  }

  // ── GET /api/mop/mapping ─────────────────────────────────
  //  Reads company_mapping.csv (next to server.js), semicolon-separated, UTF-8.
  //  Expected columns (header-based, order-independent, case-insensitive):
  //    название/title, bitrix_id, omni_id, company_id
  //  Query params:
  //    ?bitrix_id=<id>   → returns the single matching row
  //    ?company_id=<id>  → returns the single matching row
  //    ?omni_id=<id>     → returns the single matching row
  //    (none)            → returns all rows + lookup maps
  if (req.method === 'GET' && pathname === '/api/mop/mapping') {
    const csvPath = path.join(__dirname, 'company_mapping.csv');
    try {
      if (!fs.existsSync(csvPath)) {
        return json(res, 404, { error: 'company_mapping.csv не найден рядом с server.js. Положите файл когда он будет готов.' });
      }
      const buf = fs.readFileSync(csvPath);
      // try UTF-8 first; if it has the BOM or looks valid, use it; fallback to 1251
      let text = buf.toString('utf8');
      // crude mojibake check: many replacement chars → likely 1251
      if ((text.match(/\uFFFD/g) || []).length > 5) {
        text = new TextDecoder('windows-1251').decode(buf);
      }
      text = text.replace(/^\uFEFF/, ''); // strip BOM
      const lines = text.split(/\r?\n/).filter(l => l.trim());
      if (!lines.length) return json(res, 200, { rows: [], count: 0 });

      // header → column index, normalized
      const header = lines[0].split(';').map(s => s.trim().toLowerCase());
      const col = (names) => {
        for (const n of names) {
          const i = header.indexOf(n);
          if (i !== -1) return i;
        }
        return -1;
      };
      const iTitle   = col(['название','название компании','title','name','company']);
      const iBitrix  = col(['bitrix_id','bitrixid','bx_id','id_bitrix']);
      const iOmni    = col(['omni_id','omniid','omnidesk_id']);
      const iCompany = col(['company_id','companyid','uuid','company_uuid']);

      const rows = [];
      for (let i = 1; i < lines.length; i++) {
        const p = lines[i].split(';');
        const row = {
          title:      iTitle   >= 0 ? (p[iTitle]   || '').trim() : '',
          bitrix_id:  iBitrix  >= 0 ? (p[iBitrix]  || '').trim() : '',
          omni_id:    iOmni    >= 0 ? (p[iOmni]    || '').trim() : '',
          company_id: iCompany >= 0 ? (p[iCompany] || '').trim() : '',
        };
        if (!row.title && !row.bitrix_id && !row.company_id && !row.omni_id) continue;
        rows.push(row);
      }

      // single-row lookup if a query param is provided
      const u = new URL(req.url, `http://localhost:${PORT}`);
      const qBitrix  = (u.searchParams.get('bitrix_id')  || '').trim();
      const qCompany = (u.searchParams.get('company_id') || '').trim();
      const qOmni    = (u.searchParams.get('omni_id')    || '').trim();
      if (qBitrix || qCompany || qOmni) {
        const found = rows.find(r =>
          (qBitrix  && r.bitrix_id  === qBitrix) ||
          (qCompany && r.company_id === qCompany) ||
          (qOmni    && r.omni_id    === qOmni)
        );
        return json(res, 200, { found: !!found, row: found || null });
      }

      // build lookup maps for the client (by each id type)
      const byBitrix = {}, byCompany = {}, byOmni = {};
      for (const r of rows) {
        if (r.bitrix_id)  byBitrix[r.bitrix_id]   = r;
        if (r.company_id) byCompany[r.company_id] = r;
        if (r.omni_id)    byOmni[r.omni_id]       = r;
      }
      return json(res, 200, {
        count: rows.length,
        columns_detected: { title: iTitle>=0, bitrix_id: iBitrix>=0, omni_id: iOmni>=0, company_id: iCompany>=0 },
        rows,
        by_bitrix: byBitrix,
        by_company: byCompany,
        by_omni: byOmni,
      });
    } catch(e) {
      console.error('[MOP/Mapping]', e.message);
      return json(res, 500, { error: e.message });
    }
  }

  // ── GET /api/mop/debt ────────────────────────────────────
  //  Reads debit.csv (next to server.js), comma-separated, UTF-8.
  //  Key columns: company_id (uuid), Долг (debt amount), fullname, bin.
  //  ?company_id=<uuid> → single company's debt
  //  (none)             → all rows + by_company map + total
  if (req.method === 'GET' && pathname === '/api/mop/debt') {
    const csvPath = path.join(__dirname, 'debit.csv');
    try {
      if (!fs.existsSync(csvPath)) {
        return json(res, 404, { error: 'debit.csv не найден рядом с server.js' });
      }
      let text = fs.readFileSync(csvPath).toString('utf8').replace(/^\uFEFF/, '');

      // minimal CSV parser supporting quoted fields with commas
      function parseCsvLine(line) {
        const out = []; let cur = ''; let inQ = false;
        for (let i = 0; i < line.length; i++) {
          const ch = line[i];
          if (ch === '"') {
            if (inQ && line[i+1] === '"') { cur += '"'; i++; }
            else inQ = !inQ;
          } else if (ch === ',' && !inQ) { out.push(cur); cur = ''; }
          else cur += ch;
        }
        out.push(cur);
        return out;
      }

      const lines = text.split(/\r?\n/).filter(l => l.trim());
      if (!lines.length) return json(res, 200, { rows: [], total: 0 });

      const header = parseCsvLine(lines[0]).map(s => s.trim().toLowerCase());
      const idx = (names) => { for (const n of names) { const i = header.indexOf(n); if (i !== -1) return i; } return -1; };
      const iCompany = idx(['company_id','companyid','uuid']);
      const iDebt    = idx(['долг','debt']);
      const iName    = idx(['fullname','название','название компании','name']);
      const iBin     = idx(['bin','бин']);
      const iTarif   = idx(['tarif_name','тариф','вид тарифа']);

      const toNum = (s) => {
        const raw = (s || '').replace(/\u00a0/g, '').replace(/\s/g, '').trim();
        return /^-?\d+$/.test(raw) ? parseInt(raw, 10) : 0;
      };

      const rows = [];
      const byCompany = {};
      let total = 0;
      for (let i = 1; i < lines.length; i++) {
        const p = parseCsvLine(lines[i]);
        const company_id = iCompany >= 0 ? (p[iCompany] || '').trim() : '';
        const debt = iDebt >= 0 ? toNum(p[iDebt]) : 0;
        const name = iName >= 0 ? (p[iName] || '').trim() : '';
        const bin  = iBin  >= 0 ? (p[iBin]  || '').trim() : '';
        if (!company_id && !name) continue;
        const row = { company_id, debt, name, bin };
        rows.push(row);
        total += debt;
        if (company_id) byCompany[company_id] = (byCompany[company_id] || 0) + debt;
      }

      const u = new URL(req.url, `http://localhost:${PORT}`);
      const qCompany = (u.searchParams.get('company_id') || '').trim();
      if (qCompany) {
        return json(res, 200, { found: byCompany[qCompany] != null, company_id: qCompany, debt: byCompany[qCompany] || 0 });
      }

      const debtors = rows.filter(r => r.debt > 0).sort((a,b) => b.debt - a.debt);
      return json(res, 200, {
        count: rows.length,
        debtors_count: debtors.length,
        total: Math.round(total),
        by_company: byCompany,
        debtors: debtors.map(r => ({ name: r.name, company_id: r.company_id, bin: r.bin, debt: r.debt })),
      });
    } catch(e) {
      console.error('[MOP/Debt]', e.message);
      return json(res, 500, { error: e.message });
    }
  }


  // ══════════════════════════════════════════════════════════
  // BITRIX24 CRM — SQLite-backed local cache
  // ══════════════════════════════════════════════════════════
  //
  //  POST /api/bitrix/sync          — full sync OR delta from Bitrix24 → SQLite
  //  GET  /api/bitrix/deals         — read from local SQLite (instant)
  //  GET  /api/bitrix/sync-status   — progress of ongoing sync
  //  GET  /api/bitrix/test          — quick connection test (1 deal)
  //
  //  Headers for all routes: x-bx-portal, x-bx-token
  //
  // ══════════════════════════════════════════════════════════

  // ── SQLite init ──────────────────────────────────────────
  // lazy-load better-sqlite3; if missing, routes return helpful error
  let bxDb = null;
  function getBxDb() {
    if (bxDb) return bxDb;
    let Database;
    try { Database = require('better-sqlite3'); }
    catch(e) { return null; }
    const dbPath = path.join(__dirname, 'bitrix_deals.db');
    bxDb = new Database(dbPath);
    bxDb.exec(`
      CREATE TABLE IF NOT EXISTS deals (
        id            TEXT PRIMARY KEY,
        title         TEXT,
        type_id       TEXT,
        stage_id      TEXT,
        stage_semantic TEXT,
        opportunity   REAL,
        currency      TEXT,
        assigned_by   TEXT,
        closedate     TEXT,
        date_create   TEXT,
        date_modify   TEXT,
        last_activity TEXT,
        last_comm     TEXT,
        source_id     TEXT,
        category_id   TEXT,
        is_closed     TEXT,
        probability   TEXT,
        raw           TEXT,
        synced_at     TEXT
      );
      CREATE TABLE IF NOT EXISTS sync_meta (
        key   TEXT PRIMARY KEY,
        value TEXT
      );
    `);
    return bxDb;
  }

  // ── Bitrix24 helper: fetch one page ─────────────────────
  async function bxFetchPage(portal, token, start, filter) {
    // Build query string manually to avoid URL length issues
    const FIELDS = ['ID','TITLE','TYPE_ID','STAGE_ID','STAGE_SEMANTIC_ID','OPPORTUNITY',
                    'CURRENCY_ID','ASSIGNED_BY_ID','COMPANY_ID','CLOSEDATE','DATE_CREATE','DATE_MODIFY',
                    'LAST_ACTIVITY_TIME','LAST_COMMUNICATION_TIME','SOURCE_ID','CATEGORY_ID',
                    'IS_CLOSED','PROBABILITY','CLOSED','COMMENTS'];
    let qs = `start=${start}&order[DATE_MODIFY]=DESC`;
    FIELDS.forEach(f => { qs += `&select[]=${f}`; });
    if (filter) {
      Object.entries(filter).forEach(([k,v]) => { qs += `&filter[${k}]=${encodeURIComponent(v)}`; });
    }

    const parsed = new URL(portal);
    const options = {
      hostname: parsed.hostname,
      path:     `/rest/${token}/crm.deal.list.json?${qs}`,
      method:   'GET',
      headers:  { 'Accept': 'application/json' },
    };
    const { status, body } = await httpsRequest(options, null);
    if (status !== 200) {
      let msg = body;
      try { msg = JSON.parse(body)?.error_description || body; } catch {}
      throw new Error(`Bitrix24 HTTP ${status}: ${msg}`);
    }
    return JSON.parse(body);
  }

  // ── Bitrix24 helper: fetch one page of COMPANIES ─────────
  async function bxFetchCompanyPage(portal, token, start, filter) {
    const FIELDS = ['ID','TITLE','ASSIGNED_BY_ID','DATE_CREATE','DATE_MODIFY'];
    let qs = `start=${start}&order[ID]=ASC`;
    FIELDS.forEach(f => { qs += `&select[]=${f}`; });
    if (filter) {
      Object.entries(filter).forEach(([k,v]) => { qs += `&filter[${k}]=${encodeURIComponent(v)}`; });
    }
    const parsed = new URL(portal);
    const options = {
      hostname: parsed.hostname,
      path:     `/rest/${token}/crm.company.list.json?${qs}`,
      method:   'GET',
      headers:  { 'Accept': 'application/json' },
    };
    const { status, body } = await httpsRequest(options, null);
    if (status !== 200) {
      let msg = body;
      try { msg = JSON.parse(body)?.error_description || body; } catch {}
      throw new Error(`Bitrix24 HTTP ${status}: ${msg}`);
    }
    return JSON.parse(body);
  }

  // ── Bitrix24 helper: fetch users (id → name) ─────────────
  async function bxFetchUsersMap(portal, token) {
    const parsed = new URL(portal);
    const map = {};
    let start = 0;
    for (let guard = 0; guard < 60; guard++) { // up to ~3000 users
      const options = {
        hostname: parsed.hostname,
        path:     `/rest/${token}/user.get.json?start=${start}&ADMIN_MODE=true`,
        method:   'GET',
        headers:  { 'Accept': 'application/json' },
      };
      const { status, body } = await httpsRequest(options, null);
      if (status !== 200) {
        let msg = body; try { msg = JSON.parse(body)?.error_description || body; } catch {}
        throw new Error(`Bitrix24 HTTP ${status}: ${msg}`);
      }
      const data = JSON.parse(body);
      const batch = data.result || [];
      for (const u of batch) {
        map[u.ID] = [u.NAME, u.LAST_NAME].filter(Boolean).join(' ').trim() || ('ID ' + u.ID);
      }
      if (!data.next || !batch.length) break;
      start = data.next;
    }
    return map;
  }

  const _bxSync = { running: false, total: 0, fetched: 0, saved: 0, error: null, startedAt: null, finishedAt: null, mode: '' };

  // ── GET /api/bitrix/test ─────────────────────────────────
  if (req.method === 'GET' && pathname === '/api/bitrix/test') {
    const portal = (req.headers['x-bx-portal'] || '').trim().replace(/\/$/, '');
    const token  = (req.headers['x-bx-token']  || '').trim();
    if (!portal || !token) return json(res, 400, { error: 'Missing x-bx-portal or x-bx-token' });
    try {
      const data = await bxFetchPage(portal, token, 0, null);
      const db   = getBxDb();
      const cached = db ? (db.prepare('SELECT COUNT(*) as c FROM deals').get()?.c || 0) : 0;
      return json(res, 200, { ok: true, total: data.total || 0, cached });
    } catch(e) {
      return json(res, 502, { error: e.message });
    }
  }

  // ── GET /api/bitrix/companies?assigned=<id> ──────────────
  //  Companies (portfolio) of a given manager by ASSIGNED_BY_ID.
  //  Without ?assigned= returns the first page of all companies.
  //  Paginates through Bitrix (50/page) up to a safety cap.
  if (req.method === 'GET' && pathname === '/api/bitrix/companies') {
    const portal = (req.headers['x-bx-portal'] || '').trim().replace(/\/$/, '');
    const token  = (req.headers['x-bx-token']  || '').trim();
    if (!portal || !token) return json(res, 400, { error: 'Missing x-bx-portal or x-bx-token' });

    const u = new URL(req.url, `http://localhost:${PORT}`);
    const assigned = (u.searchParams.get('assigned') || '').trim();
    const countOnly = u.searchParams.get('count_only') === 'true';

    const filter = assigned ? { 'ASSIGNED_BY_ID': assigned } : null;

    try {
      // First page also returns total
      const first = await bxFetchCompanyPage(portal, token, 0, filter);
      const total = first.total || 0;

      if (countOnly) {
        return json(res, 200, { total, assigned: assigned || null });
      }

      let companies = first.result || [];
      // paginate remaining pages, capped to avoid runaway requests
      const PAGE = 50, MAX_COMPANIES = 1000;
      let start = PAGE;
      while (companies.length < total && companies.length < MAX_COMPANIES && first.next) {
        const pageData = await bxFetchCompanyPage(portal, token, start, filter);
        const batch = pageData.result || [];
        if (!batch.length) break;
        companies = companies.concat(batch);
        start += PAGE;
        if (!pageData.next) break;
      }

      // resolve manager names (id → "Имя Фамилия")
      let usersMap = {};
      try { usersMap = await bxFetchUsersMap(portal, token); }
      catch(e) { console.error('[BX users]', e.message); }

      return json(res, 200, {
        total,
        returned: companies.length,
        capped: companies.length >= MAX_COMPANIES && total > MAX_COMPANIES,
        assigned: assigned || null,
        manager_name: assigned ? (usersMap[assigned] || null) : null,
        companies: companies.map(c => ({
          id: c.ID,
          title: c.TITLE,
          assigned_by: c.ASSIGNED_BY_ID,
          manager: usersMap[c.ASSIGNED_BY_ID] || ('ID ' + c.ASSIGNED_BY_ID),
          date_create: c.DATE_CREATE,
        })),
      });
    } catch(e) {
      return json(res, 502, { error: e.message });
    }
  }

  // ── GET /api/bitrix/company-search?q=<text> ──────────────
  //  Search companies by title (substring). Returns up to 30 with manager name.
  if (req.method === 'GET' && pathname === '/api/bitrix/company-search') {
    const portal = (req.headers['x-bx-portal'] || '').trim().replace(/\/$/, '');
    const token  = (req.headers['x-bx-token']  || '').trim();
    if (!portal || !token) return json(res, 400, { error: 'Missing x-bx-portal or x-bx-token' });

    const u = new URL(req.url, `http://localhost:${PORT}`);
    const q = (u.searchParams.get('q') || '').trim();
    if (!q) return json(res, 400, { error: 'q (search text) required' });

    try {
      // Bitrix substring filter: filter[%TITLE]=text
      const first = await bxFetchCompanyPage(portal, token, 0, { '%TITLE': q });
      const companies = (first.result || []).slice(0, 30);

      let usersMap = {};
      try { usersMap = await bxFetchUsersMap(portal, token); }
      catch(e) { console.error('[BX users]', e.message); }

      return json(res, 200, {
        query: q,
        total: first.total || companies.length,
        companies: companies.map(c => ({
          id: c.ID,
          title: c.TITLE,
          bin: '',  // BIN lives in a UF_CRM_* field — wire up once field name is known
          assigned_by: c.ASSIGNED_BY_ID,
          manager: usersMap[c.ASSIGNED_BY_ID] || ('ID ' + c.ASSIGNED_BY_ID),
        })),
      });
    } catch(e) {
      return json(res, 502, { error: e.message });
    }
  }

  // ── GET /api/bitrix/company-detail?id=<id> ───────────────
  //  Full fields of one company via crm.company.get: phones, emails,
  //  industry, address. Also resolves manager name and lists UF_ fields
  //  (so we can later spot the BIN field).
  if (req.method === 'GET' && pathname === '/api/bitrix/company-detail') {
    const portal = (req.headers['x-bx-portal'] || '').trim().replace(/\/$/, '');
    const token  = (req.headers['x-bx-token']  || '').trim();
    if (!portal || !token) return json(res, 400, { error: 'Missing x-bx-portal or x-bx-token' });

    const u = new URL(req.url, `http://localhost:${PORT}`);
    const id = (u.searchParams.get('id') || '').trim();
    if (!id) return json(res, 400, { error: 'id (company id) required' });

    try {
      const parsed = new URL(portal);
      const options = {
        hostname: parsed.hostname,
        path:     `/rest/${token}/crm.company.get.json?id=${encodeURIComponent(id)}`,
        method:   'GET',
        headers:  { 'Accept': 'application/json' },
      };
      const { status, body } = await httpsRequest(options, null);
      if (status !== 200) {
        let msg = body; try { msg = JSON.parse(body)?.error_description || body; } catch {}
        return json(res, 502, { error: `Bitrix24 HTTP ${status}: ${msg}` });
      }
      const c = JSON.parse(body).result || {};

      // multi-fields: PHONE / EMAIL are arrays of { VALUE, VALUE_TYPE }
      const firstVal = (arr) => Array.isArray(arr) && arr.length ? (arr[0].VALUE || '') : '';
      const phone = firstVal(c.PHONE);
      const email = firstVal(c.EMAIL);

      // industry comes as a code (e.g. "IT", "MANUFACTURING"); map common ones
      const INDUSTRY = {
        IT:'IT', TELECOM:'Телеком', MANUFACTURING:'Производство', BANKING:'Банки',
        CONSULTING:'Консалтинг', FINANCE:'Финансы', GOVERNMENT:'Госсектор',
        DELIVERY:'Доставка', ENTERTAINMENT:'Развлечения', NOTPROFIT:'НКО',
        TRADE:'Торговля', BUILDING:'Строительство', EDUCATION:'Образование',
        MEDICINE:'Медицина', LAW:'Юр. услуги',
      };
      const industry = INDUSTRY[c.INDUSTRY] || c.INDUSTRY || '';

      // address: collect city/region if present
      const city = c.ADDRESS_CITY || c.REG_ADDRESS_CITY || '';

      // collect UF_ fields (so BIN can be spotted later)
      const ufFields = {};
      Object.keys(c).forEach(k => { if (k.startsWith('UF_')) ufFields[k] = c[k]; });

      return json(res, 200, {
        id: c.ID,
        title: c.TITLE,
        phone, email, industry, city,
        company_type: c.COMPANY_TYPE || '',
        revenue_field: c.REVENUE || '',
        assigned_by: c.ASSIGNED_BY_ID,
        date_create: c.DATE_CREATE,
        uf_fields: ufFields,   // for spotting BIN field name
      });
    } catch(e) {
      return json(res, 502, { error: e.message });
    }
  }

  // ── GET /api/bitrix/revenue?assigned=<id>[&from=YYYY-MM-DD&to=YYYY-MM-DD] ──
  //  Sum of OPPORTUNITY for a manager's deals that are CLOSED WON
  //  (STAGE_SEMANTIC_ID = 'S') with CLOSEDATE in the period.
  //  Defaults to the current calendar month.
  if (req.method === 'GET' && pathname === '/api/bitrix/revenue') {
    const portal = (req.headers['x-bx-portal'] || '').trim().replace(/\/$/, '');
    const token  = (req.headers['x-bx-token']  || '').trim();
    if (!portal || !token) return json(res, 400, { error: 'Missing x-bx-portal or x-bx-token' });

    const u = new URL(req.url, `http://localhost:${PORT}`);
    const assigned = (u.searchParams.get('assigned') || '').trim();
    if (!assigned) return json(res, 400, { error: 'assigned (manager id) required' });

    // default period = current month
    const now = new Date();
    const pad = n => String(n).padStart(2, '0');
    const defFrom = `${now.getFullYear()}-${pad(now.getMonth()+1)}-01`;
    const lastDay = new Date(now.getFullYear(), now.getMonth()+1, 0).getDate();
    const defTo   = `${now.getFullYear()}-${pad(now.getMonth()+1)}-${pad(lastDay)}`;
    const from = (u.searchParams.get('from') || defFrom).trim();
    const to   = (u.searchParams.get('to')   || defTo).trim();

    const filter = {
      'ASSIGNED_BY_ID': assigned,
      'STAGE_SEMANTIC_ID': 'S',          // closed-won only
      '>=CLOSEDATE': from,
      '<=CLOSEDATE': to + ' 23:59:59',
    };

    try {
      let total = 0, count = 0;
      const byCompany = {};   // company_id → revenue sum
      let start = 0;
      const PAGE = 50, MAX_PAGES = 60; // up to 3000 deals
      for (let p = 0; p < MAX_PAGES; p++) {
        const data = await bxFetchPage(portal, token, start, filter);
        const batch = data.result || [];
        for (const d of batch) {
          const amt = parseFloat(d.OPPORTUNITY) || 0;
          total += amt;
          count++;
          const cid = d.COMPANY_ID;
          if (cid && cid !== '0') byCompany[cid] = (byCompany[cid] || 0) + amt;
        }
        if (!data.next || !batch.length) break;
        start = data.next;
      }
      return json(res, 200, {
        assigned, from, to,
        revenue: Math.round(total),
        deals_count: count,
        by_company: byCompany,   // { "39881": 545037, ... }
      });
    } catch(e) {
      return json(res, 502, { error: e.message });
    }
  }

  if (req.method === 'GET' && pathname === '/api/bitrix/sync-status') {
    const db = getBxDb();
    const cached = db ? (db.prepare('SELECT COUNT(*) as c FROM deals').get()?.c || 0) : 0;
    const lastSync = db ? (db.prepare("SELECT value FROM sync_meta WHERE key='last_sync'").get()?.value || null) : null;
    return json(res, 200, { ..._bxSync, cached, lastSync });
  }

  // ── POST /api/bitrix/sync ────────────────────────────────
  if (req.method === 'POST' && pathname === '/api/bitrix/sync') {
    const portal = (req.headers['x-bx-portal'] || '').trim().replace(/\/$/, '');
    const token  = (req.headers['x-bx-token']  || '').trim();
    if (!portal || !token) return json(res, 400, { error: 'Missing x-bx-portal or x-bx-token' });

    const db = getBxDb();
    if (!db) return json(res, 503, { error: 'SQLite not available. Run: npm install better-sqlite3' });

    if (_bxSync.running) return json(res, 409, { error: 'Sync already running', progress: _bxSync });

    let body = '';
    try { body = await readBody(req); } catch {}
    let opts = {};
    try { opts = JSON.parse(body); } catch {}
    const mode = opts.mode || 'full'; // 'full' | 'delta'

    // Build filter
    let filter = null;
    if (mode === 'delta') {
      // Only fetch deals modified since last sync
      const lastSync = db.prepare("SELECT value FROM sync_meta WHERE key='last_sync'").get()?.value;
      if (lastSync) {
        filter = { '>=DATE_MODIFY': lastSync };
      }
    }

    // Start async sync, respond immediately
    _bxSync.running   = true;
    _bxSync.total     = 0;
    _bxSync.fetched   = 0;
    _bxSync.saved     = 0;
    _bxSync.error     = null;
    _bxSync.startedAt = new Date().toISOString();
    _bxSync.finishedAt= null;
    _bxSync.mode      = mode;

    json(res, 202, { ok: true, message: 'Sync started', mode });

    // Run sync in background
    (async () => {
      try {
        const upsert = db.prepare(`
          INSERT INTO deals (id,title,type_id,stage_id,stage_semantic,opportunity,currency,
            assigned_by,closedate,date_create,date_modify,last_activity,last_comm,
            source_id,category_id,is_closed,probability,raw,synced_at)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(id) DO UPDATE SET
            title=excluded.title, type_id=excluded.type_id, stage_id=excluded.stage_id,
            stage_semantic=excluded.stage_semantic, opportunity=excluded.opportunity,
            currency=excluded.currency, assigned_by=excluded.assigned_by,
            closedate=excluded.closedate, date_modify=excluded.date_modify,
            last_activity=excluded.last_activity, last_comm=excluded.last_comm,
            source_id=excluded.source_id, category_id=excluded.category_id,
            is_closed=excluded.is_closed, probability=excluded.probability,
            raw=excluded.raw, synced_at=excluded.synced_at
        `);
        const upsertMany = db.transaction((rows) => {
          for (const d of rows) {
            upsert.run(
              d.ID, d.TITLE||'', d.TYPE_ID||'', d.STAGE_ID||'', d.STAGE_SEMANTIC_ID||'',
              parseFloat(d.OPPORTUNITY)||0, d.CURRENCY_ID||'KZT',
              d.ASSIGNED_BY_ID||'', d.CLOSEDATE||'', d.DATE_CREATE||'', d.DATE_MODIFY||'',
              d.LAST_ACTIVITY_TIME||'', d.LAST_COMMUNICATION_TIME||'',
              d.SOURCE_ID||'', d.CATEGORY_ID||'0', d.IS_CLOSED||'N',
              d.PROBABILITY||'', JSON.stringify(d), new Date().toISOString()
            );
          }
        });

        let start = 0;
        const pageSize = 50;
        let firstPage = true;

        while (true) {
          const data = await bxFetchPage(portal, token, start, filter);
          if (firstPage) {
            _bxSync.total = data.total || 0;
            firstPage = false;
          }
          const page = data.result || [];
          if (!page.length) break;

          upsertMany(page);
          _bxSync.fetched += page.length;
          _bxSync.saved   += page.length;

          console.log(`[Bitrix sync] ${_bxSync.fetched}/${_bxSync.total} deals`);

          // Bitrix returns `next` field when there are more pages
          if (!data.next && page.length < pageSize) break;
          start = data.next || (start + pageSize);

          // Small pause to be polite to Bitrix API (2 req/sec)
          await sleep(500);
        }

        // Save last sync timestamp
        db.prepare("INSERT OR REPLACE INTO sync_meta VALUES ('last_sync', ?)").run(new Date().toISOString());
        _bxSync.running    = false;
        _bxSync.finishedAt = new Date().toISOString();
        console.log(`[Bitrix sync] Done: ${_bxSync.saved} deals saved`);
      } catch(e) {
        _bxSync.running = false;
        _bxSync.error   = e.message;
        console.error('[Bitrix sync error]', e.message);
      }
    })();

    return; // already responded with 202
  }

  // ── GET /api/bitrix/deals — read from local SQLite ───────
  if (req.method === 'GET' && pathname === '/api/bitrix/deals') {
    const db = getBxDb();
    if (!db) return json(res, 503, { error: 'SQLite not available. Run: npm install better-sqlite3' });

    const u        = new URL(req.url, `http://localhost:${PORT}`);
    const search   = (u.searchParams.get('search')   || '').toLowerCase();
    const category = u.searchParams.get('category')  || '';
    const onlyOpen = u.searchParams.get('open')      !== 'false'; // default: only open deals

    let query = 'SELECT * FROM deals WHERE 1=1';
    const params = [];
    if (onlyOpen)  { query += " AND is_closed = 'N'"; }
    if (category)  { query += ' AND category_id = ?'; params.push(category); }
    if (search)    { query += ' AND lower(title) LIKE ?'; params.push(`%${search}%`); }
    query += ' ORDER BY date_modify DESC';

    try {
      const deals = db.prepare(query).all(...params);
      const total = db.prepare('SELECT COUNT(*) as c FROM deals').get()?.c || 0;
      const lastSync = db.prepare("SELECT value FROM sync_meta WHERE key='last_sync'").get()?.value || null;
      return json(res, 200, { deals, total_db: total, filtered: deals.length, lastSync });
    } catch(e) {
      return json(res, 500, { error: e.message });
    }
  }


});

server.listen(PORT, '0.0.0.0', () => {
  console.log('');
  console.log('  ╔══════════════════════════════════════════╗');
  console.log('  ║  Omnidesk · AI Analytics — Proxy Server  ║');
  console.log('  ╠══════════════════════════════════════════╣');
  console.log(`  ║  Running at  http://localhost:${PORT}        ║`);
  console.log('  ║                                          ║');
  console.log('  ║  Open that URL in your browser.          ║');
  console.log('  ║  Press Ctrl+C to stop.                   ║');
  console.log('  ╚══════════════════════════════════════════╝');
  console.log('');
});