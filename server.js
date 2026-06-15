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

let pgPool = null, pgPoolKey = '';
function getPgPool(host, port, database, user, password, ssl) {
  const key = `${host}:${port}:${database}:${user}`;
  if (pgPool && pgPoolKey === key) return pgPool;
  if (pgPool) pgPool.end().catch(() => {});
  pgPool = new Pool({
    host, port: parseInt(port)||5432, database, user, password,
    ssl: ssl === 'true' ? { rejectUnauthorized: false } : false,
    connectionTimeoutMillis: 8000, max: 3,
    // Устанавливаем search_path чтобы schema contract была доступна по умолчанию
    options: '--search_path=contract,public',
  });
  pgPoolKey = key;
  return pgPool;
}

const PORT = 3000;

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
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type,Authorization,x-api-key,anthropic-version,x-omnidesk-domain,x-omnidesk-email,x-omnidesk-key,x-pg-host,x-pg-port,x-pg-database,x-pg-user,x-pg-password,x-pg-ssl');
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


});

server.listen(PORT, () => {
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