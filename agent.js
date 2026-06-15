/**
 * Omnidesk · AI Agent
 * ───────────────────
 * Автономный агент который:
 *   1. Каждые N минут проверяет новые кейсы в Omnidesk
 *   2. Классифицирует каждый через AI (тема, тональность, приоритет)
 *   3. Логирует решения в agent_memory.json
 *   4. Отправляет алерты в Telegram при критических кейсах
 *
 * Запускается автоматически из server.js при наличии agent_config.json
 */

const https  = require('https');
const fs     = require('fs');
const path   = require('path');

// ─── Пути к файлам ───────────────────────────────────────────
const CONFIG_PATH = path.join(__dirname, 'agent_config.json');
const MEMORY_PATH = path.join(__dirname, 'agent_memory.json');

// ─── Загрузка / сохранение памяти ────────────────────────────
function loadMemory() {
  try {
    if (fs.existsSync(MEMORY_PATH)) return JSON.parse(fs.readFileSync(MEMORY_PATH, 'utf8'));
  } catch(e) {}
  return {
    lastCheckedCaseId: 0,   // последний обработанный case_id
    lastCheckedAt: null,    // ISO timestamp последней проверки
    decisions: [],          // лог решений агента
    stats: { processed: 0, alerted: 0, errors: 0 },
    kpis: { negPct: 0, aggPct: 0, totalProcessed: 0 },
  };
}

function saveMemory(mem) {
  try {
    // Держим только последние 500 решений чтобы файл не разбухал
    if (mem.decisions.length > 500) mem.decisions = mem.decisions.slice(-500);
    fs.writeFileSync(MEMORY_PATH, JSON.stringify(mem, null, 2));
  } catch(e) { console.error('[Agent] Ошибка сохранения памяти:', e.message); }
}

// ─── HTTP helpers ─────────────────────────────────────────────
function httpsPost(options, body) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

function httpsGet(options) {
  return new Promise((resolve, reject) => {
    const req = https.request({ ...options, method: 'GET' }, res => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
      res.on('error', reject);
    });
    req.on('error', reject);
    req.end();
  });
}

// ─── Omnidesk API ─────────────────────────────────────────────
async function omnideskGet(config, endpoint) {
  const credentials = Buffer.from(config.od_email + ':' + config.od_key).toString('base64');
  const cleanDomain = config.od_domain.replace(/^https?:\/\//, '').replace(/\/$/, '');
  let ep = endpoint;
  if (!ep.endsWith('.json')) ep = ep + '.json';
  const { status, body } = await httpsGet({
    hostname: cleanDomain,
    path: '/api/' + ep,
    headers: {
      'Authorization': 'Basic ' + credentials,
      'Content-Type': 'application/json',
    },
  });
  if (status !== 200) throw new Error(`Omnidesk ${endpoint}: HTTP ${status}`);
  return JSON.parse(body);
}

// ─── OpenRouter / AI ──────────────────────────────────────────
async function classifyCase(config, caseObj) {
  const subject = caseObj.subject || '(без темы)';
  const prompt = `Классифицируй обращение в службу поддержки. Отвечай ТОЛЬКО JSON без лишнего текста.

Обращение: "${subject}"

Верни JSON:
{
  "topic": "краткая тема (2-4 слова)",
  "sentiment": "pos|neu|neg|agg",
  "priority": "низкий|средний|высокий|критический",
  "reason": "одно предложение почему такой приоритет"
}`;

  const body = JSON.stringify({
    model: config.ai_model || 'anthropic/claude-haiku',
    max_tokens: 200,
    messages: [{ role: 'user', content: prompt }],
  });

  const { status, body: resp } = await httpsPost({
    hostname: 'openrouter.ai',
    path: '/api/v1/chat/completions',
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'Bearer ' + config.ai_key,
      'HTTP-Referer': 'http://localhost:3000',
      'X-Title': 'Omnidesk AI Agent',
    },
  }, body);

  if (status !== 200) throw new Error(`AI API: HTTP ${status}`);
  const data = JSON.parse(resp);
  const text = data.choices?.[0]?.message?.content || '{}';
  const clean = text.replace(/```json|```/g, '').trim();
  return JSON.parse(clean);
}

// ─── Telegram ─────────────────────────────────────────────────
async function sendTelegram(config, message) {
  if (!config.telegram_bot_token || !config.telegram_chat_id) return;
  const body = JSON.stringify({
    chat_id: config.telegram_chat_id,
    text: message,
    parse_mode: 'HTML',
  });
  try {
    await httpsPost({
      hostname: 'api.telegram.org',
      path: `/bot${config.telegram_bot_token}/sendMessage`,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    }, body);
  } catch(e) {
    console.warn('[Agent] Telegram ошибка:', e.message);
  }
}

// ─── Форматирование алерта ────────────────────────────────────
function formatAlert(caseObj, analysis) {
  const emojiMap = { pos:'😊', neu:'😐', neg:'😟', agg:'😡' };
  const prioEmoji = { низкий:'🟢', средний:'🟡', высокий:'🔴', критический:'🚨' };
  const emoji = emojiMap[analysis.sentiment] || '😐';
  const pEmoji = prioEmoji[analysis.priority] || '🟡';

  return `${pEmoji} <b>Новое обращение #${caseObj.case_id}</b>

${emoji} <b>Тема:</b> ${analysis.topic}
<b>Тональность:</b> ${analysis.sentiment}
<b>Приоритет:</b> ${analysis.priority}

<b>Суть:</b> ${(caseObj.subject || '').slice(0, 200)}

<i>${analysis.reason}</i>`;
}

// ─── Основной тик агента ──────────────────────────────────────
async function agentTick(config, memory) {
  console.log(`[Agent] Тик: ${new Date().toLocaleTimeString('ru-RU')} · последний ID: ${memory.lastCheckedCaseId}`);

  // Грузим новые кейсы (сортировка по дате создания, новые первые)
  let newCases = [];
  try {
    const data = await omnideskGet(config, 'cases?limit=50&sort=case_id_desc');
    const entries = Object.values(data)
      .filter(v => v && typeof v === 'object' && v.case && v.case.case_id)
      .map(v => v.case);

    // Берём только те что новее lastCheckedCaseId
    newCases = entries.filter(c => c.case_id > memory.lastCheckedCaseId);
  } catch(e) {
    console.error('[Agent] Ошибка загрузки кейсов:', e.message);
    memory.stats.errors++;
    return;
  }

  if (newCases.length === 0) {
    console.log('[Agent] Новых кейсов нет');
    return;
  }

  console.log(`[Agent] Найдено новых кейсов: ${newCases.length}`);

  // Обрабатываем каждый новый кейс
  for (const c of newCases) {
    try {
      // AI классификация
      const analysis = await classifyCase(config, c);
      console.log(`[Agent] #${c.case_id} → ${analysis.sentiment} · ${analysis.priority} · ${analysis.topic}`);

      // Логируем решение в память
      const decision = {
        ts:        new Date().toISOString(),
        caseId:    c.case_id,
        subject:   (c.subject || '').slice(0, 100),
        sentiment: analysis.sentiment,
        priority:  analysis.priority,
        topic:     analysis.topic,
        reason:    analysis.reason,
        alerted:   false,
      };

      // Обновляем KPI
      memory.stats.processed++;
      memory.kpis.totalProcessed++;
      const total = memory.kpis.totalProcessed;
      // Скользящий процент негатива и агрессии (по последним 500 решениям)
      const recent = [...memory.decisions.slice(-499), decision];
      memory.kpis.negPct = Math.round(recent.filter(d => d.sentiment === 'neg').length / recent.length * 100);
      memory.kpis.aggPct = Math.round(recent.filter(d => d.sentiment === 'agg').length / recent.length * 100);

      // Алерт если высокий/критический приоритет или агрессия
      const needsAlert = analysis.priority === 'критический'
        || analysis.priority === 'высокий'
        || analysis.sentiment === 'agg';

      if (needsAlert) {
        await sendTelegram(config, formatAlert(c, analysis));
        decision.alerted = true;
        memory.stats.alerted++;
        console.log(`[Agent] 🚨 Алерт отправлен для #${c.case_id}`);
      }

      // KPI алерт: если негатив > порога
      const negThreshold = config.neg_threshold || 20;
      if (memory.kpis.negPct > negThreshold && total % 50 === 0) {
        await sendTelegram(config,
          `⚠️ <b>KPI Alert</b>\n\nНегатив достиг <b>${memory.kpis.negPct}%</b> (порог: ${negThreshold}%)\nОбработано обращений: ${total}`
        );
      }

      memory.decisions.push(decision);

      // Пауза между запросами к AI чтобы не упереться в rate limit
      await new Promise(r => setTimeout(r, 500));

    } catch(e) {
      console.error(`[Agent] Ошибка обработки кейса #${c.case_id}:`, e.message);
      memory.stats.errors++;
    }
  }

  // Обновляем lastCheckedCaseId до максимального из новых
  const maxId = Math.max(...newCases.map(c => c.case_id));
  if (maxId > memory.lastCheckedCaseId) memory.lastCheckedCaseId = maxId;
  memory.lastCheckedAt = new Date().toISOString();
}

// ─── Запуск агента ────────────────────────────────────────────
function startAgent() {
  if (!fs.existsSync(CONFIG_PATH)) {
    console.log('[Agent] agent_config.json не найден — агент не запущен');
    console.log('[Agent] Настройте агента через дашборд (вкладка Настройка → Агент)');
    return null;
  }

  let config;
  try {
    config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
  } catch(e) {
    console.error('[Agent] Ошибка чтения agent_config.json:', e.message);
    return null;
  }

  if (!config.od_domain || !config.od_email || !config.od_key || !config.ai_key) {
    console.log('[Agent] Неполная конфигурация — агент не запущен');
    return null;
  }

  const intervalMin = config.interval_minutes || 5;
  const memory = loadMemory();

  console.log('');
  console.log('  ╔══════════════════════════════════════════╗');
  console.log('  ║         Omnidesk · AI Agent              ║');
  console.log('  ╠══════════════════════════════════════════╣');
  console.log(`  ║  Интервал: каждые ${intervalMin} мин               ║`);
  console.log(`  ║  Telegram: ${config.telegram_bot_token ? 'подключён ✓' : 'не настроен'}              ║`);
  console.log(`  ║  Последний ID: ${memory.lastCheckedCaseId}                   ║`);
  console.log('  ╚══════════════════════════════════════════╝');
  console.log('');

  // Первый тик сразу при старте
  agentTick(config, memory)
    .then(() => saveMemory(memory))
    .catch(e => console.error('[Agent]', e.message));

  // Повторяем каждые N минут
  const interval = setInterval(async () => {
    // Перечитываем конфиг при каждом тике — чтобы изменения из UI применялись без рестарта
    try {
      const fresh = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8'));
      Object.assign(config, fresh);
    } catch(e) {}

    try {
      await agentTick(config, memory);
      saveMemory(memory);
    } catch(e) {
      console.error('[Agent] Ошибка тика:', e.message);
    }
  }, intervalMin * 60 * 1000);

  return interval;
}

// ─── API endpoints для дашборда ───────────────────────────────
// Возвращает статус агента и последние решения
function getAgentStatus() {
  const memory = loadMemory();
  const configExists = fs.existsSync(CONFIG_PATH);
  let config = {};
  if (configExists) {
    try { config = JSON.parse(fs.readFileSync(CONFIG_PATH, 'utf8')); } catch(e) {}
  }
  return {
    running:      configExists && !!config.od_domain,
    lastCheckedAt: memory.lastCheckedAt,
    lastCheckedCaseId: memory.lastCheckedCaseId,
    stats:        memory.stats,
    kpis:         memory.kpis,
    recentDecisions: memory.decisions.slice(-20).reverse(), // последние 20
    config: {
      interval_minutes: config.interval_minutes || 5,
      neg_threshold:    config.neg_threshold || 20,
      ai_model:         config.ai_model || 'anthropic/claude-haiku',
      telegram_configured: !!(config.telegram_bot_token && config.telegram_chat_id),
    },
  };
}

function saveConfig(newConfig) {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(newConfig, null, 2));
}

module.exports = { startAgent, getAgentStatus, saveConfig, loadMemory };
