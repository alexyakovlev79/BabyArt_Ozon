/**
 * BabyArt — Ozon Performance API — CPC (v0.4)
 *
 * Фиксы v0.4:
 * - Удалён BAOZ_CPC_ResetCursor (костыль).
 * - Дополнительно подтягиваем кампанийную статистику (показы/клики/CTR/средняя стоимость/заказы/DRR и т.д.)
 *   через JSON: /api/client/statistics/campaign/product/json
 * - Перед загрузкой товаров по кампании проверяем расход за период через
 *   /api/client/statistics/expense/json. Если суммарный расход = 0 — товары по кампании не грузим.
 *
 * Было в v0.2:
 * - Пагинация кампаний и товаров.
 * - Докачка порциями с курсором (не упираемся в 6 минут).
 * - SKU пишем текстом.
 * - Артикул заполняем по словарю из "Комиссии Озон" (A=Артикул, B=SKU) — без формул.
 */

const BAOZ_CPC_PERF_HOST = 'https://api-performance.ozon.ru';

const BAOZ_CPC_SHEET_OUT = 'Реклама за клики';
const BAOZ_CPC_SHEET_SETTINGS = 'Настройки';
const BAOZ_CPC_SHEET_COMMISSIONS = 'Комиссии Озон';

// Интервалы: L13:M (L=12, M=13)
const BAOZ_CPC_SETTINGS_FIRST_ROW = 13;
const BAOZ_CPC_SETTINGS_COL_FROM = 12; // L
const BAOZ_CPC_SETTINGS_COL_TO = 13;   // M

// Курсор/прогресс
const BAOZ_CPC_CURSOR_PROP = 'BAOZ_CPC_CURSOR_V1';

// Тайм-бюджет: выходим заранее, чтобы успеть записать данные и сохранить курсор
const BAOZ_CPC_TIME_BUDGET_MS = 330000; // ~5.5 минут
const BAOZ_CPC_SOFT_STOP_MS = 30000;    // стоп за 30 секунд до лимита

// API paging
const BAOZ_CPC_CAMPAIGNS_PAGE_SIZE = 100;
const BAOZ_CPC_PRODUCTS_PAGE_SIZE = 1000;
const BAOZ_CPC_MAX_PRODUCTS_PAGES_GUARD = 300; // предохранитель от вечных циклов

// Батч записи в лист (чтобы не держать гигантский массив в памяти)
const BAOZ_CPC_WRITE_BATCH = 2000;

const BAOZ_CPC_HEADERS = [
  'Период_с', 'Период_по', 'ID_РК', 'Название_РК',
  'SKU', 'Артикул', 'Название товара', 'Цена товара, р',
  'Показы', 'Клики', 'CTR, %', 'В корзину',
  'Средняя стоимость', 'Расход, р. с НДС', 'Заказы', 'Продажи, р',
  'Заказы модели', 'Продажи с заказа модели, р', 'DRR, %'
];

function BAOZ_CPC_Sync() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(15000)) throw new Error('CPC: не получил lock (возможно, параллельный запуск).');

  const startedAt = Date.now();
  const ss = SpreadsheetApp.getActive();

  try {
    const shOut = ss.getSheetByName(BAOZ_CPC_SHEET_OUT) || ss.insertSheet(BAOZ_CPC_SHEET_OUT);
    const shSet = ss.getSheetByName(BAOZ_CPC_SHEET_SETTINGS);
    if (!shSet) throw new Error('Нет листа "Настройки".');

    BAOZ_CPC_ensureHeader_(shOut);
    BAOZ_CPC_prepareFormats_(shOut);

    const intervals = BAOZ_CPC_readIntervals_(shSet);
    if (!intervals.length) throw new Error('Не нашла ни одного интервала в "Настройки"!L13:M.');

    let cursor = BAOZ_CPC_readCursor_();
    if (!cursor) cursor = { intervalIdx: 0, campaignIdx: 0, productPage: 1, outRow: 2, cleared: false };

    // 1) Токен
    const token = BAOZ_CPC_getToken_();

    // 2) Кампании (все страницы)
    const campaigns = BAOZ_CPC_listCpcCampaignsAll_(token);
    if (!campaigns.length) {
      throw new Error('Не нашла CPC-кампаний. Проверь PERF-ключи и что CPC вообще есть в кабинете.');
    }

    // 3) Словарь SKU -> Артикул (Комиссии Озон: A=Артикул, B=SKU)
    const sku2art = BAOZ_CPC_buildSkuToArticleMap_(ss);

    // 4) Если стартуем сначала — чистим вывод один раз
    if (!cursor.cleared) {
      const lastRow = shOut.getLastRow();
      if (lastRow > 1) shOut.getRange(2, 1, lastRow - 1, BAOZ_CPC_HEADERS.length).clearContent();
      cursor.outRow = 2;
      cursor.cleared = true;
    }

    const rowsBuf = [];
    const flush_ = () => {
      if (!rowsBuf.length) return;
      shOut.getRange(cursor.outRow, 1, rowsBuf.length, BAOZ_CPC_HEADERS.length).setValues(rowsBuf);
      cursor.outRow += rowsBuf.length;
      rowsBuf.length = 0;
    };

    // 5) Основной цикл с возможностью “паузы” по тайм-ауту
    for (let i = cursor.intervalIdx; i < intervals.length; i++) {
      const it = intervals[i];
      const spendMap = BAOZ_CPC_getCpcExpenseMap_(token, it.from, it.to); // null если не смогли получить
      const statsMap = BAOZ_CPC_getCpcCampaignStatsMap_(token, it.from, it.to);


      const cStart = (i === cursor.intervalIdx) ? cursor.campaignIdx : 0;
      for (let c = cStart; c < campaigns.length; c++) {
        const camp = campaigns[c];

        // === v0.3: сначала проверяем расход кампании за период ===
        let totalExpense = null;
        if (spendMap) {
          totalExpense = BAOZ_CPC_toNumber_(spendMap[String(camp.id)]);
          if (totalExpense === 0) {
            cursor.productPage = 1;
            continue;
          }
        }

        const st = (statsMap && statsMap[String(camp.id)]) ? statsMap[String(camp.id)] : null;

        // Если totalExpense === null (не смогли разобрать ответ/параметры) — НЕ фильтруем, идём дальше как раньше.

        let page = (i === cursor.intervalIdx && c === cursor.campaignIdx) ? cursor.productPage : 1;

        for (; page <= BAOZ_CPC_MAX_PRODUCTS_PAGES_GUARD; page++) {
          if (Date.now() - startedAt > (BAOZ_CPC_TIME_BUDGET_MS - BAOZ_CPC_SOFT_STOP_MS)) {
            cursor.intervalIdx = i;
            cursor.campaignIdx = c;
            cursor.productPage = page;
            flush_();
            BAOZ_CPC_saveCursor_(cursor);
            ss.toast(
              `CPC: пауза по лимиту времени. Продолжу: интервал ${i + 1}/${intervals.length}, кампания ${c + 1}/${campaigns.length}, page ${page}.`,
              'CPC',
              5
            );
            return;
          }

          const resp = BAOZ_CPC_httpJson_(
            'get',
            `/api/client/campaign/${encodeURIComponent(String(camp.id))}/v2/products?page=${page}&pageSize=${BAOZ_CPC_PRODUCTS_PAGE_SIZE}`,
            null,
            token
          );

          const products = BAOZ_CPC_extractProducts_(resp);
          if (!products.length) break;

          for (const p of products) {
            const skuKey = BAOZ_CPC_normSkuKey_(p.sku);

                        const row = new Array(BAOZ_CPC_HEADERS.length).fill('');
            row[0] = it.from;
            row[1] = it.to;
            row[2] = String(camp.id || '');
            row[3] = String(camp.title || '');
            row[4] = skuKey;                 // SKU (как текст)
            row[5] = sku2art[skuKey] || '';   // Артикул
            row[6] = String(p.title || '');

            // статистика (JSON /statistics/campaign/product/json) — на уровне кампании
            if (st) {
              row[8]  = (st.views === 0 || st.views) ? st.views : '';
              row[9]  = (st.clicks === 0 || st.clicks) ? st.clicks : '';
              row[10] = (st.ctr === 0 || st.ctr) ? st.ctr : '';
              row[11] = (st.toCart === 0 || st.toCart) ? st.toCart : '';
              row[12] = (st.clickPrice === 0 || st.clickPrice) ? st.clickPrice : '';
              row[13] = (st.moneySpent === 0 || st.moneySpent) ? st.moneySpent : ((totalExpense === 0 || totalExpense) ? totalExpense : '');
              row[14] = (st.orders === 0 || st.orders) ? st.orders : '';
              row[15] = (st.ordersMoney === 0 || st.ordersMoney) ? st.ordersMoney : '';
              row[18] = (st.drr === 0 || st.drr) ? st.drr : '';
            } else {
              // хотя бы расход из /statistics/expense
              row[13] = (totalExpense === 0 || totalExpense) ? totalExpense : '';
            }

            rowsBuf.push(row);
            if (rowsBuf.length >= BAOZ_CPC_WRITE_BATCH) flush_();
          }

          if (products.length < BAOZ_CPC_PRODUCTS_PAGE_SIZE) break;
        }

        // закончили кампанию
        cursor.productPage = 1;
      }

      // закончили интервал
      cursor.campaignIdx = 0;
    }

    // Всё выгрузили
    flush_();
    PropertiesService.getScriptProperties().deleteProperty(BAOZ_CPC_CURSOR_PROP);
    ss.toast(`CPC: готово. Записано строк: ${Math.max(0, cursor.outRow - 2)}.`, 'CPC', 5);

  } finally {
    lock.releaseLock();
  }
}

/** ===== Sheet helpers ===== */

function BAOZ_CPC_ensureHeader_(sh) {
  const cur = sh.getRange(1, 1, 1, BAOZ_CPC_HEADERS.length).getValues()[0];
  const curJoined = cur.map(x => String(x || '').trim()).join('|');
  const needJoined = BAOZ_CPC_HEADERS.join('|');
  if (curJoined !== needJoined) {
    sh.getRange(1, 1, 1, BAOZ_CPC_HEADERS.length).setValues([BAOZ_CPC_HEADERS]);
  }
}

function BAOZ_CPC_prepareFormats_(shOut) {
  // SKU и Артикул как текст
  shOut.getRange('E:F').setNumberFormat('@');
}

function BAOZ_CPC_readIntervals_(shSet) {
  const maxRows = 60;
  const rng = shSet.getRange(BAOZ_CPC_SETTINGS_FIRST_ROW, BAOZ_CPC_SETTINGS_COL_FROM, maxRows, 2);
  const vals = rng.getValues();

  const out = [];
  for (let i = 0; i < vals.length; i++) {
    const vFrom = vals[i][0];
    const vTo = vals[i][1];

    if (vFrom === '' || vFrom === null) break;

    const from = BAOZ_CPC_parseDate_(vFrom);
    const to = (vTo === '' || vTo === null) ? new Date() : BAOZ_CPC_parseDate_(vTo);
    out.push({ from, to });
  }
  return out;
}

function BAOZ_CPC_parseDate_(v) {
  if (v instanceof Date) return v;

  const s = String(v || '').trim();
  let m = s.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const dd = Number(m[1]), mm = Number(m[2]), yyyy = Number(m[3]);
    const HH = Number(m[4] || 0), MI = Number(m[5] || 0), SS = Number(m[6] || 0);
    return new Date(yyyy, mm - 1, dd, HH, MI, SS);
  }

  m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const yyyy = Number(m[1]), mm = Number(m[2]), dd = Number(m[3]);
    const HH = Number(m[4] || 0), MI = Number(m[5] || 0), SS = Number(m[6] || 0);
    return new Date(yyyy, mm - 1, dd, HH, MI, SS);
  }

  const d = new Date(s);
  if (!isNaN(d.getTime())) return d;

  throw new Error('Не смогла распознать дату: "' + s + '"');
}

/** ===== Cursor helpers ===== */

function BAOZ_CPC_readCursor_() {
  const s = PropertiesService.getScriptProperties().getProperty(BAOZ_CPC_CURSOR_PROP);
  if (!s) return null;
  try { return JSON.parse(s); } catch (_) { return null; }
}

function BAOZ_CPC_saveCursor_(cursor) {
  PropertiesService.getScriptProperties().setProperty(BAOZ_CPC_CURSOR_PROP, JSON.stringify(cursor));
}

/** ===== Commissions map: SKU -> Article ===== */

function BAOZ_CPC_buildSkuToArticleMap_(ss) {
  const sh = ss.getSheetByName(BAOZ_CPC_SHEET_COMMISSIONS);
  if (!sh) return {};

  const lastRow = sh.getLastRow();
  if (lastRow < 2) return {};

  // Читаем A:B
  const values = sh.getRange(2, 1, lastRow - 1, 2).getValues();
  const map = {};
  for (const [article, sku] of values) {
    const k = BAOZ_CPC_normSkuKey_(sku);
    if (!k) continue;
    if (map[k] === undefined || map[k] === '') map[k] = String(article || '');
  }
  return map;
}

function BAOZ_CPC_normSkuKey_(v) {
  if (v === null || v === undefined || v === '') return '';
  if (typeof v === 'number') return String(Math.trunc(v));
  return String(v).trim();
}

/** ===== Auth & API ===== */

function BAOZ_CPC_getToken_() {
  const props = PropertiesService.getScriptProperties().getProperties();

  const clientId = props['BABYART_OZON_PERF_CREDS_CLIENT_ID'] || '';
  const clientSecret =
    props['BABYART_OZON_PERF_CREDS_CLIENT_SECRET'] ||
    props['BABYART_OZON_PERF_CREDS_CLIENT_SEC'] ||
    '';

  if (!clientId || !clientSecret) {
    throw new Error('Нет client_id/client_secret в Script Properties. Жду BABYART_OZON_PERF_CREDS_CLIENT_ID и ...CLIENT_SECRET (или ...CLIENT_SEC).');
  }

  const cache = CacheService.getScriptCache();
  const cached = cache.get('BAOZ_CPC_TOKEN_JSON_V1');
  if (cached) {
    try {
      const obj = JSON.parse(cached);
      if (obj.access_token && obj.expires_at_ms && (Date.now() + 60000) < obj.expires_at_ms) {
        return obj.access_token;
      }
    } catch (_) {}
  }

  const tok = BAOZ_CPC_httpJson_('post', '/api/client/token', {
    client_id: clientId,
    client_secret: clientSecret,
    grant_type: 'client_credentials'
  }, null);

  const access = tok.access_token || '';
  const expiresIn = Number(tok.expires_in || 1800);
  if (!access) throw new Error('Токен не вернулся из /api/client/token: ' + JSON.stringify(tok).slice(0, 400));

  cache.put(
    'BAOZ_CPC_TOKEN_JSON_V1',
    JSON.stringify({ access_token: access, expires_at_ms: Date.now() + expiresIn * 1000 }),
    Math.max(60, Math.min(expiresIn, 21600))
  );

  return access;
}

function BAOZ_CPC_listCpcCampaignsAll_(token) {
  // 1) пробуем с advObjectType=SKU; если пусто — fallback без фильтра
  let campaigns = BAOZ_CPC_listCampaignsPaged_(token, true);
  if (!campaigns.length) campaigns = BAOZ_CPC_listCampaignsPaged_(token, false);

  const out = [];
  for (const c of campaigns) {
    if (!BAOZ_CPC_isCpcCampaign_(c)) continue;
    const id = c.id || c.campaignId || c.campaign_id;
    const title = c.title || c.name || c.campaignTitle || c.campaign_title || '';
    if (id) out.push({ id: String(id), title: String(title) });
  }

  // стабильный порядок (для курсора)
  out.sort((a, b) => Number(a.id) - Number(b.id));
  return out;
}

function BAOZ_CPC_listCampaignsPaged_(token, withAdvObjectType) {
  const all = [];
  for (let page = 1; page <= 50; page++) {
    const qs = withAdvObjectType
      ? `advObjectType=SKU&page=${page}&pageSize=${BAOZ_CPC_CAMPAIGNS_PAGE_SIZE}`
      : `page=${page}&pageSize=${BAOZ_CPC_CAMPAIGNS_PAGE_SIZE}`;

    const resp = BAOZ_CPC_httpJson_('get', `/api/client/campaign?${qs}`, null, token);
    const items = BAOZ_CPC_extractCampaigns_(resp);
    if (!items.length) break;

    all.push(...items);
    if (items.length < BAOZ_CPC_CAMPAIGNS_PAGE_SIZE) break;
  }
  return all;
}

function BAOZ_CPC_isCpcCampaign_(c) {
  const blob = JSON.stringify(c || {}).toUpperCase();
  return blob.includes('"CPC"') || blob.includes('PAY_PER_CLICK') || blob.includes('PER_CLICK') || blob.includes('CLICK');
}

function BAOZ_CPC_extractCampaigns_(obj) {
  const candidates = [
    obj && obj.campaigns,
    obj && obj.items,
    obj && obj.result && obj.result.campaigns,
    obj && obj.result && obj.result.items,
    obj && obj.data && obj.data.campaigns,
    obj && obj.data && obj.data.items
  ];
  for (const v of candidates) if (Array.isArray(v)) return v;

  if (obj && typeof obj === 'object') {
    for (const k of Object.keys(obj)) if (Array.isArray(obj[k])) return obj[k];
  }
  return [];
}

function BAOZ_CPC_extractProducts_(resp) {
  const candidates = [
    resp && resp.products,
    resp && resp.result && resp.result.products,
    resp && resp.items,
    resp && resp.result && resp.result.items
  ];
  for (const v of candidates) if (Array.isArray(v)) return v;
  return [];
}

function BAOZ_CPC_getCpcExpenseMap_(token, fromDate, toDate) {
  const ymdFrom = BAOZ_CPC_fmtYmd_(fromDate);
  const ymdTo = BAOZ_CPC_fmtYmd_(toDate);

  // один запрос на интервал (вместо N запросов на каждую кампанию)
  const path = `/api/client/statistics/expense/json?dateFrom=${encodeURIComponent(ymdFrom)}&dateTo=${encodeURIComponent(ymdTo)}`;

  try {
    const resp = BAOZ_CPC_httpJson_('get', path, null, token);
    const rows = BAOZ_CPC_extractExpenseRows_(resp);

    // map: campaignId -> totalSpent
    const map = {};

    for (const r of rows) {
      if (!r || typeof r !== 'object') continue;

      const id = (r.id ?? r.campaignId ?? r.campaign_id);
      if (!id) continue;

      const money = BAOZ_CPC_toNumber_(r.moneySpent);
      const bonus = BAOZ_CPC_toNumber_(r.bonusSpent);
      const prepay = BAOZ_CPC_toNumber_(r.prepaymentSpent);

      const spent =
        (money ?? 0) +
        (bonus ?? 0) +
        (prepay ?? 0);

      const k = String(id);
      map[k] = (map[k] || 0) + spent;
    }

    return map;
  } catch (e) {
    // если расход не получили — фильтрацию НЕ применяем (чтобы ничего не потерять)
    return null;
  }
}
function BAOZ_CPC_getCpcCampaignStatsMap_(token, dateFrom, dateTo) {
  const from = BAOZ_CPC_fmtYmd_(dateFrom);
  const to = BAOZ_CPC_fmtYmd_(dateTo);

  const path =
    '/api/client/statistics/campaign/product/json' +
    '?dateFrom=' + encodeURIComponent(from) +
    '&dateTo=' + encodeURIComponent(to);

  const obj = BAOZ_CPC_httpJson_('get', path, null, token);
  const rows = BAOZ_CPC_extractCampaignStatsRows_(obj);

  const out = {};
  for (const r of rows) {
    const id = String((r && r.id) || '').trim();
    if (!id) continue;

    out[id] = {
      id: id,
      title: (r && r.title) ? String(r.title) : '',
      objectType: (r && r.objectType) ? String(r.objectType) : '',
      status: (r && r.status) ? String(r.status) : '',
      placement: (r && r.placement) ? String(r.placement) : '',
      weeklyBudget: BAOZ_CPC_toNumber_(r && r.weeklyBudget),
      budget: BAOZ_CPC_toNumber_(r && r.budget),
      moneySpent: BAOZ_CPC_toNumber_(r && r.moneySpent),
      views: BAOZ_CPC_toNumber_(r && r.views),
      clicks: BAOZ_CPC_toNumber_(r && r.clicks),
      ctr: BAOZ_CPC_toNumber_(r && r.ctr),
      clickPrice: BAOZ_CPC_toNumber_(r && r.clickPrice),
      orders: BAOZ_CPC_toNumber_(r && r.orders),
      ordersMoney: BAOZ_CPC_toNumber_(r && r.ordersMoney),
      drr: BAOZ_CPC_toNumber_(r && r.drr),
      toCart: BAOZ_CPC_toNumber_(r && r.toCart),
      strategy: (r && r.strategy) ? String(r.strategy) : ''
    };
  }

  return out;
}

function BAOZ_CPC_extractCampaignStatsRows_(obj) {
  if (!obj || typeof obj !== 'object') return [];
  return Array.isArray(obj.rows) ? obj.rows : [];
}


function BAOZ_CPC_extractExpenseRows_(obj) {
  if (!obj || typeof obj !== 'object') return [];
  return Array.isArray(obj.rows) ? obj.rows : [];
}


function BAOZ_CPC_fmtYmd_(d) {
  const tz = Session.getScriptTimeZone() || 'GMT';
  return Utilities.formatDate(d instanceof Date ? d : new Date(d), tz, 'yyyy-MM-dd');
}

function BAOZ_CPC_toNumber_(v) {
  if (v === null || v === undefined || v === '') return null;
  if (typeof v === 'number' && !isNaN(v)) return v;

  const s = String(v)
    .trim()
    .replace(/\s+/g, '')
    .replace(',', '.');

  if (!s) return null;

  const n = Number(s);
  return isNaN(n) ? null : n;
}

function BAOZ_CPC_httpJson_(method, path, payload, bearer) {
  const url = BAOZ_CPC_PERF_HOST + path;

  const opt = {
    method: method,
    muteHttpExceptions: true,
    headers: { 'Accept': 'application/json' }
  };
  if (bearer) opt.headers['Authorization'] = 'Bearer ' + bearer;

  if (payload !== null && payload !== undefined) {
    opt.contentType = 'application/json';
    opt.payload = JSON.stringify(payload);
  }

  const resp = UrlFetchApp.fetch(url, opt);
  const code = resp.getResponseCode();
  const text = resp.getContentText() || '';

  if (code < 200 || code >= 300) {
    throw new Error(`HTTP ${code} ${path}: ${text}`);
  }

  try { return JSON.parse(text); }
  catch (_) { return { _raw: text }; }
}
