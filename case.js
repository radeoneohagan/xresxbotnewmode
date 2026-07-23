require("./settings")
const baileys = require("@whiskeysockets/baileys")
const { generateWAMessageFromContent, proto, generateWAMessageContent, generateWAMessage, prepareWAMessageMedia, downloadContentFromMessage, areJidsSameUser, getContentType } = require("@whiskeysockets/baileys")
const fs = require('fs')
const archiver = require("archiver");
const util = require('util')

// [FIX H4] Safe write helper — atomic write untuk file JSON kecil
function safeWriteJSON(filePath, data) {
  try {
    const tmpPath = filePath + '.tmp'
    fs.writeFileSync(tmpPath, JSON.stringify(data, null, 2))
    fs.renameSync(tmpPath, filePath)
  } catch (e) {
    // Fallback ke direct write jika rename gagal (cross-device)
    try { fs.writeFileSync(filePath, JSON.stringify(data, null, 2)) } catch {}
  }
}

// [AUDIT-FIX] Pembaca JSON hot-path dengan cache berbasis mtime.
// Menghindari fs.readFileSync + JSON.parse berulang pada SETIAP pesan grup
// (antiswgc.json & warndata.json) tanpa mengubah perilaku: selalu mencerminkan
// isi file terbaru karena re-parse hanya dilakukan bila mtime file berubah
// (mis. setelah safeWriteJSON dari command terkait).
// PENTING: hanya untuk pemakaian READ-ONLY — jangan memutasi objek yang dikembalikan.
const _hotJsonCache = {}
const _hotJsonMtime = {}
function readHotJson(filePath, fallback) {
  try {
    const mt = fs.statSync(filePath).mtimeMs
    if (_hotJsonMtime[filePath] === mt && (filePath in _hotJsonCache)) {
      return _hotJsonCache[filePath]
    }
    const parsed = JSON.parse(fs.readFileSync(filePath, 'utf8'))
    _hotJsonMtime[filePath] = mt
    _hotJsonCache[filePath] = parsed
    return parsed
  } catch {
    return fallback
  }
}

// ============================================================================
// [PATCH B] UNIFIED BROADCAST ENGINE (root cause RC-3)
// ----------------------------------------------------------------------------
// Satu jalur tunggal untuk seluruh fan-out (jpm, swgc, pushkontak, sendstatus).
// Menyatukan concern lintas-command yang selama ini diduplikasi & tidak konsisten:
//   1. LOCK  : akuisisi atomik (check-then-set sinkron) + rilis via finally
//              -> mencegah broadcast konkuren (F5-02) & lock nyangkut (T2-11/T2-12)
//   2. STOP  : cek flag stop tiap iterasi -> .stopjpm/.stopswgc/.stoppush bekerja
//              & loop berhenti saat disconnect (index.js set flag=true) (T2-05/T2-16)
//   3. SOCKET: cek socket hidup tiap iterasi via global.getLiveConn() (RC-2)
//              -> tidak pernah mengirim melalui socket mati pasca-reconnect
//   4. DELAY : jeda antar target (number atau function utk delay acak anti-ban)
//   5. CLEANUP: dijamin lewat finally (hapus temp file) walau terjadi exception
//
// Payload spesifik tiap command tetap berada di callback `sendOne(conn,target,i)`
// sehingga feature-parity (mentions, vcard ganda, relay groupStatus, dst) terjaga.
//
// Return: { rejected, sukses, gagal, index, total, stopped }
// ============================================================================
// [JPM ANTI-SPAM] Catat ID pesan JPM yang terkirim. Saat pesan dihapus admin/bot
// di grup lain, WhatsApp bisa mengirim retry-receipt yang membuat Baileys mengirim
// ULANG pesan yang sama (spam). getMessage (index.js) menolak resend untuk ID yang
// tercatat di sini -> setiap varian JPM cukup 1x per grup. Khusus JPM saja.
function _trackJpmMsg(sent) {
  const _id = sent?.key?.id || (typeof sent === 'string' ? sent : null)
  if (_id) {
    if (!global._jpmMsgIds) global._jpmMsgIds = new Set()
    global._jpmMsgIds.add(_id)
    if (global._jpmMsgIds.size > 5000) global._jpmMsgIds.clear()
  }
  return sent
}

async function runBroadcast(opts) {
  const {
    lockFlag = null,
    stopFlag = null,
    targets = [],
    delayMs = 5000,
    sendOne,
    onFirstSuccess = null,
    cleanup = null,
  } = opts || {}

  // --- 1. Akuisisi lock secara atomik (sinkron, tanpa await di antara) ---
  if (lockFlag && global[lockFlag]) {
    if (typeof cleanup === 'function') { try { cleanup() } catch {} }
    return { rejected: true, sukses: 0, gagal: 0, index: 0, total: targets.length, stopped: false }
  }
  if (lockFlag) global[lockFlag] = true
  if (stopFlag) global[stopFlag] = false

  let sukses = 0, gagal = 0, index = 0, stopped = false, firstDone = false

  try {
    for (let i = 0; i < targets.length; i++) {
      // --- 2. Stop-check (flag dari command .stop* atau dari disconnect) ---
      if (stopFlag && global[stopFlag]) { stopped = true; break }
      // --- 3. Socket-liveness: berhenti bila koneksi mati/reconnect ---
      if (!global._connAlive) { stopped = true; break }
      const conn = (typeof global.getLiveConn === 'function') ? global.getLiveConn() : null
      if (!conn) { stopped = true; break }

      index++
      try {
        await sendOne(conn, targets[i], index)
        sukses++
        if (!firstDone && typeof onFirstSuccess === 'function') {
          firstDone = true
          try { await onFirstSuccess(conn) } catch {}
        }
      } catch (e) {
        gagal++
      }

      // --- 4. Delay antar target (kecuali target terakhir / sedang berhenti) ---
      const more = i < targets.length - 1
      const stillGoing = !(stopFlag && global[stopFlag]) && global._connAlive
      if (more && stillGoing) {
        const _d = (typeof delayMs === 'function') ? delayMs() : delayMs
        await new Promise(r => setTimeout(r, _d))
      }
    }
  } finally {
    // --- 1b. Rilis lock apa pun yang terjadi (fix lock-nyangkut) ---
    if (lockFlag) { try { delete global[lockFlag] } catch { try { global[lockFlag] = false } catch {} } }
    if (stopFlag) { try { global[stopFlag] = false } catch {} }
    // --- 5. Cleanup dijamin ---
    if (typeof cleanup === 'function') { try { cleanup() } catch {} }
  }

  return { rejected: false, sukses, gagal, index, total: targets.length, stopped }
}

// ============================================================================
// [AUTOJOINGC SCHEDULER] — Queue-based join scheduler with human-like timing.
// - Queue max 10 link. Validasi invite code sebelum enqueue.
// - Delay: 90 menit + random 10-30 menit per siklus (berlaku untuk SEMUA hasil).
// - Membership check via error response (tanpa request metadata tambahan).
// - Persist queue ke file agar survive restart.
// - Single-timer guard (tidak pernah ada 2 timer aktif bersamaan).
// - Rate limit → random pause 30-60 menit.
// - Notifikasi owner via WA (tanpa spam, 1x per kondisi penuh).
// ============================================================================
const AUTOJOIN_QUEUE_PATH = './database/autojoingc_queue.json'
const AUTOJOIN_MAX_QUEUE = 10
const AUTOJOIN_BASE_DELAY = 90 * 60 * 1000   // 90 menit
const AUTOJOIN_RANDOM_MIN = 10 * 60 * 1000   // +10 menit
const AUTOJOIN_RANDOM_MAX = 30 * 60 * 1000   // +30 menit
const AUTOJOIN_RL_PAUSE_MIN = 30 * 60 * 1000 // rate limit pause min 30 menit
const AUTOJOIN_RL_PAUSE_MAX = 60 * 60 * 1000 // rate limit pause max 60 menit

let _autoJoinTimer = null
let _autoJoinNextAt = 0
let _ajStats = { success: 0, failed: 0, skipped: 0, lastReset: Date.now() }
let _ajQueueFullNotified = false

function _ajLoadQueue() {
  try {
    const d = JSON.parse(fs.readFileSync(AUTOJOIN_QUEUE_PATH, 'utf8'))
    if (d && typeof d === 'object' && Array.isArray(d.queue)) return d
    return { queue: [], nextAt: 0 }
  } catch { return { queue: [], nextAt: 0 } }
}
function _ajSaveQueue(data) { safeWriteJSON(AUTOJOIN_QUEUE_PATH, data) }

function _ajCalcDelay() {
  return AUTOJOIN_BASE_DELAY + AUTOJOIN_RANDOM_MIN + Math.floor(Math.random() * (AUTOJOIN_RANDOM_MAX - AUTOJOIN_RANDOM_MIN))
}

function _ajCalcRateLimitPause() {
  return AUTOJOIN_RL_PAUSE_MIN + Math.floor(Math.random() * (AUTOJOIN_RL_PAUSE_MAX - AUTOJOIN_RL_PAUSE_MIN))
}

function _ajFormatMs(milliseconds) {
  const totalMin = Math.round(milliseconds / 60000)
  const h = Math.floor(totalMin / 60)
  const m = totalMin % 60
  return h > 0 ? `${h} jam ${m} menit` : `${m} menit`
}

function _ajResetDailyStats() {
  const now = Date.now()
  if (now - _ajStats.lastReset > 24 * 60 * 60 * 1000) {
    _ajStats = { success: 0, failed: 0, skipped: 0, lastReset: now }
  }
}

async function _ajNotifyOwner(text) {
  try {
    const conn = (typeof global.getLiveConn === 'function') ? global.getLiveConn() : null
    if (!conn) return
    const ownerJid = (global.owner?.[0] || '').replace(/[^0-9]/g, '') + '@s.whatsapp.net'
    if (!ownerJid || ownerJid === '@s.whatsapp.net') return
    await conn.sendMessage(ownerJid, { text })
  } catch {}
}

function _ajIsRateLimit(err) {
  const msg = (err?.message || err?.toString() || '').toLowerCase()
  const code = err?.data || err?.output?.statusCode || err?.statusCode || 0
  return code === 429 || code === 503 ||
    msg.includes('rate') || msg.includes('too many') ||
    msg.includes('temporarily') || msg.includes('retry') ||
    msg.includes('unavailable')
}

function autoJoinGcEnqueue(invite, source) {
  if (!invite || typeof invite !== 'string') return 'invalid'
  const data = _ajLoadQueue()

  // Max queue 10
  if (data.queue.length >= AUTOJOIN_MAX_QUEUE) {
    console.log(`[AutoJoinGC] Queue penuh (${AUTOJOIN_MAX_QUEUE}), abaikan: ${invite}`)
    // Notif owner hanya sekali per kondisi penuh
    if (!_ajQueueFullNotified) {
      _ajQueueFullNotified = true
      const info = autoJoinGcGetStatus()
      _ajNotifyOwner(
        `⚠️ *[AUTO JOIN GC]*\n\n` +
        `Queue: *${data.queue.length}/${AUTOJOIN_MAX_QUEUE}*\n\n` +
        `Invite baru ditolak karena queue penuh.\n\n` +
        `Next Join:\n${info.nextInFormatted !== '-' ? info.nextInFormatted + ' lagi.' : 'Scheduler tidak aktif.'}`
      )
    }
    return 'full'
  }

  // Reset notif flag karena queue belum penuh
  _ajQueueFullNotified = false

  // Deduplicate: invite code sudah ada di queue
  if (data.queue.some(item => item.invite === invite)) {
    console.log(`[AutoJoinGC] Duplikat, abaikan: ${invite}`)
    return 'duplicate'
  }

  // Tidak cek membership di sini — akan dicek saat scheduler benar-benar akan join
  data.queue.push({ invite, time: Date.now() })
  _ajSaveQueue(data)
  console.log(`[AutoJoinGC] Enqueued: ${invite} | Queue: ${data.queue.length}/${AUTOJOIN_MAX_QUEUE}`)

  // Mulai scheduler jika belum berjalan
  autoJoinGcSchedulerStart()
  return 'ok'
}

function autoJoinGcSchedulerStart() {
  // Guard: hanya 1 timer aktif
  if (_autoJoinTimer) return
  const data = _ajLoadQueue()
  if (data.queue.length === 0) return
  if (!global.autoJoinGc) return

  let delayMs

  // Resume countdown jika ada nextAt yang masih di masa depan
  if (data.nextAt && data.nextAt > Date.now()) {
    delayMs = data.nextAt - Date.now()
    _autoJoinNextAt = data.nextAt
    console.log(`[AutoJoinGC] Resuming: ${_ajFormatMs(delayMs)} tersisa`)
  } else {
    // Hitung delay baru
    delayMs = _ajCalcDelay()
    _autoJoinNextAt = Date.now() + delayMs
    data.nextAt = _autoJoinNextAt
    _ajSaveQueue(data)
  }

  const randomPart = delayMs - AUTOJOIN_BASE_DELAY
  console.log(`[AutoJoinGC] Next Join : ${_ajFormatMs(delayMs)}`)
  console.log(`[AutoJoinGC] Random Delay : +${_ajFormatMs(randomPart > 0 ? randomPart : 0)}`)
  console.log(`[AutoJoinGC] Queue : ${data.queue.length}/${AUTOJOIN_MAX_QUEUE}`)

  _autoJoinTimer = setTimeout(() => _ajProcessNext(), delayMs)
}

async function _ajProcessNext() {
  _autoJoinTimer = null
  _autoJoinNextAt = 0

  if (!global.autoJoinGc) return

  const d = _ajLoadQueue()
  if (d.queue.length === 0) {
    d.nextAt = 0
    _ajSaveQueue(d)
    _ajNotifyOwner(
      `✅ *[AUTO JOIN GC]*\n\n` +
      `Semua queue telah selesai diproses.\n\n` +
      `Success: ${_ajStats.success}\nFailed: ${_ajStats.failed}\nSkipped: ${_ajStats.skipped}\n\n` +
      `Menunggu link baru.`
    )
    return
  }

  // Ambil item pertama (FIFO)
  const item = d.queue.shift()
  d.nextAt = 0
  _ajSaveQueue(d)

  _ajResetDailyStats()

  // Cek koneksi
  const conn = (typeof global.getLiveConn === 'function') ? global.getLiveConn() : null
  if (!conn) {
    console.log(`[AutoJoinGC] ⚠️ Skip (no connection): ${item.invite}`)
    _ajStats.skipped++
    _ajScheduleNextCycle()
    return
  }

  // Langsung coba join — jika sudah member, error akan di-catch
  try {
    await conn.groupAcceptInvite(item.invite)
    console.log(`[AutoJoinGC] ✅ Joined: ${item.invite}`)
    _ajStats.success++
  } catch (err) {
    const errMsg = (err?.message || err?.toString() || '').toLowerCase()
    console.log(`[AutoJoinGC] ❌ Gagal: ${item.invite} — ${errMsg}`)

    // Cek apakah sudah member (WhatsApp return error spesifik)
    if (errMsg.includes('already') || errMsg.includes('participant') || errMsg.includes('member')) {
      console.log(`[AutoJoinGC] ⏭️ Skipped (sudah member): ${item.invite}`)
      _ajStats.skipped++
      _ajScheduleNextCycle()
      return
    }

    // Cek apakah rate limit
    if (_ajIsRateLimit(err)) {
      const pauseMs = _ajCalcRateLimitPause()
      console.log(`[AutoJoinGC] ⚠️ Rate limit! Pause ${_ajFormatMs(pauseMs)}`)
      _ajStats.failed++
      _ajNotifyOwner(
        `⚠️ *[AUTO JOIN GC]*\n\n` +
        `WhatsApp mendeteksi Rate Limit.\n\n` +
        `Scheduler dipause.\n\n` +
        `Cooldown:\n${_ajFormatMs(pauseMs)}.`
      )
      const pauseData = _ajLoadQueue()
      _autoJoinNextAt = Date.now() + pauseMs
      pauseData.nextAt = _autoJoinNextAt
      _ajSaveQueue(pauseData)
      _autoJoinTimer = setTimeout(() => _ajProcessNext(), pauseMs)
      return
    }

    // Gagal biasa (expired/invalid/penuh/disbanded)
    _ajStats.skipped++
  }

  // Setiap siklus → delay normal sebelum berikutnya (baik sukses, gagal, atau skip)
  _ajScheduleNextCycle()
}

function _ajScheduleNextCycle() {
  if (_ajLoadQueue().queue.length > 0 && global.autoJoinGc) {
    autoJoinGcSchedulerStart()
  } else if (_ajLoadQueue().queue.length === 0) {
    _ajNotifyOwner(
      `✅ *[AUTO JOIN GC]*\n\n` +
      `Semua queue telah selesai diproses.\n\n` +
      `Success: ${_ajStats.success}\nFailed: ${_ajStats.failed}\nSkipped: ${_ajStats.skipped}\n\n` +
      `Menunggu link baru.`
    )
  }
}

function autoJoinGcSchedulerStop() {
  if (_autoJoinTimer) {
    clearTimeout(_autoJoinTimer)
    _autoJoinTimer = null
  }
  _autoJoinNextAt = 0
}

function autoJoinGcGetStatus() {
  _ajResetDailyStats()
  const data = _ajLoadQueue()
  const running = !!_autoJoinTimer
  const nextIn = _autoJoinNextAt > Date.now() ? _autoJoinNextAt - Date.now() : 0
  return {
    queueSize: data.queue.length,
    maxQueue: AUTOJOIN_MAX_QUEUE,
    running,
    nextIn,
    nextInFormatted: nextIn > 0 ? _ajFormatMs(nextIn) : '-',
    queue: data.queue,
    stats: { success: _ajStats.success, failed: _ajStats.failed, skipped: _ajStats.skipped }
  }
}

// Boot: resume scheduler setelah settings ter-load
;(function _ajBootResume() {
  setTimeout(() => {
    if (global.autoJoinGc && _ajLoadQueue().queue.length > 0) {
      console.log('[AutoJoinGC] Boot: resume scheduler...')
      autoJoinGcSchedulerStart()
    }
  }, 10000)
})()

const yts = require('yt-search');
const ytdl = require('@vreden/youtube_scraper');
const axios = require('axios')
const { exec,spawn, execSync  } = require("child_process")
const chalk = require('chalk')
const moment = require('moment-timezone');
const emojiRegex = require('emoji-regex');
const cheerio = require("cheerio")
const { wrapper } = require("axios-cookiejar-support");
const { CookieJar } = require("tough-cookie");
const ms = require("ms");
const { chatgpt, resetHistory } = require('./lib/media/chatgpt')
const { generateMusic } = require('./lib/media/suno')
const DouyinDownloader = require("./lib/media/douyin")
const Nimegami = require("./lib/media/anime")
const os = require('os')
const crypto = require('crypto');
const path = require("path");
const { unixTimestampSeconds, generateMessageTag, processTime, webApi, getRandom, getBuffer, fetchJson, runtime, clockString, sleep, isUrl, getTime, formatDate, tanggal, formatp, jsonformat, reSize, logic, generateProfilePicture, bytesToSize, parseMention, getGroupAdmins, readFileTxt, readFileJson, batasiTeks, randomText, isEmoji, getTypeUrlMedia, pickRandom, toIDR, capital, ucapan, loadModule } = require('./lib/function');
const { addResponList, delResponList, isAlreadyResponList, isAlreadyResponListGroup, sendResponList, updateResponList, getDataResponList } = require('./lib/respon-list');
const { igdl } = require('./lib/media/instagram')
const { removeBg, removeBgV1, removeBgV2, removeBgV3, removeBgV4 } = require('./lib/media/removebg')
const { fileTypeFromBuffer } = require('file-type')
const { pinterest, pinterest2, remini, mediafire, tiktokDl } = require('./lib/media/scraper');
const { uploadToPomf, uploadToCdn } = require('./lib/scrape')
const speed = require('performance-now')
const { LoadDataBase } = require('./lib/source/message')

module.exports = NXL = async (NXL, m, chatUpdate, store) => {
	try {
const from = m.key.remoteJid
await LoadDataBase(NXL, m)
if (global.moduleType == undefined) global.moduleType = 0
if (global.moduleType === 0) {
await loadModule(NXL)
global.moduleType += 1 }

if (!global._cachedBotNumber) global._cachedBotNumber = NXL.decodeJid(NXL.user.id)
const botNumber = global._cachedBotNumber
const body = ((m.type === 'conversation') ? m.message.conversation :(m.type == 'imageMessage') ? m.message.imageMessage.caption :(m.type == 'videoMessage') ? m.message.videoMessage.caption :(m.type == 'extendedTextMessage') ? m.message.extendedTextMessage.text :(m.type == 'buttonsResponseMessage') ? m.message.buttonsResponseMessage.selectedButtonId :(m.type == 'listResponseMessage') ? m.message.listResponseMessage.singleSelectReply.selectedRowId :(m.type == 'templateButtonReplyMessage') ? m.message.templateButtonReplyMessage.selectedId :(m.type == 'interactiveResponseMessage') ? JSON.parse(m.message.interactiveResponseMessage.nativeFlowResponseMessage.paramsJson).id :(m.type === 'messageContextInfo') ? (m.message.buttonsResponseMessage?.selectedButtonId ||m.message.listResponseMessage?.singleSelectReply.selectedRowId ||m.text) :'') || ''
const budy = (typeof m.text == 'string' ? m.text : '')
const prefix = "."
const isCmd = body.startsWith(prefix)
const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : '';
const args = body.trim().split(/ +/).slice(1)
const text = q = args.join(" ")
const sender = m.key.fromMe ? (NXL.user.id.split(':')[0]+'@s.whatsapp.net' || NXL.user.id) : (m.key.participant || m.key.remoteJid)
const senderNumber = sender.split('@')[0]
const typ = global.cache.owner
const kontributor = global.cache.owner
const Antilink2 = global.cache?.antilink2 || []
const Antilink = global.cache?.antilink || []
const welcome = global.cache?.welcome || []
const ANTICALLGC_PATH = './database/anticallgc.json'
if (!global.anticallgcList) {
  try { global.anticallgcList = JSON.parse(fs.readFileSync(ANTICALLGC_PATH, 'utf8')) } catch { global.anticallgcList = [] }
}
const anticallgcList = global.anticallgcList


const ANTIBOT_PATH = './database/antibot.json'
const ANTIBOTSET_PATH = './database/antibotSettings.json'
if (!global.antibotList) {
  try { global.antibotList = JSON.parse(fs.readFileSync(ANTIBOT_PATH, 'utf8')) } catch { global.antibotList = [] }
}
if (!global.antibotSettings) {
  try { global.antibotSettings = JSON.parse(fs.readFileSync(ANTIBOTSET_PATH, 'utf8')) } catch { global.antibotSettings = {} }
}
const antibotList = global.antibotList
const antibotSettings = global.antibotSettings

const ANTITOXIC_PATH = './database/antitoxic.json'
const TOXICWORDS_PATH = './database/toxicwords.json'
if (!global.antitoxicList) {
  try { global.antitoxicList = JSON.parse(fs.readFileSync(ANTITOXIC_PATH, 'utf8')) } catch { global.antitoxicList = [] }
}
if (!global.toxicWords) {
  try { global.toxicWords = JSON.parse(fs.readFileSync(TOXICWORDS_PATH, 'utf8')) } catch { global.toxicWords = [] }
}
const antitoxicList = global.antitoxicList
const toxicWords = global.toxicWords
const Reseller = global.cache.reseller
const contacts = global.cache.contacts
const Developer = global.cache.owner
const isContacts = contacts.includes(sender)
const isPc = from.endsWith('@s.whatsapp.net')

const _allOwners = [...(global.owner || []), ...(global.cache?.owner || [])]
const _ownerKey = _allOwners.join(',')
if (!global._cachedCreatorJids || global._cachedCreatorJidsKey !== _ownerKey) {
  global._cachedCreatorJidsKey = _ownerKey
  global._cachedCreatorJids = new Set([botNumber, ..._allOwners].map(v => v.replace(/[^0-9]/g, '') + '@s.whatsapp.net'))
}
if (!global._cachedPremiumJids || global._cachedPremiumJidsBase !== Reseller.length + kontributor.length) {
  global._cachedPremiumJidsBase = Reseller.length + kontributor.length
  global._cachedPremiumJids = new Set([botNumber, ...Reseller, ...kontributor].map(v => v.replace(/[^0-9]/g, '') + '@s.whatsapp.net'))
}
const isPremium = global._cachedPremiumJids.has(m.sender)
const isCreator = !!(m && m.sender && global._cachedCreatorJids.has(m.sender))
const pushname = m.pushName || `${senderNumber}`
const isBot = botNumber.includes(senderNumber)
const quoted = m.quoted ? m.quoted : m
const mime = (quoted.msg || quoted).mimetype || ''
const isMedia = /image|video|sticker|audio/.test(mime)
if (!global.groupCache) global.groupCache = {}
if (!global.groupCacheTime) global.groupCacheTime = {}
const groupMetadata = m.isGroup ? (
  (global.groupCache[from] && (Date.now() - (global.groupCacheTime[from]||0)) < (global.GROUP_CACHE_TTL || 300000))
    ? global.groupCache[from]
    : await NXL.groupMetadata(from).then(meta => {
        global.groupCache[from] = meta
        global.groupCacheTime[from] = Date.now()
        return meta
      }).catch(e => null)
) : null
const _gm = groupMetadata || { subject: '', participants: [] }
const groupName = m.isGroup ? (_gm.subject || '') : ''
const participants = m.isGroup ? (_gm.participants || []) : []
const groupAdmins = m.isGroup ? await getGroupAdmins(participants) : []
const froms = m.quoted ? m.quoted.sender : text ? (text.replace(/[^0-9]/g, '') ? text.replace(/[^0-9]/g, '') + '@s.whatsapp.net' : false) : false;
const _botJid = NXL.user.id.split(':')[0] + '@s.whatsapp.net'
const isBotAdmins = m.isGroup ? participants.some(p =>
  (p.id && areJidsSameUser(p.id, _botJid) || (p.lid && p.lid === p.id)) && p.admin
) : false
const isAdmins = m.isGroup ? groupAdmins.includes(m.sender) : false
const qmsg = (quoted.msg || quoted)
if (!global.sendJsonCache) {
  try { global.sendJsonCache = JSON.parse(fs.readFileSync('./lib/send.json')) } catch { global.sendJsonCache = {} }
}
let db_respon_list = global.sendJsonCache;
let listStore = global.sendJsonCache;
let set_proses = global.sendJsonCache;
let set_done = global.sendJsonCache
let ppuser = null
let ppnyauser = null
const getPPUser = async () => {
  if (ppuser) return ppuser
  try { ppuser = await NXL.profilePictureUrl(m.sender, 'image') }
  catch { ppuser = `${global.cdnPixabay}/photo/2015/10/05/22/37/blank-profile-picture-973460_960_720.png?q=60` }
  ppnyauser = await getBuffer(ppuser)
  return ppuser
}
try {
  let isNumber = x => typeof x === 'number' && !isNaN(x)
  let limitUser = global.limitawal
  if (!global.db) global.db = {}
  if (!global.db.users) global.db.users = {}
  let userDb = global.db.users[m.sender]
  if (typeof userDb !== 'object' || userDb === null) {
    global.db.users[m.sender] = {
      afkTime: -1,
      afkReason: '',
      limit: limitUser,
      NXL: false,
    }
  } else {
    if (!isNumber(userDb.afkTime)) userDb.afkTime = -1
    if (!('afkReason' in userDb)) userDb.afkReason = ''
    if (!isNumber(userDb.limit)) userDb.limit = limitUser
    if (typeof userDb.NXL === 'undefined') userDb.NXL = false
  }
} catch (err) {
  console.log(err)
}
if (isMedia && m.msg?.fileSha256 && global.db?.sticker && (m.msg.fileSha256.toString('base64') in global.db.sticker)) {
let hash = global.db.sticker[m.msg.fileSha256.toString('base64')]
let { text, mentionedJid } = hash
let messages = await generateWAMessage(from, { text: text, mentions: mentionedJid }, {
userJid: NXL.user.id,
quoted : m.quoted && m.quoted.fakeObj
})
messages.key.fromMe = areJidsSameUser(m.sender, NXL.user.id)
messages.key.id = m.key.id
messages.pushName = m.pushName
if (m.isGroup) messages.participant = m.sender
let msg = {
...chatUpdate,
messages: [proto.WebMessageInfo.fromObject(messages)],
type: 'append'
}
NXL.ev.emit('messages.upsert', msg)
return
}
if (m.isGroup && isAlreadyResponList(m.chat, body.toLowerCase(), db_respon_list)) {
var get_data_respon = getDataResponList(m.chat, body.toLowerCase(), db_respon_list)
if (get_data_respon.isImage === false) {
NXL.sendMessage(m.chat, { text: sendResponList(m.chat, body.toLowerCase(), db_respon_list) }, {
quoted: m
})
} else {
NXL.sendMessage(m.chat, { image: await getBuffer(get_data_respon.image_url), caption: get_data_respon.response }, {
quoted: m
})
}
}

const reSize = async(buffer, ukur1, ukur2) => {
   return new Promise(async(resolve, reject) => {
      let jimp = require('jimp')
      var baper = await jimp.read(buffer);
      var ab = await baper.resize(ukur1, ukur2).getBufferAsync(jimp.MIME_JPEG)
      resolve(ab)
   })
}
const Styles = (text, style = 1) => {
var xStr = 'abcdefghijklmnopqrstuvwxyz1234567890'.split('');
  var yStr = {
1: 'ᴀʙᴄᴅᴇꜰɢʜɪᴊᴋʟᴍɴᴏᴘqʀꜱᴛᴜᴠᴡxʏᴢ1234567890'
};
var replacer = [];
xStr.map((v, i) =>
replacer.push({
original: v,
convert: yStr[style].split('')[i]
})
);
var str = text.toLowerCase().split('');
var output = [];
str.map((v) => {
const find = replacer.find((x) => x.original == v);
find ? output.push(find.convert) : output.push(v);
});
return output.join('');
};
if (!global.gambar1Cache) {
  try { global.gambar1Cache = fs.readFileSync('./lib/image/image.jpg') } catch { global.gambar1Cache = null }
}
const gambar1 = global.gambar1Cache
let user = [m.sender]
if (m.isGroup && !m.key.fromMe && !isCreator && !isAdmins) {
  try {
    const _warnPath = './database/warndata.json'
    // [FIX WARN] Baca FRESH tiap pesan (identik Fix14). readHotJson berbasis cache
    // mtime bisa mengembalikan data BASI saat mtime tidak berubah terdeteksi
    // (granularitas mtime kasar di filesystem container) -> warn tak pernah aktif.
    let _warnData = {}
    try { _warnData = JSON.parse(fs.readFileSync(_warnPath, 'utf8')) } catch { _warnData = {} }

    const _groupData = _warnData?.[from] || {}

    let _senderJid = m.sender
    if (participants && participants.length) {
      const _sNum = (m.sender || '').replace(/@.*$/, '')
      const _p = participants.find(p => (p.lid || p.id || '').replace(/@.*$/, '') === _sNum)
      if (_p && _p.jid) _senderJid = _p.jid.replace(/@.*$/, '') + '@s.whatsapp.net'
      else if (_p && _p.id && _p.id.includes('@s.whatsapp.net')) _senderJid = _p.id
    }

    const _senderNum = _senderJid.replace(/@.*$/, '')
    const _matchKey = Object.keys(_groupData).find(k => k.replace(/@.*$/, '') === _senderNum)
    const _userWarn = _matchKey ? (_groupData[_matchKey] || 0) : 0

    if (_userWarn >= 1) {
      try {
        await NXL.sendMessage(from, { delete: m.key })
      } catch (delErr) {
      }
      return
    }
  } catch (e) {
  }
}

if (!NXL.public) {
if (!m.key.fromMe && !isCreator) return
}


if (!isCmd && m.isGroup && global._gameSessions?.[m.chat] && m.quoted) {
  const _gSession = global._gameSessions[m.chat]
  if (m.quoted.id === _gSession.messageId) {
    const _userAns = (m.text || '').toLowerCase().trim()
    const _correct = Array.isArray(_gSession.jawaban) ? _gSession.jawaban.some(j => _userAns.includes(j)) : _userAns === _gSession.jawaban
    if (_correct) {
      clearTimeout(_gSession.timeout)
      delete global._gameSessions[m.chat]
      await NXL.sendMessage(m.chat, { text: `✅ *Benar!* 🎉\n\nJawaban: *${Array.isArray(_gSession.jawaban)?_gSession.jawaban.join(' / '):_gSession.jawaban}*\n\nSelamat @${m.sender.split('@')[0]}!`, mentions: [m.sender] }, { quoted: m })
    }
  }
}

const getPPorangnya = async () => {
  const pp = await getPPUser()
  return await reSize(pp, 300, 300)
}
const reply = (teks) => {
// [FIX CONTEXT-LEAK] Snapshot 'from' sudah diambil saat command mulai (closure).
// Gunakan socket hidup; log error alih-alih swallow.
const _c = (typeof global.getLiveConn === 'function' && global.getLiveConn()) || NXL
return _c.sendMessage(from, { text : teks }, {quoted:m}).catch(err => {
  console.error('[REPLY ERROR]', from, err?.message || err)
})
}
const qtext = {key: {remoteJid: "status@broadcast", participant: "0@s.whatsapp.net"}, message: {"extendedTextMessage": {"text": `${global.ownername} ${global.versibot}`}}}
const FakeChannel = {
  key: {
    remoteJid: 'status@broadcast',
    fromMe: false,
    participant: '0@s.whatsapp.net'
  },
  message: {
    newsletterAdminInviteMessage: {
      newsletterJid: '123@newsletter',
      caption: `${global.ownername} ${global.versibot}`,
      inviteExpiration: 0
    }
  }
}

const fatext = {
key: {
participant: `0@s.whatsapp.net`,
...(m.chat ? {
remoteJid: "status@broadcast"
} : {})
},
message: {
listResponseMessage: {
title: ` *${ownername} Devloper*`
}
}
}

const qlocJpm = {key: {participant: '0@s.whatsapp.net', ...(m.chat ? {remoteJid: `status@broadcast`} : {})}, message: {locationMessage: {name: `WhatsApp Bot ${ownername}`,jpegThumbnail: ""}}}

const lampuwarna = {
key: {
participant: `0@s.whatsapp.net`,
...(m.chat ? {
remoteJid: "status@broadcast"
} : {})
},
message: {
listResponseMessage: {
title: `*${ownername} 1.0*`
}
}
}
const jpm = {
key: {
participant: `0@s.whatsapp.net`,
...(m.chat ? {
remoteJid: "status@broadcast"
} : {})
},
message: {
listResponseMessage: {
title: `NXL BOT REV`
}
}
}
function getFormattedDate() {
  var currentDate = new Date();
  var day = currentDate.getDate();
  var month = currentDate.getMonth() + 1;
  var year = currentDate.getFullYear();
  var hours = currentDate.getHours();
  var minutes = currentDate.getMinutes();
  var seconds = currentDate.getSeconds();
}

let d = new Date(new Date + 3600000)
let locale = 'id'
let week = d.toLocaleDateString(locale, { weekday: 'long' })
let date = d.toLocaleDateString(locale, {
  day: 'numeric',
  month: 'long',
  year: 'numeric'
})
const hariini = d.toLocaleDateString('id', { day: 'numeric', month: 'long', year: 'numeric' })

function msToTime(duration) {
var milliseconds = parseInt((duration % 1000) / 100),
seconds = Math.floor((duration / 1000) % 60),
minutes = Math.floor((duration / (1000 * 60)) % 60),
hours = Math.floor((duration / (1000 * 60 * 60)) % 24)

hours = (hours < 10) ? "0" + hours : hours
minutes = (minutes < 10) ? "0" + minutes : minutes
seconds = (seconds < 10) ? "0" + seconds : seconds
return hours + " jam " + minutes + " menit " + seconds + " detik"
}

function msToDate(ms) {
		temp = ms
		days = Math.floor(ms / (24*60*60*1000));
		daysms = ms % (24*60*60*1000);
		hours = Math.floor((daysms)/(60*60*1000));
		hoursms = ms % (60*60*1000);
		minutes = Math.floor((hoursms)/(60*1000));
		minutesms = ms % (60*1000);
		sec = Math.floor((minutesms)/(1000));
		return days+" Hari "+hours+" Jam "+ minutes + " Menit";
  }

async function groupStatus(jid, content) {
  // [PATCH B] Pakai socket hidup agar tidak relay via socket mati saat reconnect.
  const _conn = (typeof global.getLiveConn === 'function' && global.getLiveConn()) || NXL
  const { backgroundColor } = content;
  delete content.backgroundColor;
  const inside = await baileys.generateWAMessageContent(content, {
    upload: _conn.waUploadToServer,
    backgroundColor
  });
  const messageSecret = crypto.randomBytes(32);
  const m = baileys.generateWAMessageFromContent(jid, {
    messageContextInfo: {
      messageSecret
    },
    groupStatusMessageV2: {
      message: {
        ...inside,
        messageContextInfo: {
          messageSecret
        }
      }
    }
  }, {});
  await _conn.relayMessage(jid, m.message, {
    messageId: m.key.id
  });
  return m;
}

if (m.isGroup) {
    // [AUDIT-FIX] cache mtime (read-only) — hindari parse tiap pesan grup
    const antiswgcList = readHotJson('./database/antiswgc.json', [])

    if (antiswgcList.includes(m.chat)) {

        const botMurni = NXL.decodeJid ? NXL.decodeJid(NXL.user.id) : (NXL.user.id.split(':')[0] + '@s.whatsapp.net')


        const isFromMe = m.key.fromMe || (typeof areJidsSameUser === 'function' ? areJidsSameUser(m.sender, botMurni) : m.sender === botMurni)

        if (!isFromMe) {


            const isStatusGrupMessage =
                m.message?.groupStatusUpdateMessage ||
                m.type === 'groupStatusUpdateMessage' ||
                m.messageStubType === 91 ||
                (m.message && Object.keys(m.message).some(key => key.toLowerCase().includes('status')))

            if (isStatusGrupMessage) {

                const groupAdmins = participants.filter(p => p.admin).map(p => p.id) || []
                const isAdmin = groupAdmins.some(adminId => typeof areJidsSameUser === 'function' ? areJidsSameUser(adminId, m.sender) : adminId === m.sender)


                if (!isAdmin && !isCreator) {
                    if (isBotAdmins) {

                        await NXL.sendMessage(m.chat, {
                            text: `📵 *ANTI STATUS GRUP AKTIF*\n\n@${m.sender.split('@')[0]} terdeteksi mengirimkan Promosi Status Grup! Pesan telah dihapus oleh bot karena dilarang.`,
                            mentions: [m.sender]
                        }, { quoted: m })


                        try {
                            await NXL.sendMessage(m.chat, {
                                delete: {
                                    remoteJid: m.chat,
                                    fromMe: false,
                                    id: m.key.id,
                                    participant: m.sender
                                }
                            })
                        } catch (err) {
                            console.error('Gagal menghapus status grup:', err)
                        }
                    }
                }
            }
        }
    }
}
const BLACKLIST_SWGC_PATH = './database/blacklistswgc.json'
const BLACKLIST_JPM_PATH  = './database/blacklistjpm.json'
const JOINFILTER_PATH     = './database/joinfilter.json'


let _blacklistJpmCache = null
let _joinFilterCache = null
function loadBlacklistJpm() {
  if (_blacklistJpmCache !== null) return _blacklistJpmCache
  try { _blacklistJpmCache = JSON.parse(fs.readFileSync(BLACKLIST_JPM_PATH, 'utf8')) }
  catch { _blacklistJpmCache = [] }
  return _blacklistJpmCache
}
function saveBlacklistJpm(list) {
  _blacklistJpmCache = list
  fs.writeFileSync(BLACKLIST_JPM_PATH, JSON.stringify(list, null, 2))
}

function loadBlacklistSwgc() {
  try {
    return JSON.parse(fs.readFileSync(BLACKLIST_SWGC_PATH, 'utf8'))
  } catch {
    return []
  }
}

function saveBlacklistSwgc(list) {
  fs.writeFileSync(BLACKLIST_SWGC_PATH, JSON.stringify(list, null, 2))
}


function loadJoinFilter() {
  if (_joinFilterCache !== null) return _joinFilterCache
  try { _joinFilterCache = JSON.parse(fs.readFileSync(JOINFILTER_PATH, 'utf8')) }
  catch { _joinFilterCache = [] }
  return _joinFilterCache
}
function saveJoinFilter(list) {
  _joinFilterCache = list
  fs.writeFileSync(JOINFILTER_PATH, JSON.stringify(list, null, 2))
}

// [PERF] Hindari moment-timezone per pesan (lambat). WIB = UTC+7 (tanpa DST),
// jadi cukup geser 7 jam lalu ambil "HH:mm:ss" — hasil identik untuk perbandingan.
const timee = new Date(Date.now() + 25200000).toISOString().slice(11, 19)
if(timee < "23:59:00"){
var waktuucapan = 'Selamat Malam 🌃'}
if(timee < "19:00:00"){
var waktuucapan = 'Selamat targettang 🌆'}
if(timee < "18:00:00"){
var waktuucapan = 'Selamat Sore 🌅'}
if(timee < "15:00:00"){
var waktuucapan = 'Selamat Siang 🏙'}
if(timee < "10:00:00"){
var waktuucapan = 'Selamat Pagi 🌄'}
if(timee < "05:00:00"){
var waktuucapan = 'Selamat Subuh 🌉'}
if(timee < "03:00:00"){
var waktuucapan = 'Tengah Malam 🌌'}
const isSelfChat = from === botNumber || from === (NXL.user.id.split(':')[0] + '@s.whatsapp.net')
const msgType = m.type || ''
const quotedType = m.quoted?.type || ''
const isImageMsg   = msgType === 'imageMessage'
const isVideoMsg   = msgType === 'videoMessage'
const isAudioMsg   = msgType === 'audioMessage' || msgType === 'pttMessage'
const isStickerMsg = msgType === 'stickerMessage'
const isDocMsg     = msgType === 'documentMessage' || msgType === 'documentWithCaptionMessage'
const hasMedia     = isImageMsg || isVideoMsg || isAudioMsg || isStickerMsg || isDocMsg

const hasContent = body || hasMedia

if (!global._processedMsgIds) global._processedMsgIds = new Set()
const _msgId = m.key?.id || ''
// [PERF-FIX] Dedup lapisan kedua. Skip check untuk type 'append' (re-emit internal
// dari sticker hash & interactiveResponse) agar fitur tsb tetap berfungsi.
// Pesan WA biasa (notify) yang lolos dedup awal (message.js) hanya bisa sampai sini
// sekali saja — dedup awal sudah memblokir duplikat notify.
const _isAppend = chatUpdate && chatUpdate.type === 'append'
if (_msgId && !_isAppend) {
  if (global._processedMsgIds.has(_msgId)) return
  global._processedMsgIds.add(_msgId)
  // [PATCH C] FIFO eviction — hapus ID TERLAMA, bukan clear-all.
  if (global._processedMsgIds.size > 500) {
    const _removeCount = global._processedMsgIds.size - 400
    let _i = 0
    for (const _oldId of global._processedMsgIds) {
      if (_i++ >= _removeCount) break
      global._processedMsgIds.delete(_oldId)
    }
  }
}

// [PERF-FIX] Log pesan HANYA setelah dedup lolos — memastikan setiap pesan
// hanya mencetak log SATU kali di panel Pterodactyl (fix double/triple log).
if (m.message) {
  // [LOG RINGAN] 1 baris/pesan (info: waktu, tempat, pengirim, isi). Mengurangi
  // volume stdout -> log lebih real-time & event-loop lebih ringan. Pesan
  // sistem/kosong dilewati (tidak ada spam kolom kosong).
  let _isi = (budy && String(budy).trim()) ? String(budy).trim() : ''
  if (!_isi && m.mtype && m.mtype !== 'conversation' && m.mtype !== 'extendedTextMessage') {
    _isi = '[' + String(m.mtype).replace(/Message$/, '') + ']'
  }
  if (_isi) {
    if (_isi.length > 300) _isi = _isi.slice(0, 300) + '…'
    const _jam = new Date().toLocaleTimeString('id-ID', { hour12: false })
    if (m.isGroup) {
      // Tampilkan NAMA GRUP dengan jelas (dari cache, tanpa fetch tambahan → tetap cepat)
      const _gn = groupName || global.allGroupsCache?.[from]?.subject || global.groupCache?.[from]?.subject || from.split('@')[0]
      console.log(`${chalk.green(`[${_jam}]`)} ${chalk.magenta('GRUP:')} ${chalk.cyan(_gn)} ${chalk.yellow('» ' + (pushname || senderNumber) + ':')} ${_isi}`)
    } else {
      console.log(`${chalk.green(`[${_jam}]`)} ${chalk.blue('PRIVATE')} ${chalk.yellow((pushname || senderNumber) + ':')} ${_isi}`)
    }
  }
}

if (!isCmd && global.autoJoinGc && budy && budy.includes('chat.whatsapp.com/')) {
  const linksFound = budy.split(/\s+|\n/).filter(l => l.includes('chat.whatsapp.com/'))
  if (linksFound.length) {
    // Filter check (tetap dipertahankan)
    const filterAktif = !!global.autoJoinGcFilter
    const filterList = filterAktif ? loadJoinFilter() : []
    const sumberJid = m.isGroup ? from : sender

    if (filterAktif) {
      const senderNum = sender.replace(/@.*$/, '')
      const allowed = filterList.some(v =>
        v.id === sumberJid ||
        v.id.replace(/@.*$/, '') === senderNum
      )
      if (!allowed) { /* tidak diizinkan oleh filter */ }
      else {
        for (const link of linksFound) {
          const invite = link.split('chat.whatsapp.com/')[1]?.split(/[?\s#]/)[0]?.trim()
          if (invite && invite.length >= 10) autoJoinGcEnqueue(invite, sumberJid)
        }
      }
    } else {
      for (const link of linksFound) {
        const invite = link.split('chat.whatsapp.com/')[1]?.split(/[?\s#]/)[0]?.trim()
        if (invite && invite.length >= 10) autoJoinGcEnqueue(invite, sumberJid)
      }
    }
  }
}

if (!isCmd && hasContent && !m.key.fromMe && global.db?.users?.[m.sender]?.NXL !== false) {
  try {
    const GROQ_KEY = global.groqKey

    const SESSION_PATH = './database/sessionai.json'
    if (!fs.existsSync(SESSION_PATH)) safeWriteJSON(SESSION_PATH, {})
    let sessionDb = JSON.parse(fs.readFileSync(SESSION_PATH, 'utf8'))
    if (!sessionDb[m.sender]) {
      sessionDb[m.sender] = {
        activeSession: 'default',
        sessions: { default: { name: 'Default', createdAt: Date.now(), history: [] } }
      }
    }
    const activeSessionId = sessionDb[m.sender].activeSession || 'default'
    if (!sessionDb[m.sender].sessions[activeSessionId]) {
      sessionDb[m.sender].sessions[activeSessionId] = { name: activeSessionId, createdAt: Date.now(), history: [] }
    }
    const activeSession = sessionDb[m.sender].sessions[activeSessionId]
    const history = activeSession.history.slice(-10)

    let userContent = []
    let useVision = false
    let mediaDesc = ''

    if (isImageMsg) {
      try {
        const msgObj = m.quoted && quotedType === 'imageMessage' ? m.quoted : m
        const stream = await downloadContentFromMessage(
          msgObj.message?.imageMessage || msgObj.msg,
          'image'
        )
        let chunks = []
        for await (const chunk of stream) chunks.push(chunk)
        const imgBuffer = Buffer.concat(chunks)
        const imgBase64 = imgBuffer.toString('base64')
        const imgMime = (msgObj.message?.imageMessage?.mimetype || 'image/jpeg')

        userContent.push({
          type: 'image_url',
          image_url: { url: `data:${imgMime};base64,${imgBase64}` }
        })
        if (body) userContent.push({ type: 'text', text: body })
        else userContent.push({ type: 'text', text: 'Tolong deskripsikan gambar ini secara detail.' })
        useVision = true
      } catch (e) {
        console.log('[NXLAI IMAGE ERROR]', e.message)
        mediaDesc = '[User mengirim gambar tapi gagal diproses]'
      }
    }

    else if (isStickerMsg) {
      try {
        const msgObj = m.quoted && quotedType === 'stickerMessage' ? m.quoted : m
        const stream = await downloadContentFromMessage(
          msgObj.message?.stickerMessage || msgObj.msg,
          'sticker'
        )
        let chunks = []
        for await (const chunk of stream) chunks.push(chunk)
        const stickerBuffer = Buffer.concat(chunks)
        const stickerBase64 = stickerBuffer.toString('base64')

        userContent.push({
          type: 'image_url',
          image_url: { url: `data:image/webp;base64,${stickerBase64}` }
        })
        userContent.push({ type: 'text', text: body || 'Ini adalah sticker. Deskripsikan isinya.' })
        useVision = true
      } catch (e) {
        console.log('[NXLAI STICKER ERROR]', e.message)
        mediaDesc = '[User mengirim sticker]'
      }
    }

    else if (isAudioMsg) {
      const isPtt = msgType === 'pttMessage' || quotedType === 'pttMessage'
      mediaDesc = isPtt
        ? '[User mengirim pesan suara (voice note). Kamu tidak bisa mendengarnya, tapi balas dengan ramah dan minta mereka ketik pesannya.]'
        : '[User mengirim file audio. Kamu tidak bisa memutarnya, tapi balas dengan ramah.]'
      if (body) mediaDesc += ` Caption: "${body}"`
    }

    else if (isVideoMsg) {
      mediaDesc = '[User mengirim video. Kamu tidak bisa menontonnya, tapi balas dengan ramah.]'
      if (body) mediaDesc += ` Caption: "${body}"`
    }

    else if (isDocMsg) {
      try {
        const msgObj = m.quoted && (quotedType === 'documentMessage' || quotedType === 'documentWithCaptionMessage') ? m.quoted : m
        const docMsg = msgObj.message?.documentMessage || msgObj.message?.documentWithCaptionMessage?.message?.documentMessage || msgObj.msg
        const docMime = docMsg?.mimetype || ''
        const docName = docMsg?.fileName || 'dokumen'
        const readableMimes = ['text/plain', 'application/json', 'text/markdown', 'text/html', 'text/css', 'application/javascript', 'text/x-python', 'text/x-java', 'application/x-sh', 'application/sql']

        if (readableMimes.some(t => docMime.includes(t) || docName.match(/\.(txt|json|md|html|css|js|py|java|sh|sql|php|kt|cpp|ts)$/i))) {
          const stream = await downloadContentFromMessage(docMsg, 'document')
          let chunks = []
          for await (const chunk of stream) chunks.push(chunk)
          const docText = Buffer.concat(chunks).toString('utf8').slice(0, 4000)
          mediaDesc = `[User mengirim dokumen: "${docName}"]\nIsi dokumen:\n\`\`\`\n${docText}\n\`\`\``
        } else {
          mediaDesc = `[User mengirim dokumen: "${docName}" (${docMime}). Kamu tidak bisa membacanya langsung, tapi beri tahu user apa yang bisa dibantu.]`
        }
        if (body) mediaDesc += `\nCaption dari user: "${body}"`
      } catch (e) {
        console.log('[NXLAI DOC ERROR]', e.message)
        mediaDesc = '[User mengirim dokumen tapi gagal dibaca]'
        if (body) mediaDesc += ` Caption: "${body}"`
      }
    }

    else {
      if (body && quotedType === 'imageMessage' && m.quoted) {
        try {
          const stream = await downloadContentFromMessage(
            m.quoted.message?.imageMessage || m.quoted.msg,
            'image'
          )
          let chunks = []
          for await (const chunk of stream) chunks.push(chunk)
          const imgBase64 = Buffer.concat(chunks).toString('base64')
          const imgMime = m.quoted.message?.imageMessage?.mimetype || 'image/jpeg'
          userContent.push({ type: 'image_url', image_url: { url: `data:${imgMime};base64,${imgBase64}` } })
          userContent.push({ type: 'text', text: body })
          useVision = true
        } catch (e) {
          console.log('[NXLAI QUOTED IMAGE ERROR]', e.message)
        }
      } else if (body && quotedType === 'stickerMessage' && m.quoted) {
        try {
          const stream = await downloadContentFromMessage(
            m.quoted.message?.stickerMessage || m.quoted.msg,
            'sticker'
          )
          let chunks = []
          for await (const chunk of stream) chunks.push(chunk)
          const stickerBase64 = Buffer.concat(chunks).toString('base64')
          userContent.push({ type: 'image_url', image_url: { url: `data:image/webp;base64,${stickerBase64}` } })
          userContent.push({ type: 'text', text: body })
          useVision = true
        } catch (e) {
          console.log('[NXLAI QUOTED STICKER ERROR]', e.message)
        }
      }
    }


    if (!global._tipeFileRegex) {
      const tipeFileSrc = {
        'python|\\.py': 'py',
        'javascript|\\.js': 'js',
        'nodejs|\\.js': 'js',
        'html': 'html',
        'css': 'css',
        'json': 'json',
        'catatan|txt|dokumen': 'txt',
        'markdown|\\.md': 'md',
        'php': 'php',
        'java(?!script)': 'java',
        'kotlin': 'kt',
        'bash|shell|\\.sh': 'sh',
        'sql': 'sql',
        'cpp|c\\+\\+': 'cpp',
      }
      global._tipeFileRegex = Object.entries(tipeFileSrc).map(([k, v]) => [new RegExp(k, 'i'), v])
    }
    let ext = null
    for (const [rx, val] of global._tipeFileRegex) {
      if (rx.test(body)) { ext = val; break }
    }
    const mintaFile = /buatkan?|simpan|dalam bentuk|as file|jadikan file|kirim sebagai/i.test(body) && ext
    const isCodeFile = mintaFile && !['txt', 'md', 'json'].includes(ext)
    const prompt = isCodeFile ? global.promptCoding : global.promptUmum

    let response = ''

    if (useVision) {
      const visionRes = await axios.post(
        `${global.apiGroq}/openai/v1/chat/completions`,
        {
          model: 'meta-llama/llama-4-scout-17b-16e-instruct',
          messages: [
            { role: 'system', content: prompt },
            ...history,
            { role: 'user', content: userContent }
          ],
          max_tokens: 1024
        },
        { headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${GROQ_KEY}`
        }}
      )
      response = visionRes.data?.choices?.[0]?.message?.content || 'Tidak ada jawaban.'
    } else {
      const textInput = mediaDesc || body
      const textRes = await axios.post(
        `${global.apiGroq}/openai/v1/chat/completions`,
        {
          model: 'llama-3.3-70b-versatile',
          messages: [
            { role: 'system', content: prompt },
            ...history,
            { role: 'user', content: textInput }
          ]
        },
        { headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${GROQ_KEY}`
        }}
      )
      response = textRes.data?.choices?.[0]?.message?.content || 'Tidak ada jawaban.'
    }

    const historyUserContent = mediaDesc
      ? mediaDesc
      : (useVision ? (body || '[gambar]') : body)
    activeSession.history.push({ role: 'user', content: historyUserContent })
    activeSession.history.push({ role: 'assistant', content: response })
    if (activeSession.history.length > 20) activeSession.history = activeSession.history.slice(-20)
    sessionDb[m.sender].sessions[activeSessionId] = activeSession
    // [PATCH E] Re-read file TERBARU lalu merge HANYA key user ini sebelum tulis atomik.
    // Mencegah lost-update saat 2+ user memakai AI bersamaan (root cause F3-01):
    // tiap invokasi hanya menimpa entri miliknya, bukan seluruh snapshot basi.
    try {
      let _latestSession = {}
      try { _latestSession = JSON.parse(fs.readFileSync(SESSION_PATH, 'utf8')) } catch { _latestSession = {} }
      if (!_latestSession || typeof _latestSession !== 'object') _latestSession = {}
      _latestSession[m.sender] = sessionDb[m.sender]
      safeWriteJSON(SESSION_PATH, _latestSession)
    } catch {
      safeWriteJSON(SESSION_PATH, sessionDb)
    }

    if (mintaFile) {
      if (!fs.existsSync('./Tmp')) fs.mkdirSync('./Tmp', { recursive: true })
      const filePath = `./Tmp/nxl_${m.sender.split('@')[0]}.${ext}`
      const cleanResponse = isCodeFile
        ? response.replace(/^```[\w]*\n?/gm, '').replace(/```$/gm, '').trim()
        : response.trim()
      fs.writeFileSync(filePath, cleanResponse)
      const mimeMap = {
        py: 'text/x-python', js: 'application/javascript', html: 'text/html',
        css: 'text/css', json: 'application/json', txt: 'text/plain',
        md: 'text/markdown', php: 'application/x-php', java: 'text/x-java',
        kt: 'text/x-kotlin', sh: 'application/x-sh', sql: 'application/sql',
        cpp: 'text/x-c++src',
      }
      await NXL.sendMessage(from, {
        document: fs.readFileSync(filePath),
        mimetype: mimeMap[ext] || 'text/plain',
        fileName: `Output.${ext}`,
        caption: `📄 *File .${ext} siap digunakan!*`
      }, { quoted: m })
      fs.unlinkSync(filePath)
    } else {
      m.reply(response)
    }

  } catch (err) {
    console.log('[NXLAI ERROR]', err?.response?.data || err.message)
    m.reply('⚠️ Terjadi error saat memproses pesan kamu.')
  }
  return
}

// [CLEANUP] Blok enforcement warn ke-2 dihapus: redundan dgn Block 1 di atas
// (kondisinya subset & Block 1 selalu jalan lebih dulu + resolusi JID lebih benar).

if (m.isGroup && !m.key.fromMe && !isAdmins && !isCreator) {
  try {
    const urlRegex = /https?:\/\/[^\s]+|www\.[^\s]+/gi
    const waLinkRegex = /chat\.whatsapp\.com\/[A-Za-z0-9]+/i
    const hasUrl = urlRegex.test(budy) || waLinkRegex.test(budy)
    const inAntilink  = Antilink.includes(m.chat)
    const inAntilink2 = Antilink2.includes(m.chat)

    if (hasUrl && (inAntilink || inAntilink2)) {
      let freshMeta = null
      try { freshMeta = await NXL.groupMetadata(from) } catch {}
      const freshParticipants = freshMeta?.participants || []
      const botNum = NXL.user.id.split(':')[0]
      const isBotAdminFresh = freshParticipants.some(p =>
        p.admin && (
          (p.id && p.id.replace(/[^0-9]/g, '') === botNum) ||
          (p.jid && p.jid.replace(/[^0-9]/g, '') === botNum) ||
          (p.lid && store?.contacts && Object.values(store.contacts).some(c => c.lid === p.lid && c.id && c.id.replace(/[^0-9]/g, '') === botNum))
        )
      )
      if (isBotAdminFresh) {
        if (inAntilink) {
          try { await NXL.sendMessage(from, { delete: m.key }) } catch {}
          await NXL.sendMessage(from, {
            text: `⚠️ @${m.sender.split('@')[0]} dilarang mengirim link di grup ini!\n📵 Pesan telah dihapus.`,
            mentions: [m.sender]
          })
          return
        }
        if (inAntilink2) {
          try { await NXL.sendMessage(from, { delete: m.key }) } catch {}
          await NXL.sendMessage(from, {
            text: `🚫 @${m.sender.split('@')[0]} dikick karena mengirim link!\n❌ Pesan dihapus & member dikeluarkan.`,
            mentions: [m.sender]
          })
          try { await NXL.groupParticipantsUpdate(from, [m.sender], 'remove') } catch {}
          return
        }
      }
    }
  } catch {}
}

// [PATCH C] Welcome DIPINDAH ke sumber tunggal: handler 'group-participants.update'
// di index.js (event kanonik WhatsApp untuk add/remove/promote/demote).
// Blok welcome di sini sebelumnya menyebabkan welcome terkirim GANDA karena
// menangani groupParticipantsMessage yang datang bersamaan dengan event tsb
// (root cause RC-6/F1-05). Dihapus agar setiap member yang join tepat 1x welcome.


if (m.isGroup && !m.key.fromMe && antitoxicList.includes(m.chat) && body) {
  if (!isAdmins && !isCreator) {
    try {
      const _normalizeToxic = (str) => str
        .toLowerCase()
        .normalize('NFKD').replace(/[\u0300-\u036f]/g, '')
        .replace(/0/g,'o').replace(/1/g,'i').replace(/3/g,'e')
        .replace(/4/g,'a').replace(/5/g,'s').replace(/8/g,'b')
        .replace(/\$/g,'s').replace(/@/g,'a').replace(/\+/g,'t')
        .replace(/[^a-z]/g,'')
      const _bodyNorm = _normalizeToxic(body)
      const _foundWord = toxicWords.find(w => _bodyNorm.includes(_normalizeToxic(w)))
      if (_foundWord) {
        try { await NXL.sendMessage(m.chat, { delete: m.key }) } catch {}
        await NXL.sendMessage(m.chat, {
          text: `⚠️ @${m.sender.split('@')[0]} pesan kamu dihapus karena mengandung kata yang tidak diizinkan!`,
          mentions: [m.sender]
        }, { quoted: m })
      }
    } catch {}
  }
}


if (m.isGroup && !m.key.fromMe && antibotList.includes(m.chat)) {
  if (!isAdmins && !isCreator) {
    try {
      const targetId = m.key.id || ''
      let reasons = []
      let isBotDetected = false
      const nonHexChars = targetId.match(/[^0-9A-F]/gi)

      if (nonHexChars) {
        const uniqueChars = [...new Set(nonHexChars)].join('').toUpperCase()
        reasons.push(`Format ID Invalid: Mengandung [ ${uniqueChars} ]`)
        isBotDetected = true
      }

      if (targetId.length !== 32 && !targetId.startsWith('3EB0') && !targetId.startsWith('3A')) {
        reasons.push(`Panjang ID Tidak Wajar (${targetId.length} digit)`)
        isBotDetected = true
      }

      // [PATCH D] Prefix '3EB0' TIDAK lagi dianggap bot (root cause RC-7/F1-04).
      // Realita protokol WhatsApp: '3EB0' dipakai oleh pengguna WhatsApp Web/Desktop
      // yang SAH sekaligus sebagian bot Baileys — tidak bisa dibedakan dari prefix.
      // Menandainya sebagai bot meng-kick 100% pengguna WA Web (merusak grup & memicu
      // risiko ban). Trade-off diterima: sebagian bot ber-ID 3EB0 mungkin lolos, namun
      // itu jauh lebih aman daripada menendang anggota manusia yang sah.

      if (targetId.startsWith('BAE5')) {
        // 'BAE5' spesifik untuk client Baileys lama & TIDAK dipakai WhatsApp Web,
        // sehingga tetap menjadi sinyal bot yang aman.
        reasons.push('Terdeteksi ID Baileys Lama (BAE5)')
        isBotDetected = true
      }

      if (isBotDetected) {
        const actionType = antibotSettings[m.chat] || 'delete'
        const timeNow = new Date().toLocaleTimeString('id-ID', { timeZone: 'Asia/Jakarta' })

        const report = `⚡ *SISTEM KEAMANAN GRUP* ⚡\n` +
          `⏰ *Waktu:* ${timeNow}\n` +
          `👤 *User:* @${m.sender.split('@')[0]}\n` +
          `🔑 *ID:* ${targetId}\n\n` +
          `🚫 *Terdeteksi Unauthorized Client/Bot*\n` +
          `${reasons.map(r => `> • ${r}`).join('\n')}\n\n` +
          `🔨 *Sanksi:* ${actionType === 'kick' ? 'HAPUS & TENDANG' : 'HAPUS PESAN'}`

        await NXL.sendMessage(m.chat, {
          text: report,
          mentions: [m.sender]
        }, { quoted: m })

        try { await NXL.sendMessage(m.chat, { delete: m.key }) } catch {}

        if (actionType === 'kick') {
          // [PATCH D] Verifikasi status admin bot dgn metadata SEGAR sebelum kick,
          // supaya keputusan destruktif tidak memakai cache basi (root cause F4-02).
          // Hanya berjalan di jalur kick (jarang) sehingga tidak membanjiri groupMetadata.
          let _botAdminFresh = isBotAdmins
          try {
            const _fm = await NXL.groupMetadata(m.chat)
            const _botJidFresh = NXL.user.id.split(':')[0] + '@s.whatsapp.net'
            _botAdminFresh = (_fm?.participants || []).some(p =>
              ((p.id && areJidsSameUser(p.id, _botJidFresh)) || (p.lid && p.lid === p.id)) && p.admin
            )
          } catch {}
          if (_botAdminFresh) {
            await sleep(2000)
            try { await NXL.groupParticipantsUpdate(m.chat, [m.sender], 'remove') } catch {}
          } else {
            await NXL.sendMessage(m.chat, { text: '⚠️ Bot bukan admin, tidak bisa melakukan kick.' })
          }
        }
      }
    } catch {}
  }
}






if (!m.key.fromMe && !isCmd) {
  try {
    if (!global.db.users) global.db.users = {}
    if (!global.db.users[m.sender]) global.db.users[m.sender] = {}
    const _afkUser = global.db.users[m.sender]
    if (_afkUser.afkTime && _afkUser.afkTime > 0) {
      const _afkDuration = Date.now() - _afkUser.afkTime
      const _afkMenit = Math.floor(_afkDuration / 60000)
      const _afkDetik = Math.floor((_afkDuration % 60000) / 1000)
      const _afkStr = _afkMenit > 0 ? `${_afkMenit} menit ${_afkDetik} detik` : `${_afkDetik} detik`
      _afkUser.afkTime = -1
      _afkUser.afkReason = ''
      await NXL.sendMessage(from, {
        text: `👋 *${m.pushName}* sudah kembali!\n⏱️ AFK selama: ${_afkStr}`,
        mentions: [m.sender]
      }, { quoted: m })
    }
  } catch {}
}


if (m.isGroup && !m.key.fromMe) {
  try {
    const _mentionedJids = m.message?.extendedTextMessage?.contextInfo?.mentionedJid || []
    if (_mentionedJids.length > 0 && global.db.users) {
      for (const _mentionedJid of _mentionedJids) {
        const _mentionedUser = global.db.users[_mentionedJid]
        if (_mentionedUser?.afkTime && _mentionedUser.afkTime > 0) {
          const _afkDur = Date.now() - _mentionedUser.afkTime
          const _afkMin = Math.floor(_afkDur / 60000)
          const _afkSec = Math.floor((_afkDur % 60000) / 1000)
          const _afkStr = _afkMin > 0 ? `${_afkMin} menit ${_afkSec} detik` : `${_afkSec} detik`
          await NXL.sendMessage(from, {
            text: `💤 @${_mentionedJid.split('@')[0]} sedang AFK${_mentionedUser.afkReason ? `: _${_mentionedUser.afkReason}_` : ''}\n⏱️ Sudah AFK selama: ${_afkStr}`,
            mentions: [_mentionedJid]
          }, { quoted: m })
        }
      }
    }
  } catch {}
}

switch(command) {


case 'anticallgc': {
  if (!m.isGroup) return m.reply('❌ Perintah ini hanya untuk grup!')
  if (!isAdmins && !isCreator) return m.reply('❌ Hanya admin grup yang bisa menggunakan perintah ini!')

  const subCmd = args[0]?.toLowerCase()

  if (!subCmd || subCmd === 'status') {
    const isOn = anticallgcList.includes(m.chat)
    return m.reply(
      `📵 *STATUS ANTI CALL GRUP*\n\n` +
      `Grup: *${groupName}*\n` +
      `Status: ${isOn ? '✅ *AKTIF*' : '❌ *NONAKTIF*'}\n\n` +
      `Gunakan:\n• \`.anticallgc on\` — aktifkan\n• \`.anticallgc off\` — nonaktifkan`
    )
  }

  if (subCmd === 'on') {
    if (anticallgcList.includes(m.chat)) return m.reply('✅ Anti Call GC sudah aktif di grup ini!')
    anticallgcList.push(m.chat)
    global.anticallgcList = anticallgcList
    safeWriteJSON(ANTICALLGC_PATH, anticallgcList)
    return m.reply(
      `✅ *Anti Call Grup DIAKTIFKAN!*\n\n` +
      `📵 Jika ada member yang melakukan panggilan di grup:\n` +
      `• Panggilan otomatis di-*reject*\n` +
      `• Member akan di-*kick* (jika bot adalah admin)\n\n` +
      `_Berlaku untuk semua member kecuali admin & owner._`
    )
  }

  if (subCmd === 'off') {
    const idx = anticallgcList.indexOf(m.chat)
    if (idx === -1) return m.reply('❌ Anti Call GC sudah nonaktif di grup ini!')
    anticallgcList.splice(idx, 1)
    global.anticallgcList = anticallgcList
    safeWriteJSON(ANTICALLGC_PATH, anticallgcList)
    return m.reply('❌ *Anti Call Grup DINONAKTIFKAN!*\n\nPanggilan di grup ini kini diizinkan.')
  }

  return m.reply('❓ Perintah tidak dikenal.\nGunakan: `.anticallgc on` / `.anticallgc off` / `.anticallgc status`')
}



case 'sessionai':
case 'sesi': {
  const SESSION_PATH = './database/sessionai.json'

  const loadSession = () => {
    try { return JSON.parse(fs.readFileSync(SESSION_PATH, 'utf8')) } catch { return {} }
  }
  const saveSession = (data) => safeWriteJSON(SESSION_PATH, data)

  let sessionDb = loadSession()
  if (!sessionDb[m.sender]) {
    sessionDb[m.sender] = {
      activeSession: 'default',
      sessions: {
        default: { name: 'Default', createdAt: Date.now(), history: [] }
      }
    }
    saveSession(sessionDb)
  }

  const sub = args[0]?.toLowerCase()
  const param = args.slice(1).join(' ').trim()

  if (!sub) {
    const userData = sessionDb[m.sender]
    const active = userData.activeSession
    const sessionList = Object.entries(userData.sessions)
      .map(([id, s]) => `${id === active ? '✅' : '○'} *${id}* — ${s.name} (${s.history.length / 2 | 0} pesan)`)
      .join('\n')
    return m.reply(
      `╔══ 🤖 *SESSION AI* ══╗\n` +
      `║ User: ${m.sender.split('@')[0]}\n` +
      `║ Aktif: *${active}*\n` +
      `╚══════════════════╝\n\n` +
      `*Daftar Session:*\n${sessionList}\n\n` +
      `*Command:*\n` +
      `• \`.sessionai baru <nama>\` — buat session baru\n` +
      `• \`.sessionai pakai <id>\` — ganti session aktif\n` +
      `• \`.sessionai hapus <id>\` — hapus session\n` +
      `• \`.sessionai reset\` — hapus history session aktif\n` +
      `• \`.sessionai info\` — detail session aktif\n` +
      `• \`.sessionai list\` — daftar semua session`
    )
  }

  if (sub === 'baru' || sub === 'new' || sub === 'create') {
    if (!param) return m.reply('Contoh: `.sessionai baru Belajar Coding`')
    const newId = param.toLowerCase().replace(/\s+/g, '_').replace(/[^a-z0-9_]/g, '').slice(0, 20) + '_' + Date.now().toString().slice(-4)
    if (sessionDb[m.sender].sessions[newId]) return m.reply(`Session *${newId}* sudah ada.`)

    sessionDb[m.sender].sessions[newId] = {
      name: param.slice(0, 50),
      createdAt: Date.now(),
      history: []
    }
    sessionDb[m.sender].activeSession = newId
    saveSession(sessionDb)

    return m.reply(
      `✅ *Session baru dibuat & diaktifkan!*\n\n` +
      `ID: \`${newId}\`\n` +
      `Nama: ${param}\n` +
      `Owner: ${m.sender.split('@')[0]}\n\n` +
      `AI akan mulai dari awal di session ini. Kirim pesan apa saja untuk mulai!`
    )
  }

  if (sub === 'pakai' || sub === 'switch' || sub === 'use') {
    if (!param) return m.reply('Contoh: `.sessionai pakai default`')
    const targetId = param.toLowerCase()
    if (!sessionDb[m.sender].sessions[targetId]) {
      const available = Object.keys(sessionDb[m.sender].sessions).join(', ')
      return m.reply(`❌ Session *${targetId}* tidak ditemukan.\nYang tersedia: ${available}`)
    }
    sessionDb[m.sender].activeSession = targetId
    saveSession(sessionDb)
    const sess = sessionDb[m.sender].sessions[targetId]
    return m.reply(
      `🔄 *Session diganti!*\n\n` +
      `Aktif: *${targetId}*\n` +
      `Nama: ${sess.name}\n` +
      `History: ${sess.history.length / 2 | 0} pesan tersimpan`
    )
  }

  if (sub === 'hapus' || sub === 'delete' || sub === 'del') {
    if (!param) return m.reply('Contoh: `.sessionai hapus <id>`')
    const targetId = param.toLowerCase()
    if (targetId === 'default') return m.reply('❌ Session *default* tidak bisa dihapus.')
    if (!sessionDb[m.sender].sessions[targetId]) return m.reply(`❌ Session *${targetId}* tidak ditemukan.`)

    delete sessionDb[m.sender].sessions[targetId]
    if (sessionDb[m.sender].activeSession === targetId) {
      sessionDb[m.sender].activeSession = 'default'
    }
    saveSession(sessionDb)
    return m.reply(`🗑️ Session *${targetId}* berhasil dihapus.\nSekarang aktif: *${sessionDb[m.sender].activeSession}*`)
  }

  if (sub === 'reset' || sub === 'clear') {
    const active = sessionDb[m.sender].activeSession
    sessionDb[m.sender].sessions[active].history = []
    saveSession(sessionDb)
    return m.reply(`🔁 History session *${active}* berhasil direset!\nAI dimulai dari awal.`)
  }

  if (sub === 'info') {
    const active = sessionDb[m.sender].activeSession
    const sess = sessionDb[m.sender].sessions[active]
    const created = new Date(sess.createdAt).toLocaleString('id-ID', { timeZone: 'Asia/Jakarta' })
    const msgCount = sess.history.length / 2 | 0
    return m.reply(
      `📊 *Info Session Aktif*\n\n` +
      `ID: \`${active}\`\n` +
      `Nama: ${sess.name}\n` +
      `Owner: ${m.sender.split('@')[0]}\n` +
      `Dibuat: ${created}\n` +
      `Pesan tersimpan: ${msgCount} percakapan\n` +
      `Total token estimasi: ~${sess.history.reduce((a, h) => a + (h.content?.length || 0), 0)} chars`
    )
  }

  if (sub === 'list') {
    const userData = sessionDb[m.sender]
    const active = userData.activeSession
    const list = Object.entries(userData.sessions)
      .map(([id, s], i) => {
        const tgl = new Date(s.createdAt).toLocaleDateString('id-ID')
        return `${i + 1}. ${id === active ? '✅' : '○'} \`${id}\`\n   📌 ${s.name}\n   🗓 ${tgl} | 💬 ${s.history.length / 2 | 0} pesan`
      }).join('\n\n')
    return m.reply(`📋 *Semua Session — ${m.sender.split('@')[0]}*\n\n${list}`)
  }

  return m.reply(`Sub-command tidak dikenal. Ketik \`.sessionai\` untuk bantuan.`)
}
break

case 'buatcatatan': {
  if (!text) return reply('Contoh: .buatcatatan materi python')
  try {
    const res = await axios.post(`${global.apiGroq}/openai/v1/chat/completions`, {
      model: 'llama-3.3-70b-versatile',
      messages: [{ role: 'user', content: text }]
    }, { headers: { Authorization: `Bearer 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` } })
    const hasil = res.data.choices[0].message.content

    if (!fs.existsSync('./Tmp')) fs.mkdirSync('./Tmp', { recursive: true })
    const filePath = `./Tmp/catatan_${m.sender.split('@')[0]}.txt`
    fs.writeFileSync(filePath, hasil)

    await NXL.sendMessage(from, {
      document: fs.readFileSync(filePath),
      mimetype: 'text/plain',
      fileName: 'catatan.txt',
      caption: '📄 Catatan lo udah jadi!'
    }, { quoted: m })

    fs.unlinkSync(filePath)
  } catch (err) {
    reply('Gagal: ' + err.message)
  }
}
break

case "gemini": {
  if (!text) return reply(`contoh .${command} halo`)

  const { GoogleGenAI } = require("@google/genai")
  const apikey = "AIzaSyC-bHKyLeYQVt1LG2frmHOoVr2cF5yfe8s"
  const ai = new GoogleGenAI({ apiKey: apikey })

  const web = await ai.models.generateContent({
    model: "gemini-2.0-flash",
    contents: text,
    config: {
      systemInstruction: "kamu adalah ai XresXDigital"
    }
  })

  const hasil = web.text
  reply(`${hasil}`)
  break
}
case 'gpt':
case 'ai': {
  if (!text) return m.reply(`*PENGGUNAAN SALAH*\n\nFormat: .${command} [pertanyaan]\nContoh: .${command} Siapa pencipta Linux?`)

  try {
    await m.reply('[ PROCESS ] Menghubungkan ke server AI anonim')

    if (!global.gptanonSessions) {
      global.gptanonSessions = {}
    }
    const userSession = global.gptanonSessions[m.sender] || null

    const ip = Array.from({ length: 4 }, () => Math.floor(Math.random() * 255)).join(".")
    const jar = new CookieJar()
    const client = wrapper(
      axios.create({
        jar,
        withCredentials: true,
        headers: {
          Origin: "https://gptanon.com",
          Referer: "https://gptanon.com/chat",
          Accept: "text/event-stream",
          "Content-Type": "application/json",
          "User-Agent": "Mozilla/5.0 (Linux; Android 10; Mobile) AppleWebKit/537.36 Chrome/139.0.0.0 Safari/537.36",
          "X-Forwarded-For": ip,
          "X-Real-IP": ip,
          "CF-Connecting-IP": ip
        }
      })
    )

    const payload = {
      message: text,
      modelIds: ["google/gemma-3-27b-it"]
    }

    if (userSession) {
      payload.sessionId = userSession
    }

    const res = await client.post(
      "https://gptanon.com/api/chat/stream",
      payload,
      {
        responseType: "stream",
        validateStatus: () => true
      }
    )

    const responseData = await new Promise((resolve, reject) => {
      let currentSession = userSession
      let answer = ""
      let buffer = ""

      res.data.on("data", chunk => {
        buffer += chunk.toString()
        const events = buffer.split("\n\n")
        buffer = events.pop()

        for (const event of events) {
          const line = event.trim()
          if (!line.startsWith("data:")) continue

          try {
            const json = JSON.parse(line.replace(/^data:\s*/, ""))
            if (json.type === "session" && json.sessionId) {
              currentSession = json.sessionId
            }
            if (json.type === "complete" && json.content) {
              answer = json.content.replace(/^thought\s*/i, "").trim()
            }
          } catch {}
        }
      })

      res.data.on("end", () => {
        resolve({
          status: true,
          sessionId: currentSession,
          result: answer
        })
      })

      res.data.on("error", err => {
        reject({
          status: false,
          message: err.message
        })
      })
    })

    if (responseData.status && responseData.result) {
      if (responseData.sessionId) {
        global.gptanonSessions[m.sender] = responseData.sessionId
      }

      let txt = `*AI RESPONSE*\n\n`
      txt += `${responseData.result}\n\n`
      txt += `====================================\n`
      txt += `_Model: gemma-3-27b-it_`

      await m.reply(txt)
    } else {
      m.reply('[-] Gagal mendapatkan respon valid dari server AI')
    }

  } catch (e) {
    console.error(e)
    m.reply(`[ ERROR SYSTEM ] ${e.message || 'Terjadi kesalahan saat memproses data AI'}`)
  }
}
break

case "menu": {
  wek = `BAGAIMANA CARA MENDAPATKAN DIA :(`

  // Fix: private chats using @lid JIDs silently fail to receive
  // interactiveMessage/viewOnceMessage payloads. Normalize to the
  // real phone-number JID before building/sending the menu.
  let menuChatJid = m.chat
  if (!m.isGroup && menuChatJid.endsWith('@lid')) {
    try {
      let _resolved = null
      if (typeof NXL.signalRepository?.lidMapping?.getPNForLID === 'function') {
        _resolved = await NXL.signalRepository.lidMapping.getPNForLID(menuChatJid)
      }
      // [FIX] Jangan fallback ke senderNumber saat fromMe: untuk pesan dari nomor
      // bot sendiri, senderNumber = nomor BOT (bukan lawan chat), sehingga menu
      // salah terkirim ke room bot sendiri. Untuk fromMe, biarkan JID chat asli (X).
      if (!_resolved && senderNumber && !m.key.fromMe) {
        _resolved = senderNumber + '@s.whatsapp.net'
      }
      if (_resolved) menuChatJid = _resolved
    } catch (e) {
      // fall back to original m.chat if resolution fails
    }
  }

  const createlink = {
    key: {
      participant: `0@s.whatsapp.net`,
      remoteJid: "status@broadcast"
    },
    message: {
      extendedTextMessage: {
        text: `*${ownername} 2.0*`,
        contextInfo: {
          externalAdReply: {
            title: "${ownername} Developer",
            body: "Tap untuk buka link",
            sourceUrl: `${global.waMe}/6287728163189`,
            mediaType: 1,
            renderLargerThumbnail: true,
            showAdAttribution: true
          }
        }
      }
    }
  }

const menuRows = [
  ['semua menu',       'Lihat semua fitur bot',   '.allmenu'],
  ['owner menu',     'Khusus pemilik bot',       '.ownermenu'],
]

let sections = [
  {
    title: 'List Menu',
    highlight_label: 'allmenu',
    rows: menuRows.map(([title, description, id]) => ({ title, description, id }))
  }
]

let listMessage = { title: 'List Menu', sections }

  let msg = generateWAMessageFromContent(menuChatJid, {
    viewOnceMessage: {
      message: {
        messageContextInfo: {
          deviceListMetadata: {},
          deviceListMetadataVersion: 2
        },
        interactiveMessage: proto.Message.InteractiveMessage.create({
          contextInfo: {
            mentionedJid: [m.sender],
            isForwarded: false,
            forwardedNewsletterMessageInfo: {
              newsletterJid: global.idsal,
              newsletterName: wm,
              serverMessageId: -1
            },
            businessMessageForwardInfo: {
              businessOwnerJid: NXL.decodeJid(NXL.user.id)
            },
            quotedMessage: createlink.message,
            remoteJid: createlink.key.remoteJid,
            participant: createlink.key.participant,
          },
          body: proto.Message.InteractiveMessage.Body.create({
          }),
          footer: proto.Message.InteractiveMessage.Footer.create({
           text: Styles(`simple whatsapp bot made by ${ownername}`)
           }),
          header: proto.Message.InteractiveMessage.Header.create({
            title: `╔═❏ *${global.namabot}*
║ Versi: ${global.versibot}
║ Mode: ${botMode.toUpperCase()}
║ Owner: ${owner}
║ Dev: ${global.ownername}
╚═❏

  ╭◙  *Other Menu*
  ┆• .pushkontak
  ┆• .jedapush
  ┆• .jedajpm
  ┆• .jpm
  ┆• .bljpm
  ┆• .listbljpm
  ┆• .delbljpm
  ┆• .cekidch
  ╰◙͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏͏
  ╭◙  *Main Menu*
  ┆• .igdownload
  ┆• .removebg
  ┆• .suarateks
  ┆• .emojimix
  ┆• .pinterest
  ┆• .ttsearch
  ┆• .bratvid
  ┆• .ssweb
  ┆• .ffstalk
  ┆• .tiktok
  ┆• .brat
  ┆• .sfile
  ┆• .play
  ╰◙
  ╭◙  *Tools Media*
  ┆• .tourl
  ╰◙
  ╭◙ 𝗔𝘂𝘁𝗼 𝗠𝗲𝗻𝘂
  ┆• .autojpmswgc
  ┆• .setautoswgc
  ┆• .autojpm
  ┆• .setjpm
  ┆• .autojoingc
  ╰◙
  ╭◙ 𝗝𝗣𝗠 𝗠𝗲𝗻𝘂
  ┆•.jaser
  ┆• .jaserht
  ┆• .jedajaser
  ┆• .jpmswgc
  ┆• .listproduk
  ┆• .addproduk
  ┆• .autopromo
  ┆• .setpromo
  ┆• .payment
  ╰◙
  ╭◙  *Group Menu*
  ┆• .creategc
  ┆• .leavegc
  ┆• .joingc
  ┆• .listgrup
  ┆• .notifgrup
  ┆• .status grup
  ┆• .antilink
  ┆• .welcome
  ┆• .hidetag
  ┆• .kick
  ┆• .add
  ┆• .mute
  ┆• .unmute
  ┆• .muteinfo
  ┆• .warn
  ┆• .warnlist
  ┆• .resetwarn
  ┆• .notifgrup
  ╰◙
  ╭◙  *Owner Menu*
  ┆• .addowner <nomor>
  ┆• .delowner <nomor>
  ┆• .listowner
  ┆• .restart
  ┆• .public
  ┆• .self
  ┆• .backup
  ╰◙
  ╭◙  *Ai Menu*
  ┆• .nxlai on/off
  ┆• .sessionai
  ┆• .imagine
  ┆• .gpt
  ╰◙
`,

            hasMediaAttachment: true,
            ...(global._menuMediaCache || (global._menuMediaCache = await prepareWAMessageMedia(
              { image: fs.readFileSync(`./lib/image/image.jpg`), gifPlayback: true },
              { upload: NXL.waUploadToServer }
            ).catch(() => { global._menuMediaCache = null; return {} })))
          }),
          nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
            buttons: [
               {
 "name": "cta_url",
  "buttonParamsJson": `{\"display_text\":\"owner\",\"url\":\"${global.waMe}/${owner}\",\"merchant_url\":\"${global.waMe}/${owner}\"}`
   }
            ]
          })
        })
      }
    }
  }, {})

  await NXL.relayMessage(msg.key.remoteJid, msg.message, {
    messageId: msg.key.id
  })
}
break

case "allmenu": {
  wek = `BAGAIMANA CARA MENDAPATKAN DIA :(`

  const createlink = {
    key: {
      participant: `0@s.whatsapp.net`,
      remoteJid: "status@broadcast"
    },
    message: {
      extendedTextMessage: {
        text: `*${ownername} 2.0*`,
        contextInfo: {
          externalAdReply: {
            title: "${ownername} Developer",
            body: "Tap untuk buka link",
            sourceUrl: `${global.waMe}/6287728163189`,
            mediaType: 1,
            renderLargerThumbnail: true,
            showAdAttribution: true
          }
        }
      }
    }
  }

const menuRows = [
  ['semua menu',       'Lihat semua fitur bot',   '.allmenu'],
  ['owner menu',     'Khusus pemilik bot',       '.ownermenu'],
]

let sections = [
  {
    title: 'List Menu',
    highlight_label: 'allmenu',
    rows: menuRows.map(([title, description, id]) => ({ title, description, id }))
  }
]

let listMessage = { title: 'List Menu', sections }

  let msg = generateWAMessageFromContent(m.chat, {
    viewOnceMessage: {
      message: {
        messageContextInfo: {
          deviceListMetadata: {},
          deviceListMetadataVersion: 2
        },
        interactiveMessage: proto.Message.InteractiveMessage.create({
          contextInfo: {
            mentionedJid: [m.sender],
            isForwarded: false,
            forwardedNewsletterMessageInfo: {
              newsletterJid: global.idsal,
              newsletterName: wm,
              serverMessageId: -1
            },
            businessMessageForwardInfo: {
              businessOwnerJid: NXL.decodeJid(NXL.user.id)
            },
            quotedMessage: createlink.message,
            remoteJid: createlink.key.remoteJid,
            participant: createlink.key.participant,
          },
          body: proto.Message.InteractiveMessage.Body.create({
          }),
          footer: proto.Message.InteractiveMessage.Footer.create({
           text: Styles(`simple whatsapp bot made by ${ownername}`)
           }),
          header: proto.Message.InteractiveMessage.Header.create({
            title: `╔═❏ *${global.namabot}*
║ Versi: ${global.versibot}
║ Mode: ${botMode.toUpperCase()}
║ Owner: ${owner}
║ Dev: ${global.ownername}
╚═❏


  ╭◙  *Menu*
  ┆• .menu
  ┆• .allmenu
  ┆• .ownermenu
  ┆• .nsfwmenu
  ┆• .listcase
  ╰◙
  ╭◙  *Info & Status*
  ┆• .ping
  ┆• .statusbot
  ┆• .jumlahuser
  ┆• .iduser
  ┆• .idch
  ┆• .cekidgc
  ┆• .cekch
  ┆• .debugquoted
  ┆• .teslink
  ╰◙
  ╭◙  *AI Menu*
  ┆• .ai
  ┆• .gpt
  ┆• .nxlai
  ┆• .gemini
  ┆• .deepseek
  ┆• .imagine
  ┆• .gimage
  ┆• .sessionai
  ┆• .resetai
  ╰◙
  ╭◙  *Downloader Menu*
  ┆• .tiktok
  ┆• .douyin
  ┆• .twitter
  ┆• .threads
  ┆• .igdl
  ┆• .gdrive
  ┆• .gitclone
  ┆• .sfile
  ┆• .play
  ┆• .playch
  ┆• .music
  ╰◙
  ╭◙  *Search & Stalk Menu*
  ┆• .yts
  ┆• .npmjs
  ┆• .github
  ┆• .searchcode
  ┆• .pinterest
  ┆• .ttsearch
  ┆• .igstalk
  ┆• .ttstalk
  ┆• .ffstalk
  ┆• .stalkroblox
  ┆• .cekcuaca
  ┆• .cekgempa
  ┆• .cekkalender
  ┆• .infolibur
  ╰◙
  ╭◙  *Sticker & Media Tools*
  ┆• .sticker
  ┆• .emojimix
  ┆• .brat
  ┆• .brat2
  ┆• .tourl
  ┆• .upscale
  ┆• .hdfoto
  ┆• .hdvideo
  ┆• .blur
  ┆• .removebg
  ┆• .readviewonce
  ┆• .toaudio
  ┆• .suarateks
  ┆• .ssweb
  ┆• .web2apk
  ╰◙
  ╭◙  *Anime Menu*
  ┆• .anime
  ┆• .animesearch
  ┆• .animedetail
  ┆• .animebeauty
  ┆• .neko
  ┆• .kitsune
  ┆• .catgirl
  ┆• .waifu
  ┆• .quotesanime
  ┆• .sfw
  ┆• .cuddle
  ┆• .hug
  ┆• .kiss
  ┆• .pat
  ┆• .smile
  ┆• .blush
  ┆• .dance
  ╰◙
  ╭◙  *Fun Menu*
  ┆• .artinama
  ┆• .fitnah
  ┆• .buatcatatan
  ┆• .meme
  ┆• .faktadunia
  ┆• .planet
  ┆• .pakustad
  ┆• .tafsirmimpi
  ┆• .kecocokanpasangan
  ┆• .cekbeban
  ┆• .cekbucin
  ┆• .cekgay
  ┆• .cekfemboy
  ┆• .cekjodoh
  ┆• .cekjones
  ┆• .cekkaya
  ┆• .cekkodam
  ┆• .cekmasadepan
  ┆• .ceksange
  ┆• .cekstress
  ┆• .cekwibu
  ╰◙
  ╭◙  *Game Menu*
  ┆• .asahotak
  ┆• .caklontong
  ┆• .family100
  ┆• .tebakgambar
  ┆• .tebakhero
  ┆• .tebakgenshin
  ┆• .tebakkata
  ┆• .tebaklirik
  ┆• .tebaklagu
  ┆• .tebakbendera
  ┆• .tebakmakanan
  ┆• .tebaklogo
  ┆• .tebakgame
  ┆• .tebakanime
  ┆• .tebakhewan
  ┆• .tebakinggris
  ┆• .tebakjkt
  ┆• .tebakjorok
  ┆• .tebakkalimat
  ┆• .sambungkata
  ┆• .susunkata
  ┆• .siapakahaku
  ┆• .lengkapikalimat
  ┆• .nyerah
  ╰◙
  ╭◙  *Islami Menu*
  ┆• .islami
  ┆• .sholat
  ╰◙
  ╭◙  *JPM Menu*
  ┆• .jpm
  ┆• .jpm2
  ┆• .jpm3
  ┆• .jpmtesti
  ┆• .jpmht
  ┆• .jpmch
  ┆• .jaserht
  ┆• .jedajpm
  ┆• .setjeda
  ┆• .setjpm
  ┆• .autojpm
  ┆• .stopjpm
  ┆• .bljpm
  ┆• .listbljpm
  ┆• .delbljpm
  ╰◙
  ╭◙  *SWGC / Status Grup*
  ┆• .swgc
  ┆• .swgroup
  ┆• .jpmswgc
  ┆• .autojpmswgc
  ┆• .setautoswgc
  ┆• .stopswgc
  ┆• .blswgc
  ┆• .listblswgc
  ┆• .delblswgc
  ╰◙
  ╭◙  *Status WA Menu*
  ┆• .upsw
  ┆• .sendstatus
  ╰◙
  ╭◙  *Auto & Kontak Menu*
  ┆• .autojoingc
  ┆• .autojoingcfilter
  ┆• .addjoinfilter
  ┆• .deljoinfilter
  ┆• .listjoinfilter
  ┆• .savekontak
  ┆• .autosavekontak
  ┆• .pushkontak
  ┆• .pushkontak2
  ┆• .stoppush
  ┆• .jedapush
  ╰◙
  ╭◙  *Group Menu*
  ┆• .creategc
  ┆• .joingc
  ┆• .leavegc
  ┆• .listgrup
  ┆• .tagadmin
  ┆• .hidetag
  ┆• .kick
  ┆• .add
  ┆• .open
  ┆• .close
  ┆• .mute
  ┆• .mute-toggle
  ┆• .unmute
  ┆• .muteinfo
  ┆• .welcome
  ┆• .notifgrup
  ┆• .statusgrup
  ┆• .antilink
  ┆• .antilink2
  ┆• .antibot
  ┆• .antiswgc
  ┆• .anticallgc
  ┆• .antitoxic
  ┆• .addtoxic
  ┆• .deltoxic
  ┆• .listtoxic
  ┆• .warn
  ┆• .warnlist
  ┆• .resetwarn
  ╰◙
  ╭◙  *Toko Menu*
  ┆• .listproduk
  ┆• .addproduk
  ┆• .delproduk
  ┆• .autopromo
  ┆• .setpromo
  ┆• .payment
  ┆• .done
  ┆• .done1
  ╰◙
  ╭◙  *NSFW Menu*
  ┆• .nsfw
  ┆• .nsfwhentai
  ┆• .nsfwneko
  ┆• .nsfwass
  ┆• .nsfwboobs
  ┆• .nsfwpussy
  ┆• .nsfwecchi
  ┆• .nsfwbj
  ┆• .nsfwahegao
  ┆• .nsfwtrap
  ┆• .nsfwyuri
  ╰◙
  ╭◙  *Owner Menu*
  ┆• .owner
  ┆• .addowner
  ┆• .delowner
  ┆• .listowner
  ┆• .public
  ┆• .self
  ┆• .restart
  ┆• .backup
  ┆• .afk
  ╰◙
`,

            hasMediaAttachment: true,
            ...(global._menuMediaCache || (global._menuMediaCache = await prepareWAMessageMedia(
              { image: fs.readFileSync(`./lib/image/image.jpg`), gifPlayback: true },
              { upload: NXL.waUploadToServer }
            ).catch(() => { global._menuMediaCache = null; return {} })))
          }),
          nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
            buttons: [
               {
 "name": "cta_url",
  "buttonParamsJson": `{\"display_text\":\"owner\",\"url\":\"${global.waMe}/${owner}\",\"merchant_url\":\"${global.waMe}/${owner}\"}`
   }
            ]
          })
        })
      }
    }
  }, {})

  await NXL.relayMessage(msg.key.remoteJid, msg.message, {
    messageId: msg.key.id
  })
}
break

case "ownermenu": {
  await NXL.sendMessage(m.chat, {
    image: global.gambar1Cache || fs.readFileSync(`./lib/image/image.jpg`),
    caption: `『 👑 *Owner Menu* 👑 』
「 Khusus Pemilik Bot 」
◆━━━━━━━━━━━━━━━━◆

『 ⚙️ Pengaturan Bot 』
◇ .public ◇ .self
◇ .restart ◇ .backup

『 👥 Manajemen Owner 』
◇ .addowner ◇ .delowner
◇ .listowner

『 📢 Broadcast 』
◇ .jpm ◇ .jaser ◇ .jaserht
◇ .jedajaser ◇ .jpmswgc
◇ .jpmch ◇ .autojpm ◇ .setjpm

『 🤖 Auto System 』
◇ .autojpmswgc ◇ .setautoswgc
◇ .stopswgc ◇ .autojoingc
◇ .swgc ◇ .sendstatus ◇ .upsw

『 👥 Grup 』
◇ .creategc ◇ .leavegc
◇ .joingc ◇ .listgrup
◇ .hidetag ◇ .kick ◇ .add

◆━━━━━━━━━━━━━━━━◆
🌸 *${storename}* 🌸`,
  }, { quoted: lampuwarna })
}
break

case 'payment': {
  const qrisPaths = [
    global.qris,
    './lib/image/payment/qris.jpg',
    './lib/image/payment/qris.jpeg',
    './lib/image/payment/qris.png',
  ].filter(Boolean)

  let qrisBuffer = null
  for (const p of qrisPaths) {
    try {
      if (fs.existsSync(p)) { qrisBuffer = fs.readFileSync(p); break }
    } catch {}
  }

  if (!qrisBuffer) {
    return m.reply(
      `╭─「 *💳 ALL PAYMENT* 」\n│\n│  *E-Wallet*\n│  • DANA   : ${dana || '-'}\n│  • GoPay  : ${gopay || '-'}\n│  • OVO    : ${ovo || '-'}\n│\n│  *Rekening Bank*\n│  • ${rek || '-'}\n│\n│  _Konfirmasi transfer ke owner_\n│  📞 ${owner[0] || '-'}\n╰─「 *${storename}* 」`
    )
  }

  try {
    const qrisMedia = await prepareWAMessageMedia(
      { image: qrisBuffer },
      { upload: NXL.waUploadToServer }
    )

    let msg = generateWAMessageFromContent(m.chat, {
      viewOnceMessage: {
        message: {
          messageContextInfo: {
            deviceListMetadata: {},
            deviceListMetadataVersion: 2
          },
          interactiveMessage: proto.Message.InteractiveMessage.create({
            contextInfo: {
              mentionedJid: [m.sender],
              isForwarded: true,
              forwardedNewsletterMessageInfo: {
                newsletterJid: global.idsal || '',
                newsletterName: wm,
                serverMessageId: -1
              },
              businessMessageForwardInfo: {
                businessOwnerJid: NXL.decodeJid(NXL.user.id)
              }
            },
            body: proto.Message.InteractiveMessage.Body.create({
              text: `╭─「 *💳 ALL PAYMENT* 」\n│\n│  *E-Wallet*\n│  • DANA   : ${dana || '-'}\n│  • GoPay  : ${gopay || '-'}\n│  • OVO    : ${ovo || '-'}\n│\n│  *Rekening Bank*\n│  • ${rek || '-'}\n│\n│  _Konfirmasi transfer ke owner_\n╰─「 *${storename}* 」`
            }),
            footer: proto.Message.InteractiveMessage.Footer.create({
              text: `© ${storename}`
            }),
            header: proto.Message.InteractiveMessage.Header.create({
              title: `💳 Payment — ${storename}`,
              hasMediaAttachment: true,
              ...qrisMedia
            }),
            nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
              buttons: [
                {
                  name: "cta_url",
                  buttonParamsJson: JSON.stringify({
                    display_text: "💬 Hubungi Owner",
                    url: `${global.waMe}/${owner[0]?.replace(/[^0-9]/g, "")}`,
                    merchant_url: `${global.waMe}/${owner[0]?.replace(/[^0-9]/g, "")}`
                  })
                }
              ]
            })
          })
        }
      }
    }, {})

    await NXL.relayMessage(msg.key.remoteJid, msg.message, {
      messageId: msg.key.id
    })
  } catch (e) {
    console.error("Payment error:", e)
    m.reply(
      `╭─「 *💳 ALL PAYMENT* 」\n│\n│  *E-Wallet*\n│  • DANA   : ${dana || '-'}\n│  • GoPay  : ${gopay || '-'}\n│  • OVO    : ${ovo || '-'}\n│\n│  *Rekening Bank*\n│  • ${rek || '-'}\n│\n│  _Konfirmasi transfer ke owner_\n│  📞 ${owner[0] || '-'}\n╰─「 *${storename}* 」`
    )
  }
}
break

case 'cekidch':
case 'idch': {
    if (!text) return reply(`📌 Kirim link channel WhatsApp!\nContoh: ${global.waWeb}/channel/XXXX`)
    if (!text.includes(`${global.waWeb}/channel/`)) return m.reply("❌ Link tautan tidak valid.")

    let result = text.split(`${global.waWeb}/channel/`)[1]
    let res = await NXL.newsletterMetadata("invite", result)

    let teks = `📢 *Informasi Channel WhatsApp*

🆔 *ID:* ${res.id}
🗒️ *Nama:* ${res.name}
👥 *Total Pengikut:* ${res.subscribers}
📌 *Status:* ${res.state}
✅ *Verifikasi:* ${res.verification == "VERIFIED" ? "Terverifikasi" : "Tidak"}
    `

    const msg = generateWAMessageFromContent(m.chat, {
        viewOnceMessage: {
            message: {
                interactiveMessage: proto.Message.InteractiveMessage.create({
                    body: proto.Message.InteractiveMessage.Body.create({
                        text: teks
                    }),
                    footer: proto.Message.InteractiveMessage.Footer.create({
                        text: ""
                    }),
                    header: proto.Message.InteractiveMessage.Header.create({
                        hasMediaAttachment: false
                    }),
                    nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
                        buttons: [
                            {
                                name: "cta_copy",
                                buttonParamsJson: JSON.stringify({
                                    display_text: "📋 Salin ID",
                                    copy_code: `${res.id}`
                                })
                            }
                        ]
                    })
                })
            }
        }
    }, { quoted: m })

    return NXL.relayMessage(msg.key.remoteJid, msg.message, { messageId: msg.key.id })
}
break

case 'listgc': case 'cekidgc': {
  if (!isCreator) return m.reply(mess.owner)
  let getGroups = await NXL.groupFetchAllParticipating()
  let groups = Object.entries(getGroups).slice(0).map((entry) => entry[1])
  let anu = groups.map((v) => v.id)

  let teks = `⬣ *LIST GROUP DI BAWAH*\n\nTotal Group : ${anu.length} Group\n\n`
  let semuaId = []

  for (let x of anu) {
    let metadata2 = await NXL.groupMetadata(x)
    teks += `◉ Nama : ${metadata2.subject}\n◉ ID : ${metadata2.id}\n◉ Member : ${metadata2.participants.length}\n\n────────────────────────\n\n`
    semuaId.push(`${metadata2.subject} → ${metadata2.id}`)
  }


  const _tmpPath = `./Tmp/listgc_${Date.now()}.txt`
  if (!fs.existsSync('./Tmp')) fs.mkdirSync('./Tmp', { recursive: true })
  fs.writeFileSync(_tmpPath, semuaId.join('\n'))
  await NXL.sendMessage(from, {
    document: fs.readFileSync(_tmpPath),
    mimetype: 'text/plain',
    fileName: 'list_group.txt',
    caption: `📄 Export ${anu.length} grup berhasil.`
  }, { quoted: m })
  fs.unlinkSync(_tmpPath)


  const _allIds = semuaId.join('\n')
  const _msgGc = generateWAMessageFromContent(m.chat, {
    viewOnceMessage: {
      message: {
        interactiveMessage: proto.Message.InteractiveMessage.create({
          body: proto.Message.InteractiveMessage.Body.create({ text: teks }),
          footer: proto.Message.InteractiveMessage.Footer.create({ text: '' }),
          header: proto.Message.InteractiveMessage.Header.create({ hasMediaAttachment: false }),
          nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
            buttons: [
              {
                name: 'cta_copy',
                buttonParamsJson: JSON.stringify({
                  display_text: '📋 Salin Semua ID',
                  copy_code: _allIds
                })
              }
            ]
          })
        })
      }
    }
  }, { quoted: m })
  return NXL.relayMessage(_msgGc.key.remoteJid, _msgGc.message, { messageId: _msgGc.key.id })
}
break

case 'cekiduser':
case 'iduser': {
  let targetJid = ''


  if (m.quoted) {
    targetJid = m.quoted.sender || ''
  } else if (text) {

    const nomor = text.replace(/[^0-9]/g, '')
    if (!nomor) return reply('❌ Format tidak valid.\nContoh: .cekiduser 628xxxxxxxx atau tag/reply user')
    targetJid = nomor + '@s.whatsapp.net'
  } else if (m.mentionedJid && m.mentionedJid[0]) {
    targetJid = m.mentionedJid[0]
  } else {

    targetJid = m.sender
  }

  const nomorUser = targetJid.split('@')[0]
  let pp = null
  try { pp = await NXL.profilePictureUrl(targetJid, 'image') } catch { pp = null }

  const teks = `👤 *Informasi User WhatsApp*\n\n` +
    `🆔 *JID:* ${targetJid}\n` +
    `📱 *Nomor:* +${nomorUser}\n` +
    `🖼️ *Foto Profil:* ${pp ? 'Ada' : 'Tidak ada / Private'}`

  const msg = generateWAMessageFromContent(m.chat, {
    viewOnceMessage: {
      message: {
        interactiveMessage: proto.Message.InteractiveMessage.create({
          body: proto.Message.InteractiveMessage.Body.create({
            text: teks
          }),
          footer: proto.Message.InteractiveMessage.Footer.create({
            text: ''
          }),
          header: proto.Message.InteractiveMessage.Header.create({
            hasMediaAttachment: false
          }),
          nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
            buttons: [
              {
                name: 'cta_copy',
                buttonParamsJson: JSON.stringify({
                  display_text: '📋 Salin JID',
                  copy_code: targetJid
                })
              },
              {
                name: 'cta_copy',
                buttonParamsJson: JSON.stringify({
                  display_text: '📋 Salin Nomor',
                  copy_code: nomorUser
                })
              }
            ]
          })
        })
      }
    }
  }, { quoted: m })

  return NXL.relayMessage(msg.key.remoteJid, msg.message, { messageId: msg.key.id })
}
break

case "status": case "statusgrup": {
if (!isCreator) return m.reply(mess.owner);
if (!m.isGroup) return m.reply(mess.group);
const teks = `
- Antilink : ${Antilink.includes(m.chat) ? "✅" : "❌"}
- Antilink2 : ${Antilink2.includes(m.chat) ? "✅" : "❌"}
- Welcome : ${welcome.includes(m.chat) ? "✅" : "❌"}

_✅ = Aktif_
_❌ = Tidak Aktif_
`
return m.reply(teks)
}
break

case "done":
case "don":
case "proses":
case "ps": {
  if (!isCreator) return m.reply(mess.owner);
  if (!text) return m.reply(`*Contoh :* ${command} nama barang`);
  const status = /done|don/.test(command) ? "Transaksi Done ✅" : "Dana Telah Diterima ✅";
  const teks = `${status}

📦 Pembelian: ${text}
🗓️ Tanggal: ${tanggal(Date.now())}

📢 Cek Testimoni Pembeli:
${global.linkchannel.split("https://")[1] || "-"}

📣 Gabung Grup Share & Promosi:
${global.linkGrup.split("https://")[1] || "-"}`;
  await NXL.sendMessage(m.chat, {
    text: teks,
    contextInfo: {
    }
  }, { quoted: null });
}
break;

case 'tourl': {
    if (!quoted) return reply('Reply media (foto/video/file) yang ingin diupload.');

    const FormData = require("form-data");
    const mime = require("mime-types");
    const fs = require("fs");
    const { fromBuffer } = require("file-type");
    const axios = require("axios");


    async function uploadImgbb(buffer) {
        try {
            const IMGBB_API_KEY = '849bd793e2106ab250ec9f0956e85cbe';
            const formData = new FormData();
            formData.append("image", buffer.toString("base64"));
            const res = await axios.post(`${global.apiImgbb}/1/upload?key=${IMGBB_API_KEY}`, formData, {
                headers: formData.getHeaders()
            });
            if (!res.data.success) throw new Error(res.data.error?.message || 'Upload gagal');
            return res.data.data.url;
        } catch (err) {
            console.error("Imgbb Error:", err.message);
            return null;
        }
    }


    async function uploadFreeimage(buffer) {
        try {
            const formData = new FormData();
            formData.append("source", buffer.toString("base64"));
            formData.append("key", "6d207e02198a847aa98d0a2a901485a5");
            formData.append("format", "json");
            const res = await axios.post(`${global.apiFreeimage}/api/1/upload`, formData, {
                headers: formData.getHeaders()
            });
            if (res.data.status_code !== 200) throw new Error("Upload gagal");
            return res.data.image.url;
        } catch (err) {
            console.error("Freeimage Error:", err.message);
            return null;
        }
    }


    async function uploadTmpFiles(buffer, ext) {
        try {
            const formData = new FormData();
            formData.append("file", buffer, { filename: "file." + ext });
            const res = await axios.post(`${global.apiTmpfiles}/api/v1/upload`, formData, {
                headers: formData.getHeaders(),
                timeout: 120000
            });
            if (res.data && res.data.data && res.data.data.url) {
                return res.data.data.url.replace("tmpfiles.org/", "tmpfiles.org/dl/");
            }
            throw new Error("Upload ke tmpfiles.org gagal");
        } catch (err) {
            console.error("TmpFiles Error:", err.message);
            return null;
        }
    }


    async function uploadUguu(buffer, ext) {
        try {
            const formData = new FormData();
            formData.append("files[]", buffer, { filename: "file." + ext });
            const res = await axios.post(`${global.apiUguu}/upload`, formData, {
                headers: formData.getHeaders()
            });
            const url = res.data.files?.[0]?.url || res.data[0]?.url;
            if (!url) throw new Error("URL tidak ditemukan");
            return url;
        } catch (err) {
            console.error("Uguu Error:", err.message);
            return null;
        }
    }

    try {
        const media = await NXL.downloadAndSaveMediaMessage(quoted);
        const buffer = fs.readFileSync(media);
        const { ext } = await fromBuffer(buffer) || {};
        const filename = `file_${Date.now()}.${ext || 'bin'}`;
        const isImage = /image/.test(mime.lookup(media) || '');

        let [imgbbLink, freeimageLink, tmpFilesLink, uguuLink] = await Promise.all([
            isImage ? uploadImgbb(buffer) : Promise.resolve(null),
            isImage ? uploadFreeimage(buffer) : Promise.resolve(null),
            uploadTmpFiles(buffer, ext || 'bin'),
            uploadUguu(buffer, ext || 'bin')
        ]);

        if (!imgbbLink && !freeimageLink && !tmpFilesLink && !uguuLink) throw new Error("Semua uploader gagal");

        const formatLink = (link) => link ? link : 'Down / Bermasalah';

        let caption = `╭─ 「 UPLOAD SUCCESS 」
📂 Size: ${(buffer.length / 1024).toFixed(2)} KB
🌍 Top4Top-Uploader: ${formatLink(imgbbLink)}
🌍 Qu-Uploader: ${formatLink(freeimageLink)}
🌍 Uguu-Uploader: ${formatLink(tmpFilesLink)} ( *60* Minutes )
🌍 Pixhost-Uploader: ${formatLink(uguuLink)}
╰───────────────`;

        let msg = {
            interactiveMessage: proto.Message.InteractiveMessage.create({
                header: proto.Message.InteractiveMessage.Header.create({
                    hasMediaAttachment: false
                }),
                body: proto.Message.InteractiveMessage.Body.create({ text: caption }),
                footer: proto.Message.InteractiveMessage.Footer.create({
                    text: "Tekan tombol di bawah untuk menyalin tautan."
                }),
                nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
                    buttons: [
                        ...(imgbbLink ? [{ name: "cta_copy", buttonParamsJson: JSON.stringify({ display_text: "Copy Link Top4Top", copy_code: imgbbLink }) }] : []),
                        ...(freeimageLink ? [{ name: "cta_copy", buttonParamsJson: JSON.stringify({ display_text: "Copy Link Qu", copy_code: freeimageLink }) }] : []),
                        ...(tmpFilesLink ? [{ name: "cta_copy", buttonParamsJson: JSON.stringify({ display_text: "Copy Link Uguu", copy_code: tmpFilesLink }) }] : []),
                        ...(uguuLink ? [{ name: "cta_copy", buttonParamsJson: JSON.stringify({ display_text: "Copy Link Pixhost", copy_code: uguuLink }) }] : []),
                    ]
                })
            })
        };

        await NXL.relayMessage(m.chat, msg, { messageId: m.key.id });
        fs.unlinkSync(media);
    } catch (err) {
        reply(`❌ Gagal: ${err.message}`);
    }
}
break;
case "backupsc":
case "bck":
case "backup": {
  const isCreator = global.owner.map(v => v.replace(/[^0-9]/g, '') + '@s.whatsapp.net').includes(m.sender);
  if (!isCreator && m.sender !== botNumber) return m.reply(mess.owner);

  try {
    await m.reply("⏳ Processing Backup Script...");

    const tmpDir = "./Tmp";
    if (fs.existsSync(tmpDir)) {
      fs.readdirSync(tmpDir)
        .filter(f => !f.endsWith(".js"))
        .forEach(f => {
          try { fs.unlinkSync(`${tmpDir}/${f}`); } catch {}
        });
    }

    const archiver = require("archiver");
    const name = `Script-Backup ${ownername} ${Date.now()}`;
    const zipPath = `./${name}.zip`;
    const exclude = ["node_modules", "Session", "session", "package-lock.json", "yarn.lock", ".npm", ".cache"];

    await new Promise((resolve, reject) => {
      const output = fs.createWriteStream(zipPath);
      const archive = archiver("zip", { zlib: { level: 9 } });

      output.on("close", resolve);
      archive.on("error", reject);
      archive.pipe(output);

      fs.readdirSync(".").filter(f => !exclude.includes(f) && fs.existsSync(f)).forEach(f => {
        const stat = fs.statSync(f);
        if (stat.isDirectory()) {
          archive.directory(f, f);
        } else {
          archive.file(f, { name: f });
        }
      });

      archive.finalize();
    });

    if (!fs.existsSync(zipPath)) return m.reply("❌ Gagal membuat file zip.");

    await NXL.sendMessage(m.sender, {
      document: fs.readFileSync(zipPath),
      fileName: `${name}.zip`,
      mimetype: "application/zip",
      caption: `✅ Backup berhasil!\n📅 ${new Date().toLocaleString('id-ID')}`
    }, { quoted: m });

    fs.unlinkSync(zipPath);

    if (m.chat !== m.sender) await m.reply("✅ Script berhasil dikirim ke private chat!");

  } catch (err) {
    console.error("Backup Error:", err);
    m.reply(`❌ Backup gagal: ${err.message}`);
  }
}
break;

case "add": {
  if (!m.isGroup) return m.reply(mess.group);
  if (!isCreator && !m.isAdmin) return m.reply(mess.admin);
  if (!isBotAdmins) return m.reply(mess.botadmin);
  if (!text && !m?.quoted) return m.reply('Masukkan nomor yang ingin ditambahkan')

  let users = m?.quoted ? m?.quoted.sender : text.replace(/[^0-9]/g, '') + '@s.whatsapp.net'

  try {
    const result = await NXL.groupParticipantsUpdate(m.chat, [users], 'add')
    const status = result?.[0]?.status || result?.[0]?.content?.toString() || ''
    const statusCode = typeof status === 'number' ? status : parseInt(status) || 0

    if (statusCode === 200 || status === '200' || (typeof status === 'string' && /success/i.test(status))) {
      return NXL.sendMessage(m.chat, {
        text: `✅ Berhasil menambahkan @${users.split('@')[0]}`,
        mentions: [users]
      }, { quoted: m })
    } else if (statusCode === 403 || status === '403') {
      // Privasi tidak mengizinkan — kirim invite link sebagai fallback
      try {
        const inviteCode = await NXL.groupInviteCode(m.chat)
        const inviteLink = `https://chat.whatsapp.com/${inviteCode}`
        await NXL.sendMessage(users, {
          text: `Hai! Kamu diundang bergabung ke grup.\n\n${inviteLink}`
        })
        return NXL.sendMessage(m.chat, {
          text: `⚠️ @${users.split('@')[0]} tidak bisa ditambahkan langsung (privasi).\n📩 Undangan telah dikirim via chat pribadi.`,
          mentions: [users]
        }, { quoted: m })
      } catch (invErr) {
        return m.reply(`❌ Gagal menambahkan @${users.split('@')[0]}: Pengaturan privasi nomor tersebut tidak mengizinkan. Undangan juga gagal dikirim.`)
      }
    } else if (statusCode === 401 || status === '401') {
      // 401 = user belum menjadi kontak/nomor invalid — kirim invite sebagai fallback
      try {
        const inviteCode = await NXL.groupInviteCode(m.chat)
        const inviteLink = `https://chat.whatsapp.com/${inviteCode}`
        await NXL.sendMessage(users, {
          text: `Hai! Kamu diundang bergabung ke grup.\n\n${inviteLink}`
        })
        return NXL.sendMessage(m.chat, {
          text: `⚠️ @${users.split('@')[0]} tidak bisa ditambahkan langsung (401).\n📩 Undangan telah dikirim via chat pribadi.`,
          mentions: [users]
        }, { quoted: m })
      } catch (invErr) {
        return m.reply(`❌ Gagal menambahkan: respons WA — "401". Undangan juga gagal dikirim.`)
      }
    } else if (statusCode === 408 || status === '408') {
      return NXL.sendMessage(m.chat, {
        text: `⏳ Undangan terkirim ke @${users.split('@')[0]} (perlu persetujuan)`,
        mentions: [users]
      }, { quoted: m })
    } else if (statusCode === 409 || status === '409') {
      return m.reply(`⚠️ @${users.split('@')[0]} sudah ada di grup ini.`)
    } else {
      // Fallback generik — coba kirim invite
      try {
        const inviteCode = await NXL.groupInviteCode(m.chat)
        const inviteLink = `https://chat.whatsapp.com/${inviteCode}`
        await NXL.sendMessage(users, {
          text: `Hai! Kamu diundang bergabung ke grup.\n\n${inviteLink}`
        })
        return NXL.sendMessage(m.chat, {
          text: `⚠️ Gagal menambahkan @${users.split('@')[0]} secara langsung (${status}).\n📩 Undangan telah dikirim via chat pribadi.`,
          mentions: [users]
        }, { quoted: m })
      } catch {
        return m.reply(`⚠️ Gagal menambahkan: respons WA — ${JSON.stringify(status)}`)
      }
    }
  } catch (e) {
    // Exception = bisa jadi timeout / koneksi error — coba invite sebagai last resort
    try {
      const inviteCode = await NXL.groupInviteCode(m.chat)
      const inviteLink = `https://chat.whatsapp.com/${inviteCode}`
      await NXL.sendMessage(users, {
        text: `Hai! Kamu diundang bergabung ke grup.\n\n${inviteLink}`
      })
      return NXL.sendMessage(m.chat, {
        text: `⚠️ Gagal add langsung (${e?.message || e}).\n📩 Undangan telah dikirim via chat pribadi ke @${users.split('@')[0]}.`,
        mentions: [users]
      }, { quoted: m })
    } catch {
      return m.reply("❌ Gagal add: " + (e?.message || e))
    }
  }
}
break

case "kick":
case "kik": {
  if (!m.isGroup) return m.reply(mess.group);
  if (!isCreator && !m.isAdmin) return m.reply(mess.admin);
  if (!isBotAdmins) return m.reply(mess.botadmin);

  let target;

  if (m.mentionedJid?.[0]) {
    target = m.mentionedJid[0];
  } else if (m.quoted?.sender) {
    target = m.quoted.sender;
  } else if (text) {
    const cleaned = text.replace(/[^0-9]/g, "");
    if (cleaned) target = cleaned + "@s.whatsapp.net";
  }

  if (!target) return m.reply(`*Contoh :* .kick @tag/6283XXX`);

  try {
    await NXL.groupParticipantsUpdate(m.chat, [target], "remove");
    return NXL.sendMessage(m.chat, {
      text: `✅ Berhasil mengeluarkan @${target.split("@")[0]}`,
      mentions: [target]
    }, { quoted: m });
  } catch (err) {
    console.error("Kick error:", err);
    return m.reply("Gagal mengeluarkan anggota. Coba lagi atau cek hak akses bot.");
  }
}
break;

case "closegc":
case "close":
case "opengc":
case "open": {
  if (!m.isGroup) return m.reply(mess.group);
  if (!isCreator && !m.isAdmin) return m.reply(mess.admin);
  if (!isBotAdmins) return m.reply(mess.botadmin);

  try {
    if (command === "open" || command === "opengc") {
      await NXL.groupSettingUpdate(m.chat, 'not_announcement');
      return m.reply("Grup berhasil dibuka! Sekarang semua anggota dapat mengirim pesan.");
    }

    if (command === "close" || command === "closegc") {
      await NXL.groupSettingUpdate(m.chat, 'announcement');
      return m.reply("Grup berhasil ditutup! Sekarang hanya admin yang dapat mengirim pesan.");
    }
  } catch (error) {
    console.error("Error updating group settings:", error);
    return m.reply("Terjadi kesalahan saat mencoba mengubah pengaturan grup.");
  }
}
break;

case "ht":
case "hidetag": {
    if (!m.isGroup) return m.reply(mess.group);
    if (!isCreator) return m.reply(mess.owner);
    if (!q) return m.reply(`*Contoh :* ${command} pesannya`);
    NXL.sendMessage(from, { text : q ? q : '' , mentions: participants.map(a => a.id)}, {quoted:lampuwarna})
}
break

case "welcome": {
    if (!m.isGroup) return m.reply(mess.group);
    if (!isCreator) return m.reply(mess.owner);
    if (!text) return m.reply(`*Contoh :* ${command} on/off`);
    if (!/on|off/.test(text)) return m.reply(`*contoh:* ${command} on/off`);

    if (/on/.test(text)) {
        if (welcome.includes(m.chat))
            return m.reply("Welcome di grup ini sudah aktif ✅");

        welcome.push(m.chat);
        safeWriteJSON("./database/welcome.json", welcome);
        return m.reply("Berhasil menyalakan welcome di grup ini ✅");
    }

    if (/off/.test(text)) {
        if (!welcome.includes(m.chat))
            return m.reply("Welcome di grup ini sudah tidak aktif ❌");

        const inde = welcome.indexOf(m.chat);
        welcome.splice(inde, 1);
        safeWriteJSON("./database/welcome.json", welcome);
        return m.reply("Berhasil mematikan welcome di grup ini ✅");
    }
}
break;

case 'antitoxic': {
  if (!isCreator && !isAdmins) return m.reply(mess.owner)
  if (!m.isGroup) return m.reply(mess.group)
  if (!text) return m.reply(`*Contoh:*
• .antitoxic on
• .antitoxic off`)

  const isOn = antitoxicList.includes(m.chat)

  if (text === 'on') {
    if (isOn) return m.reply('Antitoxic sudah aktif di grup ini!')
    antitoxicList.push(m.chat)
    global.antitoxicList = antitoxicList
    safeWriteJSON(ANTITOXIC_PATH, antitoxicList)
    return m.reply(`✅ Antitoxic diaktifkan!
Pesan yang mengandung kata toxic akan otomatis dihapus.

Gunakan .addtoxic <kata> untuk menambah kata.`)
  }

  if (text === 'off') {
    if (!isOn) return m.reply('Antitoxic di grup ini sudah tidak aktif!')
    const idx = antitoxicList.indexOf(m.chat)
    if (idx !== -1) antitoxicList.splice(idx, 1)
    global.antitoxicList = antitoxicList
    safeWriteJSON(ANTITOXIC_PATH, antitoxicList)
    return m.reply('✅ Antitoxic dinonaktifkan di grup ini.')
  }
}
break

case 'addtoxic': {
  if (!isCreator && !isAdmins) return m.reply(mess.owner)
  if (!text) return m.reply('Contoh: .addtoxic <kata> Bisa beberapa sekaligus: .addtoxic kata1 kata2 kata3')

  const newWords = text.toLowerCase().split(' ').map(w => w.trim()).filter(w => w.length > 0)
  const added = []
  const exists = []

  for (const w of newWords) {
    if (toxicWords.includes(w)) {
      exists.push(w)
    } else {
      toxicWords.push(w)
      added.push(w)
    }
  }

  global.toxicWords = toxicWords
  safeWriteJSON(TOXICWORDS_PATH, toxicWords)

  let res = ''
  if (added.length) res += `✅ Berhasil ditambahkan: ${added.map(w => `*${w}*`).join(', ')}
`
  if (exists.length) res += `⚠️ Sudah ada sebelumnya: ${exists.map(w => `*${w}*`).join(', ')}`
  return m.reply(res.trim())
}
break

case 'deltoxic': {
  if (!isCreator && !isAdmins) return m.reply(mess.owner)
  if (!text) return m.reply('Contoh: .deltoxic <kata>')

  const delWords = text.toLowerCase().split(' ').map(w => w.trim()).filter(w => w.length > 0)
  const deleted = []
  const notFound = []

  for (const w of delWords) {
    const idx = toxicWords.indexOf(w)
    if (idx !== -1) {
      toxicWords.splice(idx, 1)
      deleted.push(w)
    } else {
      notFound.push(w)
    }
  }

  global.toxicWords = toxicWords
  safeWriteJSON(TOXICWORDS_PATH, toxicWords)

  let res = ''
  if (deleted.length) res += `✅ Berhasil dihapus: ${deleted.map(w => `*${w}*`).join(', ')}
`
  if (notFound.length) res += `❌ Tidak ditemukan: ${notFound.map(w => `*${w}*`).join(', ')}`
  return m.reply(res.trim())
}
break

case 'listtoxic': {
  if (!isCreator && !isAdmins) return m.reply(mess.owner)
  if (toxicWords.length === 0) return m.reply('📋 Belum ada kata toxic yang ditambahkan. Gunakan .addtoxic <kata> untuk menambah.')

  const list = toxicWords.map((w, i) => `${i + 1}. ${w}`).join('')
  return m.reply(`📋 *Daftar Kata Toxic* (${toxicWords.length} kata)

${list}`)
}
break

case "antibot": {
  if (!isCreator && !isAdmins) return m.reply(mess.owner)
  if (!m.isGroup) return m.reply(mess.group)
  if (!text) return m.reply(`*Contoh:*\n• .antibot on → aktif, hapus pesan bot\n• .antibot kick → aktif, hapus & tendang\n• .antibot off → nonaktif`)

  const isAntibotOn = antibotList.includes(m.chat)

  if (text === 'on') {
    if (isAntibotOn && antibotSettings[m.chat] !== 'kick') return m.reply('Antibot sudah aktif di grup ini!')
    if (!isAntibotOn) antibotList.push(m.chat)
    antibotSettings[m.chat] = 'delete'
    global.antibotList = antibotList
    global.antibotSettings = antibotSettings
    safeWriteJSON(ANTIBOT_PATH, antibotList)
    safeWriteJSON(ANTIBOTSET_PATH, antibotSettings)
    return m.reply('✅ Antibot diaktifkan!\nBot terdeteksi → pesan dihapus.')
  }

  if (text === 'kick') {
    if (isAntibotOn && antibotSettings[m.chat] === 'kick') return m.reply('Antibot mode kick sudah aktif!')
    if (!isAntibotOn) antibotList.push(m.chat)
    antibotSettings[m.chat] = 'kick'
    global.antibotList = antibotList
    global.antibotSettings = antibotSettings
    safeWriteJSON(ANTIBOT_PATH, antibotList)
    safeWriteJSON(ANTIBOTSET_PATH, antibotSettings)
    return m.reply('✅ Antibot diaktifkan mode KICK!\nBot terdeteksi → hapus pesan & tendang.')
  }

  if (text === 'off') {
    if (!isAntibotOn) return m.reply('Antibot di grup ini sudah tidak aktif!')
    const idx = antibotList.indexOf(m.chat)
    if (idx !== -1) antibotList.splice(idx, 1)
    delete antibotSettings[m.chat]
    global.antibotList = antibotList
    global.antibotSettings = antibotSettings
    safeWriteJSON(ANTIBOT_PATH, antibotList)
    safeWriteJSON(ANTIBOTSET_PATH, antibotSettings)
    return m.reply('✅ Antibot dinonaktifkan di grup ini.')
  }

  return m.reply(`*Contoh:*\n• .antibot on\n• .antibot kick\n• .antibot off`)
}
break

case "antilink": {
    if (!isCreator) return m.reply(mess.owner)
    if (!m.isGroup) return m.reply(mess.group);
    if (!text) return m.reply(`*Contoh :* ${command} on/off`);

    const isAntilink = Antilink.includes(m.chat);
    const isAntilink2 = Antilink2.includes(m.chat);

    if (text === "on") {
        if (isAntilink) return m.reply(`Antilink di grup ini sudah aktif!`);
        if (isAntilink2) {
            const posisi = Antilink2.indexOf(m.chat);
            if (posisi !== -1) Antilink2.splice(posisi, 1);
            safeWriteJSON("./database/antilink2.json", Antilink2);
        }
        Antilink.push(m.chat);
        safeWriteJSON("./database/antilink.json", Antilink);
        return m.reply(`Berhasil menyalakan antilink di grup ini ✅`);
    }

    if (text === "off") {
        if (!isAntilink) return m.reply(`Antilink di grup ini sudah tidak aktif!`);
        const posisi = Antilink.indexOf(m.chat);
        if (posisi !== -1) Antilink.splice(posisi, 1);
        safeWriteJSON("./database/antilink.json", Antilink);
        return m.reply(`Berhasil mematikan antilink di grup ini ✅`);
    }
}
break;

case "antilink2": {
    if (!isCreator) return m.reply(mess.owner);
    if (!m.isGroup) return m.reply(mess.group);
    if (!text) return m.reply(`*Contoh :* ${command} on/off`);

    const isAntilink = Antilink.includes(m.chat);
    const isAntilink2 = Antilink2.includes(m.chat);

    if (text === "on") {
        if (isAntilink2) return m.reply(`Antilink2 di grup ini sudah aktif!`);
        if (isAntilink) {
            const posisi = Antilink.indexOf(m.chat);
            if (posisi !== -1) Antilink.splice(posisi, 1);
            safeWriteJSON("./database/antilink.json", Antilink);
        }
        Antilink2.push(m.chat);
        safeWriteJSON("./database/antilink2.json", Antilink2);
        return m.reply(`Berhasil menyalakan antilink2 di grup ini ✅`);
    }

    if (text === "off") {
        if (!isAntilink2) return m.reply(`Antilink2 di grup ini sudah tidak aktif!`);
        const posisi = Antilink2.indexOf(m.chat);
        if (posisi !== -1) Antilink2.splice(posisi, 1);
        safeWriteJSON("./database/antilink2.json", Antilink2);
        return m.reply(`Berhasil mematikan antilink2 di grup ini ✅`);
    }
}
break;

case "jpmch": {
  if (!isCreator) return m.reply(mess.owner)
  if (global.statusjpm) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai!`)
  if (!global.botReady) return m.reply(`⏳ Bot baru saja reconnect, harap tunggu 20 detik lalu coba lagi.`)
  if (!text) return m.reply(`*Contoh :* ${command} pesannya & bisa dengan foto juga`)

  let mediaPath
  if (/image/.test(mime)) {
    mediaPath = await NXL.downloadAndSaveMediaMessage(qmsg)
  }

  const all = [...(global.store?.chats?.array || [])]
  const channelList = all.filter(c => c.id?.endsWith("@newsletter")).map(c => c.id)
  if (channelList.length < 1) {
    if (mediaPath && fs.existsSync(mediaPath)) fs.unlinkSync(mediaPath)
    return m.reply("❌ Tidak ada channel ditemukan")
  }

  const jenis = mediaPath ? "teks & foto" : "teks"
  const senderChat = m.chat
  const messageContent = mediaPath
    ? { image: fs.readFileSync(mediaPath), caption: text }
    : { text }

  global.messageJpm = messageContent

  await m.reply(`⏳ Memproses JPM ${jenis} ke *${channelList.length}* Channel...`)

  // [PATCH B] Broadcast lewat engine terpadu (lock+stop+socket+finally cleanup)
  const _res = await runBroadcast({
    lockFlag: 'statusjpm',
    stopFlag: 'stopjpm',
    targets: channelList,
    delayMs: () => global.JedaJpm || 5000,
    sendOne: async (conn, chId) => { _trackJpmMsg(await conn.sendMessage(chId, global.messageJpm)) },
    cleanup: () => { if (mediaPath && fs.existsSync(mediaPath)) fs.unlinkSync(mediaPath) }
  })

  if (_res.rejected) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai!`)

  if (global.pendingGroupsRefresh && global._nxlConn) {
    global.pendingGroupsRefresh = false
    global.prefetchAllGroups().catch(() => {})
  }

  await m.reply(`✅ JPM Channel selesai!\nTerkirim ke *${_res.sukses}/${channelList.length}* Channel.`)
}
break

case "cekch": {
  if (!isCreator) return
  const all = [...(global.store?.chats?.array || [])]
  const ch = all.filter(c => c.id?.endsWith("@newsletter"))
  return m.reply(`Total: ${all.length}\nChannel: ${ch.length}\n\n${ch.map(c => c.id).join("\n") || "kosong"}`)
}
break

case "jpm3": {
  if (!isCreator) return m.reply(mess.owner)
  if (global.statusjpm) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai atau hentikan dengan .stopjpm`)
  if (!global.botReady) return m.reply(`⏳ Bot baru saja reconnect, harap tunggu 20 detik lalu coba lagi.`)
  if (!text) return m.reply(
    `*Cara pakai .jpm3:*\n\nPisah isi tiap slide dengan tanda *|*\n\n*Contoh:*\n.jpm3 Halo ini slide 1 | Ini slide 2 | Ini slide 3\n\n_Bisa juga reply/attach foto sebagai thumbnail slide_\n_Minimal 1 slide, maksimal 10 slide_`
  )

  const rawSlides = text.split('|').map(s => s.trim()).filter(Boolean).slice(0, 10)
  if (rawSlides.length < 1) return m.reply(`❌ Tidak ada konten slide yang valid.`)

  let imgBuffer = null
  try {
    if (/image/.test(mime)) {
      const mediaPath = await NXL.downloadAndSaveMediaMessage(qmsg)
      imgBuffer = fs.readFileSync(mediaPath)
      fs.unlinkSync(mediaPath)
    }
  } catch { imgBuffer = null }

  async function uploadSlideImage(buffer) {
    const { imageMessage } = await generateWAMessageContent({ image: buffer }, { upload: NXL.waUploadToServer })
    return imageMessage
  }
  async function getDefaultImage() {
    const { imageMessage } = await generateWAMessageContent({ image: { url: `${global.apiTelegraph}/file/82afde68f917e8d0dbfe3.png` } }, { upload: NXL.waUploadToServer })
    return imageMessage
  }

  let sharedImageMessage
  try {
    sharedImageMessage = imgBuffer ? await uploadSlideImage(imgBuffer) : await getDefaultImage()
  } catch (e) {
    return m.reply(`❌ Gagal memproses gambar: ${e.message}`)
  }

  const ownerWa = `${global.waMe}/${(owner[0] || '').replace(/[^0-9]/g, '')}`
const cards = rawSlides.map((slideText) => ({
  header: { imageMessage: sharedImageMessage, hasMediaAttachment: true },
  body: { text: slideText },
  nativeFlowMessage: {
    buttons: [{
      name: 'cta_url',
      buttonParamsJson: JSON.stringify({
        display_text: ' Chat Owner',
        url: ownerWa,
        merchant_url: ownerWa
      })
    }]
  }
}))

  const buildCarousel = (targetJid) => generateWAMessageFromContent(
    targetJid,
    { viewOnceMessage: { message: { interactiveMessage: { body: {}, carouselMessage: { cards, messageVersion: 1 } } } } },
    {}
  )

  let allGroups
  try {
    allGroups = await Promise.race([
      global.getGroupsCached ? global.getGroupsCached() : NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    return m.reply(`❌ Gagal ambil daftar grup: ${e.message}`)
  }

  const groupIds = Object.keys(allGroups)
  let blacklist = []
  try { blacklist = loadBlacklistJpm() } catch { blacklist = [] }
  const blacklistIds = blacklist.map(v => v.id || v)
  const filteredGroupIds = groupIds.filter(id => !blacklistIds.includes(id))
  const skipped = groupIds.length - filteredGroupIds.length
  if (filteredGroupIds.length < 1) return m.reply(`❌ Tidak ada grup target.`)

  const senderChat = m.chat
  const jedaDetik = ((global.JedaJpm || 5000) / 1000).toFixed(1)

  let { key: _pKey } = await NXL.sendMessage(m.chat, {
    text: `⏳ JPM Slide (${rawSlides.length} slide)\n📨 Target: *${filteredGroupIds.length}* grup\n⏱️ Jeda: *${jedaDetik}* detik${skipped > 0 ? `\n⛔ Di-skip blacklist: *${skipped}*` : ''}\n\n⏳ Mengirim ke grup pertama...`
  })

  // [PATCH B] Broadcast lewat engine terpadu
  const _res = await runBroadcast({
    lockFlag: 'statusjpm',
    stopFlag: 'stopjpm',
    targets: filteredGroupIds,
    delayMs: () => global.JedaJpm || 5000,
    sendOne: async (conn, groupId) => {
      const carouselMsg = await buildCarousel(groupId)
      await conn.relayMessage(groupId, carouselMsg.message, { messageId: carouselMsg.key.id })
      _trackJpmMsg(carouselMsg)
    },
    onFirstSuccess: async (conn) => {
      await conn.sendMessage(m.chat, { text: `✅ JPM Slide berjalan!\n🚀 Grup pertama terkirim\n📨 Target: *${filteredGroupIds.length}* grup\n⏱️ Jeda: *${jedaDetik}* detik`, edit: _pKey })
    }
  })

  if (_res.rejected) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai atau hentikan dengan .stopjpm`)

  if (global.pendingGroupsRefresh && global._nxlConn) {
    global.pendingGroupsRefresh = false
    global.prefetchAllGroups?.().catch(() => {})
  }
  await NXL.sendMessage(senderChat, {
    text: `✅ JPM Slide selesai!\nTerkirim ke *${_res.sukses}/${filteredGroupIds.length}* grup.\n${skipped > 0 ? `⛔ Di-skip blacklist: *${skipped}* grup` : ''}`
  }, { quoted: m })
}
break

case "jasher": case "jpm": case "jaser": {
  if (!isCreator) return m.reply(mess.owner)
  if (!text) return m.reply(`*Contoh :* ${command} pesannya & bisa dengan foto juga`)
  if (!global.botReady) return m.reply(`⏳ Bot baru saja reconnect, harap tunggu 20 detik lalu coba lagi.`)

  let mediaPath
  if (/image/.test(mime)) {
    mediaPath = await NXL.downloadAndSaveMediaMessage(qmsg)
  }

  let allGroups
  try {
    allGroups = await global.getGroupsCached()
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}\nBot mungkin belum siap, tunggu sebentar lalu coba lagi.`)
  }

  let blacklist = []
  try { blacklist = loadBlacklistJpm() } catch { blacklist = [] }
  const blacklistIds = blacklist.map(v => v.id || v)

  // Payload murni — Baileys auto-generate link preview dari URL (identik Fix14)
  const messageContent = mediaPath
    ? { image: fs.readFileSync(mediaPath), caption: text }
    : { text }

  global.messageJpm = messageContent
  global.statusjpm = true

  const senderChat = m.chat
  const jenis = mediaPath ? "teks & foto" : "teks"

  // Fix14 pattern: while(true) loop yang membaca live cache
  const seenIds = new Set()
  let successCount = 0

  const getNextId = () => {
    const liveIds = Object.keys(global.allGroupsCache || {})
    for (const id of liveIds) {
      if (!seenIds.has(id) && !blacklistIds.includes(id)) return id
    }
    return null
  }

  const initialCount = Object.keys(allGroups).filter(id => !blacklistIds.includes(id)).length
  const initialBlacklist = Object.keys(allGroups).filter(id => blacklistIds.includes(id)).length
  await m.reply(`⏳ JPM ${jenis} dimulai!\n📨 Target: *${initialCount}* grup\n⛔ Blacklist: *${initialBlacklist}* grup\n⏱️ Jeda: *${(global.JedaJpm || 4000) / 1000}* detik`)

  while (true) {
    if (global.stopjpm) { delete global.stopjpm; break }

    const groupId = getNextId()
    if (!groupId) {
      if (global.allGroupsFetching) {
        await new Promise(r => setTimeout(r, 500))
        continue
      }
      break
    }

    seenIds.add(groupId)
    try {
      _trackJpmMsg(await NXL.sendMessage(groupId, global.messageJpm, { quoted: FakeChannel }))
      successCount++
    } catch (err) {
      console.error(`[JPM] Gagal kirim ke ${groupId}:`, err?.message || err)
    }

    const hasMore = getNextId() !== null || global.allGroupsFetching
    if (hasMore) {
      await new Promise(r => setTimeout(r, global.JedaJpm || 4000))
    }
  }

  const skipped = Object.keys(global.allGroupsCache || {}).filter(id => blacklistIds.includes(id)).length
  const totalAll = Object.keys(global.allGroupsCache || {}).length
  const targetTotal = Math.max(0, totalAll - skipped)
  const gagalTerlewat = Math.max(0, targetTotal - successCount)
  if (mediaPath && fs.existsSync(mediaPath)) fs.unlinkSync(mediaPath)
  delete global.statusjpm

  if (global.pendingGroupsRefresh && global._nxlConn) {
    global.pendingGroupsRefresh = false
    global.prefetchAllGroups().catch(() => {})
  }
  await NXL.sendMessage(senderChat, {
    text: `✅ JPM ${jenis} selesai!\n📤 Terkirim: *${successCount}* dari *${targetTotal}* grup\n❌ Gagal/terlewat: *${gagalTerlewat}* grup\n⛔ Blacklist dilewati: *${skipped}* grup`
  }, { quoted: m })
}
break

case "jpmht": {
  if (!isCreator) return m.reply(mess.owner)
  if (global.statusjpm) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai!`)
  if (!text) return m.reply(`*Contoh :* ${command} pesannya & bisa dengan foto juga`)
  if (!global.botReady) return m.reply(`⏳ Bot baru saja reconnect, harap tunggu 20 detik lalu coba lagi.`)

  let mediaPath
  if (/image/.test(mime)) {
    mediaPath = await NXL.downloadAndSaveMediaMessage(qmsg)
  }

  let allGroups
  try {
    allGroups = await global.getGroupsCached()
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}\nBot mungkin belum siap, tunggu sebentar lalu coba lagi.`)
  }
  const groupIds = Object.keys(allGroups)

  let blacklist = []
  try {
    blacklist = loadBlacklistJpm()
  } catch {
    blacklist = []
  }

  const blacklistIds = blacklist.map(v => v.id || v)
  const filteredGroupIds = groupIds.filter(id => !blacklistIds.includes(id))
  const skipped = groupIds.length - filteredGroupIds.length

  const messageContent = mediaPath
    ? { image: fs.readFileSync(mediaPath), caption: text }
    : { text }

  global.messageJpm = messageContent

  const senderChat = m.chat
  const jenis = mediaPath ? "teks & foto" : "teks"
  await m.reply(`⏳ Memproses JPM Hidetag ${jenis} ke *${filteredGroupIds.length}* grup...\n${skipped > 0 ? `⛔ *${skipped}* grup di-skip (blacklist)` : ''}`)

  // [PATCH B] Broadcast lewat engine terpadu; mentions di-set per-grup di dalam sendOne
  const _res = await runBroadcast({
    lockFlag: 'statusjpm',
    stopFlag: 'stopjpm',
    targets: filteredGroupIds,
    delayMs: () => global.JedaJpm || 5000,
    sendOne: async (conn, groupId) => {
      const _localMsg = { ...global.messageJpm }
      _localMsg.mentions = (allGroups[groupId]?.participants || []).map(e => e.jid || e.id)
      // preview via quoted FakeChannel
      _trackJpmMsg(await conn.sendMessage(groupId, _localMsg, { quoted: FakeChannel }))
    },
    cleanup: () => { if (mediaPath && fs.existsSync(mediaPath)) fs.unlinkSync(mediaPath) }
  })

  if (_res.rejected) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai!`)

  if (global.pendingGroupsRefresh && global._nxlConn) {
    global.pendingGroupsRefresh = false
    global.prefetchAllGroups().catch(() => {})
  }
  await NXL.sendMessage(senderChat, {
    text: `✅ JPM Hidetag ${jenis} selesai!\nTerkirim ke *${_res.sukses}/${filteredGroupIds.length}* grup.\n${skipped > 0 ? `⛔ Di-skip blacklist: *${skipped}* grup` : ''}`
  }, { quoted: m })
}
break

case 's': case 'sticker': case 'sgif': {

if (!quoted) throw `Balas Video/Image Dengan Caption ${prefix + command}`
if (/image/.test(mime)) {
let media = await quoted.download()
let encmedia = await NXL.sendImageAsSticker(from, media, m, { packname: global.packname, author: global.author })
await fs.unlinkSync(encmedia)
} else if (/video/.test(mime)) {
if ((quoted.msg || quoted).seconds > 11) return reply('Maksimal 10 detik!')
let media = await quoted.download()
let encmedia = await NXL.sendVideoAsSticker(from, media, m, { packname: global.packname, author: global.author })
await fs.unlinkSync(encmedia)
} else {
throw `Kirim Gambar/Video Dengan Caption ${prefix + command}\nDurasi Video 1-9 Detik`
}
}
break

case 'public': {
    if (!isCreator) return m.reply(mess.owner)
    NXL.public = true
    if (!fs.existsSync('./database')) fs.mkdirSync('./database', { recursive: true })
    safeWriteJSON('./database/botmode.json', { public: true })
    reply('*Berhasil Mengubah Ke Penggunaan Publik*')
}
break
case 'self': {
    if (!isCreator) return m.reply(mess.owner)
    NXL.public = false
    if (!fs.existsSync('./database')) fs.mkdirSync('./database', { recursive: true })
    safeWriteJSON('./database/botmode.json', { public: false })
    reply('*Sukses Berubah Menjadi Pemakaian Sendiri*')
}
break

case "setjeda": {
    if (!isCreator) return m.reply(mess.owner);

    const infoJeda = `*Contoh :*\n${command} push 5000\n${command} jpm 6000\n\nKeterangan format waktu:\n1 detik = 1000\n\nJeda waktu saat ini:\nJeda Pushkontak > ${global.JedaPushkontak}\nJeda JPM > ${global.JedaJpm}`;

    if (!text) return m.reply(infoJeda);

    let jedaArgs = text.split(" ");
    if (jedaArgs.length < 2) return m.reply(infoJeda);

    let target = jedaArgs[0].toLowerCase();
    let value = jedaArgs[1];

    if (isNaN(value)) return m.reply("Harus berupa angka!");
    let jeda = parseInt(value);

    let path = require.resolve("./settings.js");
    let data = fs.readFileSync(path, "utf-8");

    if (target === "push") {
        let newData = data.replace(/global\.JedaPushkontak\s*=\s*\d+/, `global.JedaPushkontak = ${jeda}`);
        fs.writeFileSync(path, newData, "utf-8");
        global.JedaPushkontak = jeda;
        return m.reply(`✅ Berhasil mengubah *Jeda Push Kontak* menjadi *${jeda}* ms`);
    }

    if (target === "jpm") {
        let newData = data.replace(/global\.JedaJpm\s*=\s*\d+/, `global.JedaJpm = ${jeda}`);
        fs.writeFileSync(path, newData, "utf-8");
        global.JedaJpm = jeda;
        return m.reply(`✅ Berhasil mengubah *Jeda JPM* menjadi *${jeda}* ms`);
    }

    return m.reply(`Pilihan tidak valid!\nGunakan: *push* atau *jpm*`);
}
break;

case "pushkontak":
case "puskontak": {
  if (!isCreator) return m.reply(mess.owner);
  if (!text) return m.reply(`*Contoh :* ${command} pesannya`);

  global.textpushkontak = text;

  let a
  try {
    a = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  const Data = Object.values(a);

  if (Data.length < 1) return m.reply("Tidak ada grup chat.");

  let rows = [];
  for (let u of Data) {
    const name = u.subject || "Unknown";
    rows.push({
      title: name,
      description: `Total Member: ${u.participants.length}`,
      id: `.pushkontak-response ${u.id}`
    });
  }

  await NXL.sendMessage(m.chat, {
    buttons: [
      {
        buttonId: 'action',
        buttonText: { displayText: 'Pilih Grup' },
        type: 4,
        nativeFlowInfo: {
          name: 'single_select',
          paramsJson: JSON.stringify({
            title: 'Pilih Grup',
            sections: [
              {
                title: `© Powered By ${ownername}`,
                rows: rows
              }
            ]
          })
        }
      }
    ],
    headerType: 1,
    viewOnce: true,
    text: `\nPilih Target Grup Pushkontak\n`
  }, { quoted: m });
}
  break;

case "pushkontak-response": {
  if (!isCreator) return m.reply(mess.owner)
  if (global.statuspush) return m.reply(`⚠️ Pushkontak sedang berjalan, tunggu sampai selesai!`)
  if (!global.textpushkontak) return m.reply(`Data teks pushkontak tidak ditemukan!\nSilahkan ketik *.pushkontak* pesannya`);

  const teks = global.textpushkontak
  const jidawal = m.chat

  const groupId = text.trim()
  console.log("Group ID:", groupId)

  let data
  try {
    data = await NXL.groupMetadata(groupId)
  } catch (e) {
    console.log("Error groupMetadata:", e.message)
    return m.reply(`Gagal ambil data grup!\nError: ${e.message}`)
  }

  console.log("Participants sample:", data.participants[0])

  const halls = data.participants
  .map(v => v.jid || v.id)
  .filter(id => id && id.endsWith('@s.whatsapp.net'))
  .filter(id => id !== botNumber && id.split("@")[0] !== global.owner)

  console.log("Total halls:", halls.length)

  await m.reply(`🚀 Memulai pushkontak ke dalam grup ${data.subject} dengan total member ${halls.length}`)

  delete global.textpushkontak

  // [PATCH B] Broadcast lewat engine terpadu (lock statuspush dijamin lepas via finally)
  const _res = await runBroadcast({
    lockFlag: 'statuspush',
    stopFlag: 'stoppush',
    targets: halls,
    delayMs: () => global.JedaPushkontak,
    sendOne: async (conn, mem) => { await conn.sendMessage(mem, { text: teks }, { quoted: FakeChannel }) }
  })

  if (_res.rejected) return m.reply(`⚠️ Pushkontak sedang berjalan, tunggu sampai selesai!`)

  await m.reply(`✅ Sukses pushkontak!\nPesan berhasil dikirim ke *${_res.sukses}* member.`, jidawal)
}
break

case "pushkontak2":
case "puskontak2": {
  if (!isCreator) return m.reply(mess.owner)
  if (!text || !text.includes("|")) return m.reply(`Masukan pesan & nama kontak\n*Contoh :* ${command} pesan|namakontak`)

  global.textpushkontak2 = text.split("|")[0]
  global.namakontak2 = text.split("|")[1]

  let a
  try {
    a = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  const Data = Object.values(a)
  if (Data.length < 1) return m.reply("Tidak ada grup chat.")

  let rows = []
  for (let u of Data) {
    const name = u.subject || "Unknown"
    rows.push({
      title: name,
      description: `Total Member: ${u.participants.length}`,
      id: `.pushkontak-response2 ${u.id}`
    })
  }

  await NXL.sendMessage(m.chat, {
    buttons: [
      {
        buttonId: 'action',
        buttonText: { displayText: 'Pilih Grup' },
        type: 4,
        nativeFlowInfo: {
          name: 'single_select',
          paramsJson: JSON.stringify({
            title: 'Pilih Grup',
            sections: [
              {
                title: `© Powered By ${ownername}`,
                rows: rows
              }
            ]
          })
        }
      }
    ],
    headerType: 1,
    viewOnce: true,
    text: `\nPilih Target Grup PushkontakV2\n`
  }, { quoted: m })
}
break

case "pushkontak-response2": {
  if (!isCreator) return m.reply(mess.owner)
  if (global.statuspush) return m.reply(`⚠️ Pushkontak sedang berjalan, tunggu sampai selesai!`)
  if (!global.textpushkontak2) return m.reply(`Data teks pushkontak tidak ditemukan!\nSilahkan ketik *.pushkontak2* pesannya|namakontak`)

  const teks = global.textpushkontak2
  const namaKontak = global.namakontak2
  const jidawal = m.chat
  const groupId = text.trim()

  let data
  try {
    data = await NXL.groupMetadata(groupId)
  } catch (e) {
    return m.reply(`Gagal ambil data grup!\nError: ${e.message}`)
  }

  const halls = data.participants
    .map(v => v.jid || v.id)
    .filter(id => id && id.endsWith('@s.whatsapp.net'))
    .filter(id => id !== botNumber && id.split("@")[0] !== global.owner)

  if (halls.length < 1) return m.reply(`Tidak ada member yang bisa di-push.`)

  await m.reply(`🚀 Memulai pushkontak autosave ke dalam grup *${data.subject}*\nTotal member: *${halls.length}*`)

  // [PATCH B] Broadcast lewat engine terpadu; sendOne kirim teks + vcard (feature-parity)
  const _res = await runBroadcast({
    lockFlag: 'statuspush',
    stopFlag: 'stoppush',
    targets: halls,
    delayMs: () => global.JedaPushkontak,
    sendOne: async (conn, mem) => {
      const nomorBersih = mem.split("@")[0]
      const vcard = `BEGIN:VCARD\nVERSION:3.0\nFN:${namaKontak} #${nomorBersih}\nTEL;type=CELL;type=VOICE;waid=${nomorBersih}:+${nomorBersih}\nEND:VCARD`
      await conn.sendMessage(mem, { text: teks }, { quoted: FakeChannel })
      await conn.sendMessage(mem, {
        contacts: {
          displayName: `${namaKontak} #${nomorBersih}`,
          contacts: [{ vcard }]
        }
      })
    },
    cleanup: () => { try { delete global.textpushkontak2; delete global.namakontak2 } catch {} }
  })

  if (_res.rejected) return m.reply(`⚠️ Pushkontak sedang berjalan, tunggu sampai selesai!`)

  await m.reply(`✅ Sukses pushkontak!\nTotal kontak berhasil dikirim ke *${_res.sukses}* dari *${halls.length}* member.`, jidawal)
  break
}

case "savenomor":
case "autosavekontak":
case "save": {
  if (!isCreator) return m.reply(mess.owner)

  let nomor, nama

  if (m.isGroup) {
    if (!text) return m.reply(`*Contoh penggunaan di grup:*\n${command} @tag|nama\natau reply target dengan:\n${command} nama`)

    if (m.mentionedJid[0]) {
      nomor = m.mentionedJid[0]
      nama = text.split("|")[1]?.trim()
      if (!nama) return m.reply(`Harap tulis nama setelah "|"\n*Contoh:* ${command} @tag|nama`)
    } else if (m.quoted) {
      nomor = m.quoted.sender
      nama = text.trim()
    } else if (/^\d+$/.test(text.split("|")[0])) {
      nomor = text.split("|")[0].replace(/[^0-9]/g, "") + "@s.whatsapp.net"
      nama = text.split("|")[1]?.trim()
      if (!nama) return m.reply(`Harap tulis nama setelah "|"\n*Contoh:* ${command} 628xxxx|nama`)
    } else {
      return m.reply(`*Contoh penggunaan di grup:*\n${command} @tag|nama\natau reply target dengan:\n${command} nama`)
    }
  } else {
    if (!text) return m.reply(`*Contoh penggunaan di private:*\n${command} nama`)
    nomor = m.chat
    nama = text.trim()
  }

  const contactAction = {
    fullName: nama,
    lidJid: nomor,
    saveOnPrimaryAddressbook: true
  }

  const nomorBersih = nomor.split("@")[0]

const vcard = `BEGIN:VCARD\nVERSION:3.0\nFN:${nama}\nTEL;type=CELL;type=VOICE;waid=${nomorBersih}:+${nomorBersih}\nEND:VCARD`

try {
  await NXL.sendMessage(m.chat, {
    contacts: {
      displayName: nama,
      contacts: [{ vcard }]
    }
  })
  return m.reply(`✅ Berhasil mengirim kontak\n\n- Nomor: ${nomorBersih}\n- Nama: ${nama}`)
} catch (e) {
  return m.reply(`❌ Gagal mengirim kontak!\nError: ${e.message}`)
}
}
break

case "savekontak":
case "svkontak": {
  if (!isCreator) return m.reply(mess.owner)
  if (!text) return m.reply(`Masukan namakontak\n*Contoh :* ${command} nama`)

  global.namakontak = text

  let a
  try {
    a = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  const Data = Object.values(a)
  if (Data.length < 1) return m.reply("Tidak ada grup chat.")

  let rows = []
  for (let u of Data) {
    const name = u.subject || "Unknown"
    rows.push({
      title: name,
      description: `Total Member: ${u.participants.length}`,
      id: `.savekontak-response ${u.id}`
    })
  }

  await NXL.sendMessage(m.chat, {
    buttons: [
      {
        buttonId: 'action',
        buttonText: { displayText: 'Pilih Grup' },
        type: 4,
        nativeFlowInfo: {
          name: 'single_select',
          paramsJson: JSON.stringify({
            title: 'Pilih Grup',
            sections: [
              {
                title: `© Powered By ${ownername}`,
                rows: rows
              }
            ]
          })
        }
      }
    ],
    headerType: 1,
    viewOnce: true,
    text: `\nPilih Target Grup Savekontak\n`
  }, { quoted: m })
}
break

case "savekontak-response": {
  if (!isCreator) return m.reply(mess.owner)
  if (!global.namakontak) return m.reply(`Data nama savekontak tidak ditemukan!\nSilahkan ketik *.savekontak* namakontak`)

  try {
    const groupId = text.trim()
    const res = await NXL.groupMetadata(groupId)

    const halls = res.participants
      .map(v => v.jid || v.id)
      .filter(id => id && id.endsWith('@s.whatsapp.net'))
      .filter(id => id !== botNumber && id.split("@")[0] !== global.owner)

    if (!halls.length) return m.reply("Tidak ada kontak yang bisa disimpan.")

    let existingContacts = []
    try {
      existingContacts = JSON.parse(fs.readFileSync('./database/contacts.json', 'utf8'))
    } catch {
      existingContacts = []
    }

    const newContacts = [...new Set([...existingContacts, ...halls])]
    safeWriteJSON('./database/contacts.json', newContacts)

    const vcardContent = halls.map(contact => {
      const phone = contact.split("@")[0]
      return [
        "BEGIN:VCARD",
        "VERSION:3.0",
        `FN:${global.namakontak} - ${phone}`,
        `TEL;type=CELL;type=VOICE;waid=${phone}:+${phone}`,
        "END:VCARD",
        ""
      ].join("\n")
    }).join("")

    fs.writeFileSync("./database/contacts.vcf", vcardContent, "utf8")

    if (m.isGroup) {
      await m.reply(`Berhasil membuat file kontak dari grup *${res.subject}*\n\nFile kontak telah dikirim ke private chat\nTotal *${halls.length}* kontak`)
    }

    await NXL.sendMessage(
      m.sender,
      {
        document: fs.readFileSync("./database/contacts.vcf"),
        fileName: `${res.subject}.vcf`,
        caption: `✅ File kontak berhasil dibuat\nGrup: *${res.subject}*\nTotal: *${halls.length}* kontak`,
        mimetype: "text/vcard"
      },
      { quoted: m }
    )

    delete global.namakontak
    safeWriteJSON("./database/contacts.json", [])
    fs.writeFileSync("./database/contacts.vcf", "")

  } catch (err) {
    return m.reply("Terjadi kesalahan:\n" + err.toString())
  }
}
break

case "jedajaser":
case "stopjpm": {
  if (!isCreator) return m.reply(mess.owner)
  if (!global.statusjpm) return m.reply("Jpm sedang tidak berjalan!")
  global.stopjpm = true
  return m.reply("Berhasil menghentikan jpm ✅")
}
break

case "stoppushkontak":
case "stoppush":
case "stoppus": {
  if (!isCreator) return m.reply(mess.owner)
  if (!global.statuspush) return m.reply("Pushkontak sedang tidak berjalan!")
  global.stoppush = true
  return m.reply("Berhasil menghentikan pushkontak ✅")
}
break

case "blacklistjpm":
case "bljpm": {
  if (!isCreator) return m.reply(mess.owner)

  let a
  try {
    a = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  const Data = Object.values(a)
  if (Data.length < 1) return m.reply("Tidak ada grup chat.")

  let rows = []
  for (let u of Data) {
    const name = u.subject || "Unknown"
    rows.push({
      title: name,
      description: `Total Member: ${u.participants.length}`,
      id: `.bljpm-response ${u.id}`
    })
  }

  await NXL.sendMessage(m.chat, {
    buttons: [
      {
        buttonId: 'action',
        buttonText: { displayText: 'Pilih Grup' },
        type: 4,
        nativeFlowInfo: {
          name: 'single_select',
          paramsJson: JSON.stringify({
            title: 'Pilih Grup',
            sections: [
              {
                title: `© Powered By ${ownername}`,
                rows: rows
              }
            ]
          })
        }
      }
    ],
    headerType: 1,
    viewOnce: true,
    text: `\nPilih Grup yang akan di-Blacklist JPM\n`
  }, { quoted: m })
}
break

case "bljpm-response": {
  if (!isCreator) return m.reply(mess.owner)

  const groupId = text.trim()

  let res
  try {
    res = await NXL.groupMetadata(groupId)
  } catch (e) {
    return m.reply(`Gagal ambil data grup!\nError: ${e.message}`)
  }

  let blacklist = []
  try {
    blacklist = loadBlacklistJpm()
  } catch {
    blacklist = []
  }

  if (blacklist.find(v => v.id === groupId)) {
    return m.reply(`Grup *${res.subject}* sudah ada di blacklist JPM!`)
  }

  blacklist.push({ id: groupId, name: res.subject })
  saveBlacklistJpm(blacklist)

  await m.reply(`✅ Berhasil menambahkan grup ke blacklist JPM\n\n- Grup: *${res.subject}*\n- ID: ${groupId}`)
}
break

case "listbljpm": {
  if (!isCreator) return m.reply(mess.owner)

  let blacklist = []
  try {
    blacklist = loadBlacklistJpm()
  } catch {
    blacklist = []
  }

  if (blacklist.length < 1) return m.reply("Tidak ada grup yang di-blacklist JPM.")

  const list = blacklist.map((v, i) => `${i + 1}. *${v.name}*\n    ID: ${v.id}`).join("\n\n")
  await m.reply(`📋 *List Blacklist JPM*\n\n${list}\n\nTotal: *${blacklist.length}* grup`)
}
break

case "delbljpm": {
  if (!isCreator) return m.reply(mess.owner)

  let blacklist = []
  try {
    blacklist = loadBlacklistJpm()
  } catch {
    blacklist = []
  }

  if (blacklist.length < 1) return m.reply("Tidak ada grup yang di-blacklist JPM.")

  if (!text) {
    const list = blacklist.map((v, i) => `${i + 1}. *${v.name}*`).join("\n")
    return m.reply(`Masukan nomor urut grup yang ingin dihapus\n*Contoh :* ${command} 1\n\n${list}`)
  }

  const index = parseInt(text.trim()) - 1
  if (isNaN(index) || index < 0 || index >= blacklist.length) {
    return m.reply(`Nomor tidak valid! Masukan angka 1 - ${blacklist.length}`)
  }

  const removed = blacklist.splice(index, 1)[0]
  saveBlacklistJpm(blacklist)

  await m.reply(`✅ Berhasil menghapus grup dari blacklist JPM\n\n- Grup: *${removed.name}*\n- ID: ${removed.id}`)
}
break

case "own": case "owner": {
await NXL.sendContact(m.chat, [global.owner], global.ownername, "Developer Bot", m)
}
break

case "addowner":
case "addown": {
  if (!isCreator) return m.reply(mess.owner)
  let input = m.quoted ? m.quoted.sender : m.mentionedJid[0] ? m.mentionedJid[0] : text ? text.replace(/[^0-9]/g, "") + "@s.whatsapp.net" : null
  if (!input) return m.reply(`*Contoh penggunaan :*\nketik ${command} 6285XXX`)
  let jid = input.split("@")[0]
  const _botNum = NXL.user.id.split(":")[0] + "@s.whatsapp.net"
  if (jid == global.owner || input == _botNum) return m.reply(`Nomor ${jid} sudah menjadi ownerbot.`)
  const Own = Developer
  if (Own.includes(input)) return m.reply(`Nomor ${jid} sudah menjadi ownerbot.`)
  Own.push(input)
  safeWriteJSON("./database/owner.json", Own)
  await m.reply(`Berhasil menambah owner ✅\n- ${input.split("@")[0]}`)
}
break

case "delowner":
case "delown": {
  if (!isCreator) return m.reply(mess.owner)
  let input = m.quoted ? m.quoted.sender : m.mentionedJid[0] ? m.mentionedJid[0] : text ? text.replace(/[^0-9]/g, "") + "@s.whatsapp.net" : null
  if (!input) return m.reply(`*Contoh penggunaan :*\nketik ${command} 6285XXX`)
  const Own = Developer
  if (input.toLowerCase() === "all") {
    Own.length = 0
    safeWriteJSON("./database/owner.json", Own)
    return m.reply("Berhasil menghapus semua owner ✅")
  }
  if (!Own.includes(input)) return m.reply("Nomor tidak ditemukan!")
  const index = Own.indexOf(input)
  Own.splice(index, 1)
  safeWriteJSON("./database/owner.json", Own)
  await m.reply(`Berhasil menghapus owner ✅\n- ${input.split("@")[0]}`)
}
break

case "listowner":
case "listown": {
if (!isCreator) return m.reply(mess.owner)
  const Own = JSON.parse(fs.readFileSync("./database/owner.json"))
  if (Own.length < 1) return m.reply("Tidak ada owner tambahan.")

  let teks = Own.map((i, idx) => {
    const num = i.split("@")[0]
    return `${idx + 1}. ${num}`
  }).join("\n")

  return m.reply(`*Daftar Owner Tambahan*\n\n${teks}`)
}
break

case 'creategc':
case 'creategrup': {
  if (!isCreator) return m.reply(mess.owner)
  if (!args.join(" ")) return m.reply(`Contoh: ${command} namagrup`)
  try {
    let cret = await NXL.groupCreate(args.join(" "), [])
    let response = await NXL.groupInviteCode(cret.id)
    let teks2 = `*BERHASIL MEMBUAT GRUP*

• Nama: ${cret.subject}
• Owner: @${cret.owner.split("@")[0]}
• Dibuat: ${moment(cret.creation * 1000).tz("Asia/Kolkata").format("DD/MM/YYYY HH:mm:ss")}
• ID: ${cret.id}
• Link: chat.whatsapp.com/${response}`
    m.reply(teks2)
  } catch {
    m.reply('Terjadi kesalahan')
  }
}
break

case 'leavegc': {
  try {
    if (!isCreator) return m.reply(mess.owner)
    if (!m.isGroup) return m.reply(mess.group)
    await NXL.groupLeave(m.chat)
  } catch (err) {
    console.error(err)
    m.reply('Terjadi kesalahan saat leave grup.')
  }
}
break

case 'joingc': {
  if (!isCreator) return m.reply(mess.owner)
  if (!text) return reply(`Contoh: ${prefix + command} ${global.waChat}/xxx`)

  if (!args[0] || !args[0].includes('chat.whatsapp.com')) return reply('Link tidak valid!')

  const result = args[0].split('chat.whatsapp.com/')[1]?.split('?')[0]
  if (!result) return reply('Link tidak valid!')

  reply(mess.wait)

  try {
    await NXL.groupAcceptInvite(result)
    reply('Sukses join ke grup!')
  } catch (err) {
    const code = err?.data || err?.output?.statusCode
    if (code === 400) return reply('Grup tidak ditemukan atau link tidak valid.')
    if (code === 401) return reply('Bot dikick dari grup tersebut.')
    if (code === 403) return reply('Bot tidak diizinkan masuk grup ini.')
    if (code === 409) return reply('Bot sudah ada di grup tersebut.')
    if (code === 410) return reply('URL grup telah direset.')
    if (code === 500) return reply('Grup penuh.')
    reply(`Gagal join: ${err.message}`)
  }
}
break

case "restart": {
  if (!isCreator) return m.reply(mess.owner)
  await m.reply(`🔄 Bot sedang restart...`)
  process.exit(0)
}
break

case 'ffstalk': {
  try {
    if (!text) return m.reply(`Contoh: ${command}ffstalk 12345678`)

    const MY_UID = 'Zh0tJcWKLpRGc9VMEugNF9jyGQc2'
    const MY_KEY = 'DciKiLzUuZmOzJRl7RuVFT8UPfZ8Tm'

    const regions = ['ID', 'SG', 'IND', 'BR', 'VN', 'TH', 'TW', 'US', 'RU', 'ME', 'PK']
    let apiUrl = null

    for (const region of regions) {
      const res = await fetchJson(
        `${global.apiHlgaming}/main/games/freefire/account/api?sectionName=AllData&PlayerUid=${text}&region=${region}&useruid=${MY_UID}&api=${MY_KEY}`
      )
      if (res?.result?.AccountInfo) { apiUrl = res; break }
    }

    if (!apiUrl?.result) return m.reply('❌ Akun tidak ditemukan. Pastikan UID benar.')

    const acc    = apiUrl.result.AccountInfo
    const guild  = apiUrl.result.GuildInfo
    const cap    = apiUrl.result.captainBasicInfo
    const pet    = apiUrl.result.petInfo
    const social = apiUrl.result.socialinfo
    const credit = apiUrl.result.creditScoreInfo
    const outfit = apiUrl.result.AccountProfileInfo

    function getBRRank(rp) {
      if (rp >= 6000) return '🏅 Heroic'
      if (rp >= 4200) return '💎 Diamond III'
      if (rp >= 3600) return '💎 Diamond II'
      if (rp >= 3000) return '💎 Diamond I'
      if (rp >= 2400) return '🥇 Platinum III'
      if (rp >= 2100) return '🥇 Platinum II'
      if (rp >= 1800) return '🥇 Platinum I'
      if (rp >= 1500) return '🥈 Gold III'
      if (rp >= 1200) return '🥈 Gold II'
      if (rp >= 900)  return '🥈 Gold I'
      if (rp >= 600)  return '🥉 Silver III'
      if (rp >= 300)  return '🥉 Silver II'
      return '🥉 Silver I'
    }

    function getCSRank(rp) {
      if (rp >= 3000) return '🏅 Heroic'
      if (rp >= 2100) return '💎 Diamond'
      if (rp >= 1500) return '🥇 Platinum'
      if (rp >= 900)  return '🥈 Gold'
      if (rp >= 300)  return '🥉 Silver'
      return '🥉 Bronze'
    }

    function rankBar(rp, max) {
      const pct = Math.min(Math.round((rp / max) * 10), 10)
      return '█'.repeat(pct) + '░'.repeat(10 - pct)
    }

    function formatDate(ts) {
      if (!ts) return '-'
      return new Date(parseInt(ts) * 1000).toLocaleDateString('id-ID', {
        day: '2-digit', month: 'long', year: 'numeric'
      })
    }

    function timeAgo(ts) {
      if (!ts) return '-'
      const diff = Math.floor(Date.now() / 1000) - parseInt(ts)
      if (diff < 60)     return 'Baru saja'
      if (diff < 3600)   return `${Math.floor(diff/60)} menit lalu`
      if (diff < 86400)  return `${Math.floor(diff/3600)} jam lalu`
      if (diff < 604800) return `${Math.floor(diff/86400)} hari lalu`
      return `${Math.floor(diff/604800)} minggu lalu`
    }

    const brRank  = getBRRank(acc?.BrRankPoint || 0)
    const csRank  = getCSRank(acc?.CsRankPoint || 0)
    const brBar   = rankBar(acc?.BrRankPoint || 0, 6000)
    const csBar   = rankBar(acc?.CsRankPoint || 0, 3000)

const response = `
*= FREE FIRE STALKER =*

[ PROFIL ]
| Nama      : *${acc?.AccountName || '-'}*
| UID       : ${text}
| Server    : ${acc?.AccountRegion?.toUpperCase() || '-'}
| Level     : ${acc?.AccountLevel || '-'} (${(acc?.AccountEXP || 0).toLocaleString()} EXP)
| Like      : ${(acc?.AccountLikes || 0).toLocaleString()}
| Login     : ${timeAgo(acc?.AccountLastLogin)}
| Bergabung : ${formatDate(acc?.AccountCreateTime)}
| Bio       : ${social?.AccountSignature || 'Tidak ada bio'}

[ RANK ]
| BR  : ${brRank}
|      ${brBar} ${acc?.BrRankPoint || 0} RP
|      Max: ${getBRRank(acc?.BrMaxRank || 0)}
|
| CS  : ${csRank}
|      ${csBar} ${acc?.CsRankPoint || 0} RP
|      Max: ${getCSRank(acc?.CsMaxRank || 0)}

[ GUILD ]
${guild ? `| Nama   : ${guild.GuildName}
| Level  : ${guild.GuildLevel}
| Member : ${guild.GuildMember}/${guild.GuildCapacity}
${cap ? `| Kapten : ${cap.nickname} (Lv.${cap.level})` : ''}` : '| Tidak bergabung guild'}

[ PET ]
${pet ? `| Level : ${pet.level}
| EXP   : ${pet.exp}` : '| Tidak ada pet'}

[ INFO LAIN ]
| Credit Score  : ${credit?.creditScore || 100}/100
| Mode Favorit  : ${social?.AccountPreferMode?.replace('Prefermode_', '').toUpperCase() || '-'}
| Bahasa        : ${social?.AccountLanguage?.replace('Language_', '') || '-'}

Powered By ${global.ownername}.
`.trim()

    m.reply(response)
  } catch (err) {
    console.error(err)
    m.reply('❌ Error: ' + err.message)
  }
}
break

case 'ssweb': {
  try {
    if (!text) return m.reply(`Contoh: ${command}ssweb https://google.com`)
    if (!text.startsWith('http://') && !text.startsWith('https://')) {
      text = 'https://' + text
    }
    m.reply('⏳ Mengambil screenshot...')

    const encodedUrl = encodeURIComponent(text)
    const apiUrl = `${global.apiScreenshot}/?key=YOUR_KEY&url=${encodedUrl}&dimension=1366x768&format=jpg&cacheLimit=0`
    const ssUrl = `${global.apiThumIo}/get/width/1280/crop/720/noanimate/${text}`
    await NXL.sendMessage(m.chat, {
      image: { url: ssUrl },
      caption: `*Screenshot Web*\n\n> URL: ${text}\n> Powered By ${global.ownername}.`
    }, { quoted: m })

  } catch (err) {
    console.error(err)
    m.reply('❌ Gagal screenshot. Pastikan URL valid.')
  }
}
break

case 'emojimix':
case 'mix': {
  if (!text) return m.reply(`*PENGGUNAAN SALAH*\n\nFormat: .${command} [dua_emoji]\nContoh: .${command} 😭😎`)

  const regex = emojiRegex()
  const emojis = text.match(regex)

  if (!emojis || emojis.length < 2) {
    return console.log('[-] Mohon masukkan minimal dua emoji untuk digabungkan')
  }

  try {
    await console.log('[ PROCESS ] Sedang menggabungkan kedua emoji Anda, mohon tunggu')

    const c = (emoji) => "u" + emoji.codePointAt(0).toString(16).padStart(4, "0")

    const api = (e1, e2, v) =>
      `https://www.gstatic.com/android/keyboard/emojikitchen/${v}/${c(e1)}/${c(e1)}_${c(e2)}.png`

    const versions = ["20201001", "20220823", "20230301", "20230803"]
    let resultBuffer = null

    for (const v of versions) {
      for (const [e1, e2] of [[emojis[0], emojis[1]], [emojis[1], emojis[0]]]) {
        const url = api(e1, e2, v)
        try {
          const response = await axios.get(url, {
            headers: { "User-Agent": "Postify/1.0.0" },
            responseType: "arraybuffer",
          })
          if (response.status === 200) {
            resultBuffer = Buffer.from(response.data)
            break
          }
        } catch (error) {
          continue
        }
      }
      if (resultBuffer) break
    }

    if (!resultBuffer) {
      return m.reply('[-] Kombinasi emoji tidak didukung oleh Google Emoji Kitchen')
    }

    if (typeof NXL.sendImageAsSticker === 'function') {
      await NXL.sendImageAsSticker(m.chat, resultBuffer, m, {
        packname: "Emoji Mix",
        author: "PT SONTOLOYO"
      })
    } else {
      await NXL.sendMessage(m.chat, { 
        sticker: resultBuffer 
      }, { quoted: m })
    }

  } catch (e) {
    console.error(e)
    m.reply(`[ ERROR SYSTEM ] ${e.message}`)
  }
}
break


    case 'bratfoto':
    case 'brat': {
    if (!text) return reply('Mana Text Nya')
var image = `${global.apiSiputzx}/api/m/brat?text=${text}&isAnimated=false&delay=500`
await NXL.sendImageAsSticker(m.chat, image, m, {packname: author})
NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key }})
}
break

case 'brat2':
    case 'bratvid':
    case 'bratvideo': {
    if (!text) return reply('Mana Text Nya')
var image = `${global.apiSiputzx}/api/m/brat?text=${text}&isAnimated=true&delay=500`
await NXL.sendImageAsSticker(m.chat, image, m, {packname: author})
NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key }})
}
break

case "tiktok":
      case "tt":
        {
          let momok = "`𝗧 𝗜 𝗞 𝗧 𝗢 𝗞 - 𝗗 𝗢 𝗪 𝗡 𝗟 𝗢 𝗔 𝗗`";
          if (!text.startsWith("https://")) {
            return reply("url");
          }
          await tiktokDl(q).then(async result => {
            await NXL.sendMessage(m.chat, {
              react: {
                text: "🕖",
                key: m.key
              }
            });
            if (!result.status) {
              return reply("Error!");
            }
            if (result.durations == 0 && result.duration == "0 Seconds") {
              let araara = new Array();
              let urutan = 0;
              for (let a of result.data) {
                let imgsc = await prepareWAMessageMedia({
                  image: {
                    url: `${a.url}`
                  }
                }, {
                  upload: NXL.waUploadToServer
                });
                await araara.push({
                  header: proto.Message.InteractiveMessage.Header.fromObject({
                    title: `Foto Slide Ke *${urutan += 1}*`,
                    hasMediaAttachment: true,
                    ...imgsc
                  }),
                  nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.fromObject({
                    buttons: [{
                      name: "cta_url",
                      buttonParamsJson: `{\"display_text\":\"Link Tautan Foto\",\"url\":\"${a.url}\",\"merchant_url\":\"${global.googleWeb}\"}`
                    }]
                  })
                });
              }
              const msgii = await generateWAMessageFromContent(m.chat, {
                viewOnceMessageV2Extension: {
                  message: {
                    messageContextInfo: {
                      deviceListMetadata: {},
                      deviceListMetadataVersion: 2
                    },
                    interactiveMessage: proto.Message.InteractiveMessage.fromObject({
                      body: proto.Message.InteractiveMessage.Body.fromObject({
                        text: "*TIKTOK - DOWNLOADER*"
                      }),
                      carouselMessage: proto.Message.InteractiveMessage.CarouselMessage.fromObject({
                        cards: araara
                      })
                    })
                  }
                }
              }, {
                userJid: m.sender,
                quoted: m
              });
              await NXL.relayMessage(m.chat, msgii.message, {
                messageId: msgii.key.id
              });
            } else {
              let urlVid = await result.data.find(e => e.type == "nowatermark_hd" || e.type == "nowatermark");
              await NXL.sendMessage(m.chat, {
                video: {
                  url: urlVid.url
                },
                caption: momok,
                footer: `\n${namabot}`,
                buttons: [{
                  buttonId: `.toaudio`,
                  buttonText: {
                    displayText: "ᴀᴍʙɪʟ ᴍᴜsɪᴋɴʏᴀ"
                  }
                }],
                viewOnce: true
              }, {
                quoted: m
              });
            }
          }).catch(e => console.log(e));
          await NXL.sendMessage(m.chat, {
            react: {
              text: "✅",
              key: m.key
            }
          });
        }
        break;
case "toaudio":
case "tomp3":
case "toaudio3": {
  const taMsg = m.quoted || (isMedia ? m : null)
  if (!taMsg) return reply(`Kirim/reply video, voice note, atau audio dengan caption *${prefix + command}*`)

  const taMime = (taMsg.msg || taMsg).mimetype || taMsg.mimetype || mime || ""
  if (!/video|audio/.test(taMime)) {
    return reply(`❌ Hanya video/audio yang bisa dikonversi ke mp3.\nFormat terdeteksi: *${taMime || 'tidak diketahui'}*`)
  }

  await NXL.sendMessage(m.chat, { react: { text: "⏳", key: m.key } })

  const ffmpegPath = require('@ffmpeg-installer/ffmpeg').path
  const ffmpeg = require('fluent-ffmpeg')
  ffmpeg.setFfmpegPath(ffmpegPath)

  const inputPath = path.join(os.tmpdir(), `toaudio_in_${Date.now()}`)
  const outputPath = path.join(os.tmpdir(), `toaudio_out_${Date.now()}.mp3`)

  try {
    const buffer = await taMsg.download()
    if (!buffer || buffer.length < 100) {
      await NXL.sendMessage(m.chat, { react: { text: "❌", key: m.key } })
      return reply("❌ Gagal mengambil media.")
    }

    fs.writeFileSync(inputPath, buffer)

    await new Promise((resolve, reject) => {
      ffmpeg(inputPath)
        .noVideo()
        .audioCodec('libmp3lame')
        .audioBitrate(128)
        .format('mp3')
        .on('end', resolve)
        .on('error', reject)
        .save(outputPath)
    })

    const audioBuffer = fs.readFileSync(outputPath)
    if (!audioBuffer || audioBuffer.length < 100) {
      throw new Error("Hasil konversi kosong/tidak valid.")
    }

    await NXL.sendMessage(m.chat, { react: { text: "✅", key: m.key } })
    await NXL.sendMessage(m.chat, {
      audio: audioBuffer,
      mimetype: "audio/mpeg",
      fileName: `${(taMsg.filename || 'audio').replace(/\.[^/.]+$/, '')}.mp3`
    }, { quoted: m })

  } catch (error) {
    console.error("[TOAUDIO ERROR]", error.message)
    await NXL.sendMessage(m.chat, { react: { text: "❌", key: m.key } })
    reply(`❌ Gagal mengonversi ke mp3.\n\n*Alasan:* ${error.message}`)
  } finally {
    try { fs.unlinkSync(inputPath) } catch {}
    try { fs.unlinkSync(outputPath) } catch {}
  }
}
break

case "pin":
case "pinterest": {
  if (!text) return m.reply(`*Penggunaan Salah!*\ncontoh: .${command} anime aesthetic`);


  await NXL.sendMessage(m.chat, { react: { text: '⏰', key: m.key } });


  async function pinterest(query) {
    try {
      const { data } = await axios.get(`${global.apiFaa}/faa/pinterest?q=${encodeURIComponent(query)}`);
      return data.result;
    } catch (err) {
      throw Error(err.message);
    }
  }

  try {
    let res = await pinterest(text);
    if (!res || res.length === 0) return m.reply("Gambar tidak ditemukan.");


    const jumlahGambar = 5;
    const images = res.slice(0, jumlahGambar);


    let cards = await Promise.all(images.map(async (url, i) => {
      return {
        header: proto.Message.InteractiveMessage.Header.create({
          ...(await prepareWAMessageMedia({ image: { url: url } }, { upload: NXL.waUploadToServer })),
          title: '',
          subtitle: `Gambar ${i + 1} dari ${images.length}`,
          hasMediaAttachment: true
        }),
        body: { text: `Image Result - ${i + 1}` },
        nativeFlowMessage: { buttons: [] }
      };
    }));


    let msg = generateWAMessageFromContent(
      m.chat,
      {
        viewOnceMessage: {
          message: {
            interactiveMessage: {
              body: { text: `✨ Hasil pencarian Pinterest untuk: *${text}*` },
              carouselMessage: {
                cards: cards,
                messageVersion: 1
              }
            }
          }
        }
      },
      { quoted: m }
    );


    await NXL.relayMessage(m.chat, msg.message, { messageId: msg.key.id });

  } catch (err) {
    console.error(err);
    m.reply("Terjadi kesalahan atau server API sedang down.");
  }
}
break;

case "gimage": {
  if (!text) return m.reply(`『 🎨 Gemini Image 』\n\n◇ Contoh: .gimage anime girl with sword`)
  await m.reply(`『 🎨 』 Generating gambar *${text}*...`)
  try {
    const prompt = encodeURIComponent(text)
    const seed = Math.floor(Math.random() * 99999)
    const url = `${global.apiPollinations}/prompt/${prompt}?seed=${seed}&width=1024&height=1024&nologo=true&enhance=true`
    const imageRes = await fetch(url)
    const buffer = Buffer.from(await imageRes.arrayBuffer())
    await NXL.sendMessage(m.chat, { image: buffer, caption: `『 🎨 *AI Image Generator* 』\n「 ${text} 」\n◆━━━━━━━━━━━━━━━━◆\n🌸 *${storename}* 🌸` }, { quoted: m })
  } catch (e) { m.reply(`❌ Gagal generate gambar.\n${e.message}`) }
}
break

case 'play':
case 'song': {
  if (!text) return m.reply(`*PENGGUNAAN SALAH*\n\nFormat: .${command} [judul_lagu]\nContoh: .${command} Night Changes`)

  try {
    // [REACT] Tanda proses dimulai (loading) agar tidak terkesan diam/bengong
    await NXL.sendMessage(m.chat, { react: { text: '⏳', key: m.key } })
    // Pencarian via @vreden/youtube_scraper (search) + yt-search (fallback)
    let video = null
    try {
      const searchResult = await ytdl.search(text)
      if (searchResult && Array.isArray(searchResult) && searchResult.length > 0) {
        video = searchResult.find(v => v.type === 'video') || searchResult[0]
      }
    } catch {}

    // Fallback ke yt-search jika vreden search gagal
    if (!video) {
      const ytsResult = await yts(text)
      const found = ytsResult.videos?.[0]
      if (found) video = { title: found.title, url: found.url, thumbnail: found.thumbnail, duration: found.seconds }
    }

    if (!video) { await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } }); return m.reply('❌ Hasil pencarian kosong atau lagu tidak ditemukan') }

    const videoUrl = video.url || video.videoUrl || `https://www.youtube.com/watch?v=${video.videoId || video.id}`
    const title = video.title || 'Unknown'
    const thumbnail = video.thumbnail || video.thumbnailUrl || ''
    const durSec = video.duration || video.seconds || 0
    const minutes = Math.floor(durSec / 60)
    const seconds = (durSec % 60).toString().padStart(2, '0')
    const formattedDuration = `${minutes}:${seconds}`

    let txt = `*MUSIC DOWNLOADER*\n\n`
    txt += `* TRACK   : ${title}\n`
    txt += `* DURATION: ${formattedDuration}\n`
    txt += `* SOURCE  : YouTube\n`
    txt += `====================================\n\n`
    txt += `_Sedang mengirim berkas audio, mohon tunggu_`

    if (thumbnail) {
      await NXL.sendMessage(m.chat, { image: { url: thumbnail }, caption: txt }, { quoted: m })
    } else {
      await m.reply(txt)
    }

    // Download audio via @vreden/youtube_scraper ytmp3 (primary)
    let audioUrl = null
    try {
      const dlResult = await ytdl.ytmp3(videoUrl, 128)
      audioUrl = dlResult?.download?.url || dlResult?.download || dlResult?.url || dlResult?.audio || dlResult?.result?.download?.url || dlResult?.result?.download || null
      if (typeof audioUrl !== 'string') audioUrl = null
    } catch {}

    // Fallback: lib/scrape.js ytdl API (shinoa.us.kg)
    if (!audioUrl) {
      try {
        const { ytdl: ytdlScrape } = require('./lib/scrape')
        const scrapeResult = await ytdlScrape(videoUrl)
        audioUrl = scrapeResult?.result?.download || scrapeResult?.download || scrapeResult?.url || null
      } catch {}
    }

    if (!audioUrl) { await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } }); return m.reply('❌ Gagal mendapatkan link download audio. Coba lagi nanti.') }

    await NXL.sendMessage(m.chat, {
      audio: { url: audioUrl },
      mimetype: 'audio/mpeg',
      ptt: false
    }, { quoted: m })

    // [REACT] Tanda selesai (done)
    await NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key } })

  } catch (e) {
    console.error('[PLAY ERROR]', e.message)
    await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
    m.reply('[ ERROR SYSTEM ] Gagal memproses data musik, silakan coba beberapa saat lagi')
  }
}
break

case 'playch': {
  // [PATCH E] Fix F1-06: 'elaina' tidak terdefinisi & global.mess.only.owner tidak ada.
  if (!isCreator) return m.reply(mess.owner)
  if (!text) return NXL.sendMessage(m.chat, { text: 'Contoh: .playch aku yang tersakiti' }, { quoted: m })
  if (!global.idsal) return NXL.sendMessage(m.chat, { react: { text: "❌", key: m.key } })

  const crypto = require("crypto")
  const axios = require("axios")
  const fs = require("fs")
  const { exec } = require("child_process")
  const yts = require("yt-search")
  const os = require("os")

  const stKey = "C5D58EF67A7584E4A29F6C35BBC4EB12"
  const stRegex = /^((?:https?:)?\/\/)?((?:www|m|music)\.)?(?:youtube\.com|youtu\.be)\/(?:watch\?v=)?(?:embed\/)?(?:v\/)?(?:shorts\/)?([a-zA-Z0-9_-]{11})/
  const stClient = axios.create({
    headers: {
      "content-type": "application/json",
      "origin": `${global.apiSavetubeMe}`,
      "user-agent": "Mozilla/5.0 (Android 15; Mobile; SM-F958; rv:130.0) Gecko/130.0 Firefox/130.0"
    }
  })

  const stDecrypt = async (enc) => {
    const sr = Buffer.from(enc, "base64")
    const ky = Buffer.from(stKey, "hex")
    const iv = sr.slice(0, 16)
    const dt = sr.slice(16)
    const dc = crypto.createDecipheriv("aes-128-cbc", ky, iv)
    const res = Buffer.concat([dc.update(dt), dc.final()])
    return JSON.parse(res.toString())
  }

  const stGetCdn = async () => {
    try {
      const res = await stClient.get(`${global.apiSavetubeVip}/api/random-cdn`)
      return res.data ? { status: true, data: res.data.cdn } : { status: false }
    } catch {
      return { status: false }
    }
  }

  const stDownload = async (url, format = "mp3") => {
    const id = url.match(stRegex)?.[3]
    if (!id) return { status: false, msg: "ID not found" }

    const cdn = await stGetCdn()
    if (!cdn.status) return cdn

    try {
      const info = await stClient.post(`https://${cdn.data}/v2/info`, {
        url: `${global.ytWeb}/watch?v=${id}`
      })
      const dec = await stDecrypt(info.data.data)
      const dl = await stClient.post(`https://${cdn.data}/download`, {
        id,
        downloadType: format === "mp3" ? "audio" : "video",
        quality: format === "mp3" ? "128" : format,
        key: dec.key
      })

      return {
        status: true,
        title: dec.title,
        channel: dec.channelTitle || "Unknown",
        format,
        thumb: dec.thumbnail || `${global.cdnYtimg}/vi/${id}/hqdefault.jpg`,
        duration: dec.durationLabel || dec.duration,
        dl: dl.data.data.downloadUrl,
        url: `${global.ytShort}/${id}`
      }
    } catch (e) {
      return { status: false, msg: e.message }
    }
  }

  try {
    await NXL.sendMessage(m.chat, { react: { text: "🔎", key: m.key } })

    const search = await yts(text)
    const video = search.videos.find(v => v.seconds < 900)
    if (!video) return NXL.sendMessage(m.chat, { react: { text: "❌", key: m.key } })

    const ytChannel = video.author?.name || video.author?.username || "Unknown"

    const data = await stDownload(video.url, "mp3")
    if (!data.status) return NXL.sendMessage(m.chat, { react: { text: "❌", key: m.key } })

    const audioResponse = await axios.get(data.dl, { responseType: 'arraybuffer' })
    const thumbResponse = await axios.get(data.thumb, { responseType: 'arraybuffer' })

    const tempInput = path.join(os.tmpdir(), `in_${crypto.randomBytes(4).toString('hex')}.mp3`)
    const tempOutput = path.join(os.tmpdir(), `out_${crypto.randomBytes(4).toString('hex')}.opus`)

    fs.writeFileSync(tempInput, audioResponse.data)

    await new Promise((resolve, reject) => {
      exec(`ffmpeg -y -i "${tempInput}" -c:a libopus -b:a 128k -vbr on -compression_level 10 "${tempOutput}"`, (err) => {
        if (err) reject(err)
        else resolve()
      })
    })

    const opusBuffer = fs.readFileSync(tempOutput)

    await NXL.sendMessage(global.idsal, {
      audio: opusBuffer,
      mimetype: "audio/ogg; codecs=opus",
      ptt: true,
      contextInfo: {
        forwardingScore: 999,
        isForwarded: true,
        forwardedNewsletterMessageInfo: {
          newsletterJid: global.idsal,
          serverMessageId: 100,
          newsletterName: global.ownername || global.botname
        },
        externalAdReply: {
          title: data.title,
          body: `Channel • ${ytChannel}`,
          thumbnail: thumbResponse.data,
          sourceUrl: data.url,
          mediaType: 1,
          renderLargerThumbnail: true
        }
      }
    })

    fs.unlinkSync(tempInput)
    fs.unlinkSync(tempOutput)

    await NXL.sendMessage(m.chat, { react: { text: "✅", key: m.key } })

  } catch (e) {
    console.error(e)
    await NXL.sendMessage(m.chat, { react: { text: "❌", key: m.key } })
  }
}
break

case "swgroup":
case "swgrup": {
  if (!isCreator) return m.reply(mess.owner)
  const quoted = m.quoted ? m.quoted : m;
  const mime = (quoted.msg || quoted).mimetype || "";
  const caption = m.body.replace(/^\.swgrup\s*/i, "").trim();
  const jid = m.chat;

  if (/image/.test(mime)) {
    const buffer = await quoted.download();
    await NXL.sendMessage(jid, { groupStatusMessage: { image: buffer, caption } });
    m.reply("\xe2\x9c\x85")
  } else if (/video/.test(mime)) {
    const buffer = await quoted.download();
    await NXL.sendMessage(jid, { groupStatusMessage: { video: buffer, caption } });
    m.reply("\xe2\x9c\x85")
  } else if (/audio/.test(mime)) {
    const buffer = await quoted.download();
    await NXL.sendMessage(jid, { groupStatusMessage: { audio: buffer } });
    m.reply("\xe2\x9c\x85")
  } else if (caption) {
    await NXL.sendMessage(jid, { groupStatusMessage: { text: caption } });
    m.reply("\xe2\x9c\x85")
  } else {
    await reply(`reply media atau tambahkan teks.\nexample: ${prefix + command} (reply image/video/audio) hai ini saya`);
  }
}
break

case 'ttsearch':
case 'tiktoksearch':
case 'caratt': {
  if (!text) return m.reply(`*Cari video TikTok*\nContoh: .ttsearch kucing lucu`)

  await m.reply(`🔍 Mencari TikTok: *${text}*...`)
  await NXL.sendMessage(m.chat, { react: { text: '⏳', key: m.key } })

  try {
    // [FIX ttsearch] sort_type=1 => "Banyak disukai" (Most Liked / paling viral),
    // bukan default relevansi. publish_time=0 => semua waktu. Ini menyamai tab
    // "Teratas/Banyak disukai" di app TikTok, lalu di-refine lokal by play_count.
    const res = await axios.get(
      `${global.apiTikwm}/api/feed/search?keywords=${encodeURIComponent(text)}&count=30&cursor=0&hd=1&sort_type=1&publish_time=0`,
      { headers: { 'User-Agent': 'Mozilla/5.0' }, timeout: 20000 }
    )

    const items = res.data?.data?.videos || []
    if (!items.length) {
      await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
      return m.reply('❌ Video tidak ditemukan untuk: ' + text)
    }


    const _kwLower = text.toLowerCase().trim()
    const _kwWords = _kwLower.split(/\s+/).filter(w => w.length > 0)
    const _filtered = items.filter(v => {
      const haystack = ((v.title || v.desc || '') + ' ' + (v.author?.unique_id || '')).toLowerCase()
      return _kwWords.some(w => haystack.includes(w))
    })

    const _pool = _filtered.length > 0 ? _filtered : items

    const results = _pool.sort((a, b) => (b.play_count || 0) - (a.play_count || 0)).slice(0, 5)

    await m.reply(`✅ Ditemukan ${items.length} video, mengunduh ${results.length} teratas...`)

    const sendOneTikTok = async (v, idx) => {
      const videoId = v.video_id || v.id
      const tiktokUrl = `${global.ttWeb}/@${v.author?.unique_id || 'user'}/video/${videoId}`

      const cap = `🎵 *TikTok Result ${idx + 1}/${results.length}*\n` +
        `👤 *Akun:* @${v.author?.unique_id || v.author?.nickname || '-'}\n` +
        `📝 *Deskripsi:* ${(v.title || v.desc || '').slice(0, 150)}\n` +
        `❤️ *Likes:* ${(v.digg_count || 0).toLocaleString()}\n` +
        `▶️ *Views:* ${(v.play_count || 0).toLocaleString()}\n\n` +
        `_Powered by ${ownername}_`

      const result = await tiktokDl(tiktokUrl)

      if (!result || !result.status) throw new Error('tiktokDl gagal: status false')

      if (result.durations == 0 && result.duration == "0 Seconds") {
        let araara = []
        let urutan = 0
        for (let a of result.data) {
          let imgsc = await prepareWAMessageMedia(
            { image: { url: `${a.url}` } },
            { upload: NXL.waUploadToServer }
          )
          araara.push({
            header: proto.Message.InteractiveMessage.Header.fromObject({
              title: `Foto Slide Ke *${urutan += 1}*`,
              hasMediaAttachment: true,
              ...imgsc
            }),
            nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.fromObject({
              buttons: [{
                name: "cta_url",
                buttonParamsJson: `{\"display_text\":\"Link Tautan Foto\",\"url\":\"${a.url}\",\"merchant_url\":\"${global.googleWeb}\"}`
              }]
            })
          })
        }
        const msgii = await generateWAMessageFromContent(m.chat, {
          viewOnceMessageV2Extension: {
            message: {
              messageContextInfo: { deviceListMetadata: {}, deviceListMetadataVersion: 2 },
              interactiveMessage: proto.Message.InteractiveMessage.fromObject({
                body: proto.Message.InteractiveMessage.Body.fromObject({ text: cap }),
                carouselMessage: proto.Message.InteractiveMessage.CarouselMessage.fromObject({ cards: araara })
              })
            }
          }
        }, { userJid: m.sender, quoted: m })
        await NXL.relayMessage(m.chat, msgii.message, { messageId: msgii.key.id })

      } else {
        let urlVid = result.data.find(e => e.type == "nowatermark_hd" || e.type == "nowatermark")
        if (!urlVid) throw new Error('URL nowatermark tidak ditemukan')
        await NXL.sendMessage(m.chat, {
          video: { url: urlVid.url },
          caption: cap,
          mimetype: 'video/mp4'
        }, { quoted: m })
      }
    }

    let gagal = 0
    let _consecutiveFail = 0
    for (let i = 0; i < results.length; i++) {
      // [PATCH E] F6-02: hentikan bila koneksi mati, atau jika sumber tampak down
      // (2 hasil beruntun gagal total) agar tidak menggiling operasi berat sia-sia.
      if (!global._connAlive) break
      if (_consecutiveFail >= 2) {
        await m.reply('⚠️ Sumber TikTok sepertinya sedang bermasalah, sisa video dilewati.')
        break
      }
      const v = results[i]
      await m.reply(`⏬ Mengunduh video ${i + 1}/${results.length}...`)

      let berhasilKirim = false
      for (let attempt = 1; attempt <= 2; attempt++) {
        try {
          await sendOneTikTok(v, i)
          berhasilKirim = true
          break
        } catch (e) {
          console.error(`[TTSEARCH video ${i + 1} attempt ${attempt} error]`, e.message)
          if (attempt < 2) {
            await new Promise(r => setTimeout(r, 3000))
          }
        }
      }

      if (!berhasilKirim) {
        gagal++
        _consecutiveFail++
        const cap = `🎵 *TikTok Result ${i + 1}/${results.length}*\n` +
          `👤 *Akun:* @${v.author?.unique_id || '-'}\n` +
          `📝 *Deskripsi:* ${(v.title || v.desc || '').slice(0, 100)}\n` +
          `❤️ *Likes:* ${(v.digg_count || 0).toLocaleString()}\n\n` +
          `_Powered by ${ownername}_`
        await m.reply(`⚠️ Video ${i + 1} gagal diunduh.\n\n${cap}`)
      } else {
        _consecutiveFail = 0
      }

      if (i < results.length - 1) await new Promise(r => setTimeout(r, 4000))
    }

    if (gagal === results.length) {
      await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
      await m.reply('❌ Semua video gagal. Coba lagi nanti.')
    } else {
      await NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key } })
    }

  } catch (e) {
    console.error("[TTSEARCH ERROR]", e.message)
    await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
    m.reply('❌ Gagal mencari TikTok: ' + e.message)
  }
}
break

case 'instagram':
case 'igdl':
case 'ig':
case 'igdownload':
case 'igvideo':
case 'igimage':
case 'igvid':
case 'igimg': {
  if (!text) return m.reply(`❌ Masukkan link Instagram!\nContoh: .igdl ${global.igWeb}/p/xxx`)
  if (!text.match(/instagram\.com\/(p|reel|tv|stories|share)/gi)) return m.reply('❌ URL Instagram tidak valid.\nContoh yang valid:\n- instagram.com/p/xxx\n- instagram.com/reel/xxx')

  await NXL.sendMessage(m.chat, { react: { text: '⏱️', key: m.key } })
  await m.reply('⏳ Sedang mengunduh dari Instagram...')

  try {
    const res = await igdl(text)

    if (res && res.status && res.result && res.result.downloadUrl && res.result.downloadUrl.length > 0) {
      const { metadata, downloadUrl } = res.result
      const cap = `🎬 *Instagram Downloader*\n👤 *User:* ${metadata?.username || '-'}\n📝 *Caption:* ${(metadata?.caption || '').slice(0, 200) || '-'}\n\n_Powered by ${ownername}_`

      for (const url of downloadUrl) {
        try {
          const isImage = url.includes('.jpg') || url.includes('.jpeg') || url.includes('.png') || url.includes('.webp') || url.includes('ftype=jpg')
          if (isImage) {
            await NXL.sendMessage(m.chat, { image: { url }, caption: cap }, { quoted: m })
          } else {
            await NXL.sendMessage(m.chat, { video: { url }, caption: cap }, { quoted: m })
          }
        } catch (e) {
          console.error("[IGDL media send error]", e.message)
        }
      }
      await NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key } })

    } else {
      await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
      m.reply('❌ Gagal mengunduh. Pastikan:\n1. Link valid dan postingan publik\n2. Bukan link expired\n3. Coba lagi sebentar lagi')
    }

  } catch (error) {
    console.error("[IGDL ERROR]", error.message)
    await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
    m.reply('❌ Terjadi kesalahan saat download Instagram.\n' + error.message)
  }
}
break;

case 'removebg':
case 'nobg': {
  const q = m.quoted ? m.quoted : m
  const mime = (q.msg || q).mimetype || ''
  
  if (!/image/.test(mime)) return m.reply(`[ PENGGUNAAN SALAH ]\n\nKirim atau balas gambar dengan perintah .${command}`)

  try {
    await m.reply('[ PROCESS ] Sedang memproses gambar Anda, mohon tunggu')

    const media = await q.download()
    if (!media) return m.reply('[-] Gagal mengunduh gambar, silakan coba lagi')

    const mode = (command === 'upscale') ? 'upscale' : 'removebg'
    const baseURL = "https://api.yunusek.org"

    const form = new FormData()
    
    const imageBlob = new Blob([media], { type: mime })
    form.append("image", imageBlob, "image.png")

    if (mode === 'removebg') {
      form.append("high_res", "false")
    } else if (mode === 'upscale') {
      form.append("scale", "2")
    }

    const endpoint = mode === 'removebg' ? '/api/remove-background' : '/api/upscale'

    const res = await axios.post(`${baseURL}${endpoint}`, form, {
      headers: {
        ...form.getHeaders?.() || {},
        "User-Agent": "okhttp/4.9.2",
        "Accept-Encoding": "gzip",
      },
      responseType: "arraybuffer",
      timeout: 30000
    })

    const resultBuffer = Buffer.from(res.data)
    if (!resultBuffer) return m.reply('[-] Gagal memproses gambar')

    await NXL.sendMessage(m.chat, { 
      image: resultBuffer, 
      caption: `*IMAGE PROCESSED*\n\n* MODE : ${mode.toUpperCase()}\n\n_System watermark: PT SONTOLOYO_` 
    }, { quoted: m })

  } catch (e) {
    console.error(e)
    m.reply(`[ ERROR SYSTEM ] ${e.message}`)
  }
}
break

case 'hdfoto':
case 'hdgambar':
case 'enhancefoto': {
  const hdMsg = m.quoted || (isMedia ? m : null)
  if (!hdMsg) return m.reply(`Kirim/reply gambar dengan caption *${prefix + command}*`)
  const hdMime = (hdMsg.msg || hdMsg).mimetype || hdMsg.mimetype || mime || ''
  if (!/image|webp/.test(hdMime)) return m.reply(`❌ Hanya gambar yang bisa di-HD-kan.`)

  await NXL.sendMessage(m.chat, { react: { text: '⏳', key: m.key } })
  const hdProses = await NXL.sendMessage(m.chat, { text: '⏳ Memproses HD Foto...' }, { quoted: m })

  try {
    const buf = await hdMsg.download()
    if (!buf || buf.length < 100) throw new Error('Gagal mengambil gambar.')

    let resultBuf = null


    try {
      const res1 = await remini(buf, 'enhance')
      if (res1) resultBuf = await getBuffer(res1)
    } catch {}


    if (!resultBuf || resultBuf.length < 100) {
      await NXL.sendMessage(m.chat, { text: '⏳ Server 1 gagal, mencoba server 2...', edit: hdProses.key })
      try {
        const FormData = require('form-data')
        const form2 = new FormData()
        form2.append('image', buf, { filename: 'image.jpg', contentType: 'image/jpeg' })
        form2.append('scale', '2')
        form2.append('noise', '1')
        const res4 = await axios.post(`${global.apiTraceMoe}/waifu2x`, form2, {
          headers: form2.getHeaders(), timeout: 30000, responseType: 'arraybuffer'
        })
        if (res4?.data) resultBuf = Buffer.from(res4.data)
      } catch {}
    }


    if (!resultBuf || resultBuf.length < 100) {
      await NXL.sendMessage(m.chat, { text: '⏳ Server 2 gagal, memproses lokal...', edit: hdProses.key })
      try {
        const jimp = (await import('jimp')).default
        const img = await jimp.read(buf)
        img.resize(img.getWidth() * 2, img.getHeight() * 2, jimp.RESIZE_BICUBIC)
        img.convolute([[0,-1,0],[-1,5,-1],[0,-1,0]])
        resultBuf = await img.getBufferAsync(jimp.MIME_JPEG)
      } catch {}
    }

    if (!resultBuf || resultBuf.length < 100) {
      await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
      return NXL.sendMessage(m.chat, { text: '❌ Semua server gagal. Coba lagi nanti.', edit: hdProses.key })
    }

    await NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key } })
    await NXL.sendMessage(m.chat, {
      image: resultBuf,
      caption: `✅ *HD Foto Berhasil*\n_Powered by ${ownername}_`
    }, { quoted: m })

  } catch (e) {
    console.error('[HDFOTO]', e.message)
    await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
    m.reply(`❌ Gagal proses HD foto: ${e.message}`)
  }
}
break

case 'hdvideo':
case 'enhancevideo': {
  const hvMsg = m.quoted || (isMedia ? m : null)
  if (!hvMsg) return m.reply(`Kirim/reply video dengan caption *${prefix + command}*`)
  const hvMime = (hvMsg.msg || hvMsg).mimetype || hvMsg.mimetype || mime || ''
  if (!/video/.test(hvMime)) return m.reply(`❌ Hanya video yang bisa di-HD-kan.`)

  await NXL.sendMessage(m.chat, { react: { text: '⏳', key: m.key } })
  await NXL.sendMessage(m.chat, { text: '⏳ Memproses HD Video...\n_Proses ini membutuhkan waktu lebih lama_' }, { quoted: m })

  try {
    const vidBuf = await hvMsg.download()
    if (!vidBuf || vidBuf.length < 100) throw new Error('Gagal mengambil video.')

    const tmpIn  = `/tmp/hd_in_${Date.now()}.mp4`
    const tmpOut = `/tmp/hd_out_${Date.now()}.mp4`
    fs.writeFileSync(tmpIn, vidBuf)

    await new Promise((resolve, reject) => {
      exec(
        `ffmpeg -y -i "${tmpIn}" -vf "scale=iw*2:ih*2:flags=lanczos,unsharp=5:5:1.5:5:5:0" -c:v libx264 -crf 18 -preset fast -c:a copy "${tmpOut}"`,
        { timeout: 120000 },
        (err) => { if (err) reject(err); else resolve() }
      )
    })

    const resultVid = fs.readFileSync(tmpOut)
    try { fs.unlinkSync(tmpIn); fs.unlinkSync(tmpOut) } catch {}

    await NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key } })
    await NXL.sendMessage(m.chat, {
      video: resultVid,
      caption: `✅ *HD Video Berhasil*\n_Powered by ${ownername}_`,
      mimetype: 'video/mp4'
    }, { quoted: m })

  } catch (e) {
    console.error('[HDVIDEO]', e.message)
    await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
    m.reply(`❌ Gagal proses HD video.\n_Pastikan ffmpeg terinstall di server._\n\nError: ${e.message}`)
  }
}
break

case 'upscale':
case 'perbesar': {
  const upMsg = m.quoted || (isMedia ? m : null)
  if (!upMsg) return m.reply(`Kirim/reply gambar dengan caption *${prefix + command}*\n\n*Contoh:*\n${prefix + command} 2x _(2x atau 4x, default 2x)_`)
  const upMime = (upMsg.msg || upMsg).mimetype || upMsg.mimetype || mime || ''
  if (!/image|webp/.test(upMime)) return m.reply(`❌ Hanya gambar yang bisa di-upscale.`)

  const scale = Math.min(Math.max(parseInt((args[0] || '2').replace('x', '')) || 2, 2), 4)

  await NXL.sendMessage(m.chat, { react: { text: '⏳', key: m.key } })
  await NXL.sendMessage(m.chat, { text: `⏳ Upscale ${scale}x sedang diproses...` }, { quoted: m })

  try {
    const upBuf = await upMsg.download()
    if (!upBuf || upBuf.length < 100) throw new Error('Gagal mengambil gambar.')

    const jimp = (await import('jimp')).default
    const img = await jimp.read(upBuf)
    const newW = img.getWidth() * scale
    const newH = img.getHeight() * scale

    if (newW > 8000 || newH > 8000) return m.reply(`❌ Resolusi hasil terlalu besar (${newW}x${newH}).\nCoba scale lebih kecil.`)

    img.resize(newW, newH, jimp.RESIZE_BICUBIC)
    const resultBuf = await img.getBufferAsync(jimp.MIME_JPEG)

    await NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key } })
    await NXL.sendMessage(m.chat, {
      image: resultBuf,
      caption: `✅ *Upscale ${scale}x Berhasil*\n📐 Resolusi: ${newW}x${newH}\n_Powered by ${ownername}_`
    }, { quoted: m })

  } catch (e) {
    console.error('[UPSCALE]', e.message)
    await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
    m.reply(`❌ Gagal upscale: ${e.message}`)
  }
}
break

case 'blur': {
  const blurMsg = m.quoted || (isMedia ? m : null)
  if (!blurMsg) return m.reply(`Kirim/reply gambar dengan caption *${prefix + command}*\n\n*Contoh:*\n${prefix + command} 10 _(level 1-100, default 15)_`)
  const blurMime = (blurMsg.msg || blurMsg).mimetype || blurMsg.mimetype || mime || ''
  if (!/image|webp/.test(blurMime)) return m.reply(`❌ Hanya gambar yang bisa di-blur.`)

  const blurLevel = Math.min(Math.max(parseInt(args[0]) || 15, 1), 100)

  await NXL.sendMessage(m.chat, { react: { text: '⏳', key: m.key } })

  try {
    const blurBuf = await blurMsg.download()
    if (!blurBuf || blurBuf.length < 100) throw new Error('Gagal mengambil gambar.')

    const jimp = (await import('jimp')).default
    const img = await jimp.read(blurBuf)
    img.blur(blurLevel)
    const resultBuf = await img.getBufferAsync(jimp.MIME_JPEG)

    await NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key } })
    await NXL.sendMessage(m.chat, {
      image: resultBuf,
      caption: `✅ *Blur Level ${blurLevel} Berhasil*\n_Powered by ${ownername}_`
    }, { quoted: m })

  } catch (e) {
    console.error('[BLUR]', e.message)
    await NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key } })
    m.reply(`❌ Gagal blur gambar: ${e.message}`)
  }
}
break

case "suarateks":
case "stt": {
  const contextInfo = m.message?.extendedTextMessage?.contextInfo
  const quotedMsg = contextInfo?.quotedMessage
  const audioMsg = quotedMsg?.audioMessage || quotedMsg?.pttMessage

  if (!audioMsg) return m.reply("*Reply voice note/audio dulu!*")

  try {
    await m.reply("『 🎙️ 』 Mengkonversi suara ke teks...")

    const stream = await downloadContentFromMessage(audioMsg, "audio")
    const chunks = []
    for await (const chunk of stream) chunks.push(chunk)
    const audioBuffer = Buffer.concat(chunks)

    const form = new FormData()
    form.append("file", new Blob([audioBuffer], { type: "audio/ogg" }), "audio.ogg")
    form.append("model", "whisper-large-v3-turbo")
    form.append("response_format", "json")
    form.append("language", "id")

    const res = await fetch(`${global.apiGroq}/openai/v1/audio/transcriptions`, {
      method: "POST",
      headers: {
        "Authorization": `Bearer ${global.groqKey}`
      },
      body: form,
      signal: AbortSignal.timeout(30000)
    })

    const rawText = await res.text()
    let json
    try {
      json = JSON.parse(rawText)
    } catch {
      return m.reply(`❌ Server error:\n${rawText.slice(0, 300)}`)
    }

    if (json?.error) return m.reply(`❌ Groq Error: ${json.error?.message || json.error}`)

    const teks = json?.text
    if (!teks) return m.reply(`❌ Gagal konversi.\n${JSON.stringify(json)}`)

    await NXL.sendMessage(m.chat, {
      text: `『 🎙️ *Speech to Text* 』\n◆━━━━━━━━━━━━━━━━◆\n\n${teks}\n\n◆━━━━━━━━━━━━━━━━◆\n🌸 *${storename}* 🌸`
    }, { quoted: m })

  } catch (e) {
    m.reply(`❌ Error: ${e.message}`)
  }
}
break
case "debugquoted": {
  const raw = JSON.stringify(m.message, null, 2).slice(0, 1000)
  return m.reply(`raw: ${raw}`)
}
break
case 'statusbot':
case 'botstatus': {
    const { performance } = require('perf_hooks')
    const os = require('os')
    const fs = require('fs')
    const moment = require('moment-timezone')
    const QuickChart = require('quickchart-js')

    let timestamp = m.messageTimestamp ? (typeof m.messageTimestamp === 'number' ? m.messageTimestamp : m.messageTimestamp.low) : (Date.now() / 1000);
    let now = Date.now();
    let latensi = now - (timestamp * 1000);

    const startProcess = performance.now()

    let osName = 'Unknown OS'
    try {
        if (process.platform === 'linux' && fs.existsSync('/etc/os-release')) {
            const osInfo = fs.readFileSync('/etc/os-release', 'utf8')
            const nameMatch = osInfo.match(/^NAME="?(.+?)"?$/m)
            const verMatch = osInfo.match(/^VERSION="?(.+?)"?$/m)
            const name = nameMatch ? nameMatch[1].replace(/"/g, '') : ''
            const version = verMatch ? verMatch[1].replace(/"/g, '') : ''
            osName = `${name} ${version}`.trim()
        } else if (process.platform === 'win32') osName = 'Windows'
        else if (process.platform === 'darwin') osName = 'macOS'
        else osName = os.type()
    } catch {
        osName = os.type()
    }

    const rtimage = (seconds) => {
        const d = Math.floor(seconds / (3600 * 24))
        const h = Math.floor((seconds % (3600 * 24)) / 3600)
        const m = Math.floor((seconds % 3600) / 60)
        const s = Math.floor(seconds % 60)
        return `*${d}* Hari *${h}* Jam *${m}* Menit`
    }

    const runtimeFormat = (seconds) => {
        const d = Math.floor(seconds / (3600 * 24))
        const h = Math.floor((seconds % (3600 * 24)) / 3600)
        const m = Math.floor((seconds % 3600) / 60)
        const s = Math.floor(seconds % 60)
        return `*${d}* ☀️ Hari\n│ *${h}* 🕐 Jam\n│ *${m}* ⏰ Menit\n│ *${s}* ⏱️ Detik`
    }

    const formatp = (bytes) => `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`

    const getCpuUsage = async (delay = 100) => {
        const start = os.cpus()
        await new Promise(r => setTimeout(r, delay))
        const end = os.cpus()
        let idleDiff = 0, totalDiff = 0

        for (let i = 0; i < start.length; i++) {
            const s = start[i].times
            const e = end[i].times
            const idle = e.idle - s.idle
            const total = Object.keys(s).reduce((a, t) => a + (e[t] - s[t]), 0)
            idleDiff += idle
            totalDiff += total
        }
        return 100 - Math.round((idleDiff / totalDiff) * 100)
    }

    const cpuUsagePercent = await getCpuUsage()

    const cpus = os.cpus()
    const avgSpeed = cpus.reduce((a, c) => a + c.speed, 0) / cpus.length
    const cpuModel = cpus[0]?.model?.trim() || 'Unknown CPU'
    const cpuCore = cpus.length

    const mem = os.totalmem()
    const free = os.freemem()

    let swapTotal = 0, swapFree = 0
    try {
        if (fs.existsSync('/proc/meminfo')) {
            const info = fs.readFileSync('/proc/meminfo', 'utf8')
            const swapTotalMatch = info.match(/^SwapTotal:\s+(\d+)/m)
            const swapFreeMatch = info.match(/^SwapFree:\s+(\d+)/m)
            swapTotal = swapTotalMatch ? parseInt(swapTotalMatch[1]) * 1024 : 0
            swapFree = swapFreeMatch ? parseInt(swapFreeMatch[1]) * 1024 : 0
        }
    } catch {}

    const totalMemAll = mem + swapTotal
    const usedMemAll = (mem - free) + (swapTotal - swapFree)
    const percentUsed = totalMemAll > 0 ? (usedMemAll / totalMemAll) * 100 : 0

    const runtimeText = runtimeFormat(process.uptime())
    const waktu = moment().tz("Asia/Jakarta").format('HH:mm:ss')
    const tanggal = moment().tz("Asia/Jakarta").locale("id").format('dddd, D MMMM YYYY')

    const endProcess = performance.now()
    const responInSeconds = ((endProcess - startProcess) / 1000).toFixed(4)

    const val = parseFloat(responInSeconds);
    let p = 0;

    if (val >= 1.0000) p = 100;
    else if (val <= 0.0001) p = 0;
    else if (val <= 0.0010) p = 0 + ((val - 0.0001) / (0.0010 - 0.0001)) * 20;
    else if (val <= 0.0100) p = 20 + ((val - 0.0010) / (0.0100 - 0.0010)) * 20;
    else if (val <= 0.1000) p = 40 + ((val - 0.0100) / (0.1000 - 0.0100)) * 20;
    else if (val <= 0.6000) p = 60 + ((val - 0.1000) / (0.6000 - 0.1000)) * 20;
    else p = 80 + ((val - 0.6000) / (1.0000 - 0.6000)) * 20;

    const chart = new QuickChart();
    chart.setVersion('3');
    chart.setWidth(500);
    chart.setHeight(300);
    chart.setConfig({
        type: 'bar',
        data: {
            labels: [''],
            datasets: [
                { label: 'Safe', data: [20], backgroundColor: '#32CD32', barPercentage: 1, categoryPercentage: 1 },
                { label: 'Low Risk', data: [20], backgroundColor: '#ADFF2F', barPercentage: 1, categoryPercentage: 1 },
                { label: 'Warning', data: [20], backgroundColor: '#FFFF00', barPercentage: 1, categoryPercentage: 1 },
                { label: 'High Risk', data: [20], backgroundColor: '#FFA500', barPercentage: 1, categoryPercentage: 1 },
                { label: 'Critical', data: [20], backgroundColor: '#FF0000', barPercentage: 1, categoryPercentage: 1 },
            ],
        },
        options: {
            indexAxis: 'y',
            layout: { padding: { top: 60, bottom: 20, left: 20, right: 20 } },
            scales: {
                x: {
                    stacked: true, min: 0, max: 100,
                    ticks: {
                        display: true, color: '#999', maxRotation: 45, minRotation: 45,
                        font: { size: 10 },
                        callback: (val) => {
                            const l = {0:'0.0001', 10:'0.0003', 20:'0.0010', 30:'0.0030', 40:'0.0100', 50:'0.0300', 60:'0.1000', 70:'0.3000', 80:'0.6000', 90:'0.8000', 100:'1.0000'};
                            return l[val] || '';
                        }
                    },
                    grid: { display: false }
                },
                y: { display: false, stacked: true }
            },
            plugins: {
                legend: { display: false },
                tooltip: { enabled: false },
                annotation: {
                    clip: false,
                    annotations: {
                        text: {
                            type: 'label',
                            xValue: p, yValue: 0, yAdjust: -125,
                            content: [`Respond: ${responInSeconds}s`],
                            color: 'black', font: { size: 14, weight: 'bold' },
                            position: 'center', backgroundColor: 'transparent'
                        },
                        panah: {
                            type: 'point', xValue: p, yValue: 0, yAdjust: -100,
                            pointStyle: 'triangle', rotation: 180, radius: 8,
                            backgroundColor: 'black', borderColor: 'black'
                        },
                        garis: {
                            type: 'line', xMin: p, xMax: p, yMin: -0.5, yMax: 0.5,
                            borderColor: 'black', borderWidth: 2, borderDash: [6, 4]
                        }
                    }
                }
            }
        }
    });

    let pingIcon
    if (latensi < 100) pingIcon = "🟢"
    else if (latensi < 300) pingIcon = "🔵"
    else if (latensi < 600) pingIcon = "🟡"
    else if (latensi < 1000) pingIcon = "🟠"
    else pingIcon = "🔴"

    const response = `
╭───⏱️ *[ STATUS BOT ]* ⏱️
│
├ 💠 *Ping:* ${pingIcon} ${latensi.toFixed(0)} ms
├ 💠 *Respon:* ${responInSeconds} detik
│
├ 📈 *Uptime:*
│  ${runtimeText}
│
├ 🖥️ *Server Info:*
│  🔵 Platform : ${os.platform()}
│  💻 OS        : ${osName}
│  🧿 Hostname : ${os.hostname()}
│  🌎 Zona      : ${Intl.DateTimeFormat().resolvedOptions().timeZone}
│  🧠 CPU       : ${cpuModel}
│  🔩 Core      : ${cpuCore} Core
│  ⚡ Speed     : ${avgSpeed.toFixed(2)} MHz
│
├ 📊 *RAM Usage:*
│  ${formatp(usedMemAll)} / ${formatp(totalMemAll)} (${percentUsed.toFixed(1)}%)
│
├ ⚡ *CPU Usage:*
│  ${cpuUsagePercent.toFixed(1)}% dari ${cpuCore} Core
│
├ 🗓️ *Tanggal:* ${tanggal}
├ 🕒 *Waktu:* ${waktu} WIB
╰─────────────────────
`.trim()

    NXL.sendMessage(m.chat, {
      text: response,
      contextInfo: {
        externalAdReply: {
          title: "🏓 Status bot online >.<",
          body: namabot,
          thumbnailUrl: chart.getUrl(),
          sourceUrl: `${global.storeNXLhost}`,
          mediaType: 1,
          renderLargerThumbnail: true
        }
      }
    }, { quoted: m })
}
break

case "rvo": case "readviewonce": {
 if (!isCreator) return reply(mess.owner);
if (!m.quoted) return reply("reply pesan viewOnce nya!")
let msg = m?.quoted?.message?.imageMessage || m?.quoted?.message?.videoMessage || m?.quoted?.message?.audioMessage || m?.quoted
if (!msg.viewOnce && m.quoted.mtype !== "viewOnceMessageV2" && !msg.viewOnce) return reply("Pesan itu bukan viewonce!")
let media = await downloadContentFromMessage(msg, msg.mimetype == 'image/jpeg' ? 'image' : msg.mimetype == 'video/mp4' ? 'video' : 'audio')
    let type = msg.mimetype
    let buffer = Buffer.from([])
    for await (const chunk of media) {
        buffer = Buffer.concat([buffer, chunk])
    }
    if (/video/.test(type)) {
        return NXL.sendMessage(m.chat, {video: buffer, caption: msg.caption || ""}, {quoted: m})
    } else if (/image/.test(type)) {
        return NXL.sendMessage(m.chat, {image: buffer, caption: msg.caption || ""}, {quoted: m})
    } else if (/audio/.test(type)) {
        return NXL.sendMessage(m.chat, {audio: buffer, mimetype: "audio/mpeg", ptt: true}, {quoted: m})
    }
}
break

case 'sfile': {
  if (!text) return m.reply(`Contoh: ${prefix + command} ${global.apiSfile}/xxxxxx`)

  if (!text.includes('sfile.co') && !text.includes('sfile.mobi')) {
    return m.reply('Link harus dari sfile.co atau sfile.mobi!')
  }

  m.reply(mess.wait)

  try {
    const pageRes = await fetch(text, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
        'Referer': text
      }
    })
    const html = await pageRes.text()
    const $ = cheerio.load(html)

    const title = $('title').text().trim()
    const desc = $('meta[name="description"]').attr('content') || ''
    const size = desc.match(/size\s+([\d.]+ \w+)/i)?.[1] || '-'
    const uploader = $('a[href*="/user/"]').first().text().trim() || '-'
    const uploaded = desc.match(/on\s+(\d+ \w+ \d+)/i)?.[1] || '-'
    const downloads = $('p, span, div').filter((_, el) => $(el).text().includes('Downloads')).first().text().match(/[\d,]+/)?.[0] || '-'
    const mimetype = $('p').filter((_, el) => $(el).text().includes('/')).first().text().trim() || '-'
    const fileId = text.split('/').pop().split('?')[0]
    const category = $('a[href*="/category/"]').first().text().trim() || '-'

    m.reply(
      `*Sfile Info*\n\n` +
      `Nama: ${title}\n` +
      `Ukuran: ${size}\n` +
      `Tipe: ${mimetype}\n` +
      `Kategori: ${category}\n` +
      `Upload oleh: ${uploader}\n` +
      `Tanggal: ${uploaded}\n` +
      `Downloads: ${downloads}\n` +
      `File ID: ${fileId}\n\n` +
      `Link: ${text}`
    )

  } catch (err) {
    m.reply(`Gagal: ${err.message}`)
  }
}
break

case 'swgc': case 'upswgc': {
  if (!isCreator) return reply(mess.owner)
  const { fromBuffer } = require("file-type");
  const fs = require("fs");

  let content = {}
  let buffer, ext, tempFile

  if (m.quoted) {
    try {
      buffer = await m.quoted.download()
      if (!buffer) return m.reply("❌ Gagal mengambil media quoted.")
      ext = (await fromBuffer(buffer))?.ext || 'bin'
      tempFile = path.join(__dirname, `tmp_${Date.now()}.${ext}`)
      fs.writeFileSync(tempFile, buffer)

      const quotedType = m.quoted.mtype || Object.keys(m.quoted.message || {})[0] || ''
      if (/image|video|audio/.test(quotedType)) {
        if (/image/.test(quotedType)) {
          content.image = { url: tempFile }
          if (text) content.caption = text
        } else if (/video/.test(quotedType)) {
          content.video = { url: tempFile }
          if (text) content.caption = text
        } else if (/audio/.test(quotedType)) {
          if (text) {
            fs.unlinkSync(tempFile)
            return m.reply("Audio tidak boleh disertai caption.")
          }
          content.audio = { url: tempFile }
          content.ptt = false
        }
      } else {
        fs.unlinkSync(tempFile)
        return m.reply("Reply harus berupa image/video/audio.")
      }
    } catch (e) {
      return m.reply("❌ Media tidak valid atau gagal diproses.")
    }
  } else if (text) {
    content.text = text
  } else {
    return m.reply("Kirim media (foto/video/audio) atau teks, bisa dengan reply atau langsung.")
  }

  if (content.text && !content.text.trim()) return m.reply("Teks tidak boleh kosong.")

  let allGroups = await NXL.groupFetchAllParticipating()
  let validGroups = []

  for (let [gid, metadata] of Object.entries(allGroups)) {
    validGroups.push({
      title: metadata.subject || gid,
      description: gid,
      id: `.sendstatus ${gid} ${encodeURIComponent(JSON.stringify(content))}`
    })
  }

  if (!validGroups.length) {
    if (tempFile && fs.existsSync(tempFile)) fs.unlinkSync(tempFile)
    return m.reply("Tidak ada grup valid yang bisa dipilih.")
  }

  const encodedAll = encodeURIComponent(JSON.stringify(content))
  const allGids = Object.keys(allGroups).join(',')

  const chunkSize = 10
  const sections = []
  for (let i = 0; i < validGroups.length; i += chunkSize) {
    sections.push({
      title: `Grup ${i + 1}–${Math.min(i + chunkSize, validGroups.length)}`,
      rows: validGroups.slice(i, i + chunkSize)
    })
  }

  let msg = generateWAMessageFromContent(m.chat, {
    viewOnceMessage: {
      message: {
        messageContextInfo: { deviceListMetadata: {}, deviceListMetadataVersion: 2 },
        interactiveMessage: {
          body: { text: `\`\`\`Pilih Grup Tujuan ♨️\`\`\`\n\nTotal grup: *${validGroups.length}*` },
          nativeFlowMessage: {
            buttons: [
              {
                name: "single_select",
                buttonParamsJson: JSON.stringify({
                  title: "PILIH GRUP",
                  sections: sections
                })
              },
              {
                name: "quick_reply",
                buttonParamsJson: JSON.stringify({
                  display_text: `📢 Kirim ke Semua Grup (${validGroups.length})`,
                  id: `.sendstatus ALL ${encodedAll}`
                })
              }
            ]
          }
        }
      }
    }
  }, { quoted: m }, {})

  await NXL.relayMessage(msg.key.remoteJid, msg.message, {
    messageId: msg.key.id
  })
}
break

case 'upsw':
case 'upstatus':
case 'upstatuswa':
case 'uploadsw': {
  if (!isCreator) return m.reply(mess.owner)

  // [UPSW] Logika audience + upload status dipindah ke helper bersama
  // (lib/upswStatus.js) agar dapat di-reuse oleh .done1–.done10 TANPA
  // duplikasi kode. Perilaku upload tetap identik (socket hidup Patch A,
  // statusJidList dari store.contacts PN-only + nomor bot).
  const { uploadStatusWA } = require('./lib/upswStatus')

  const captionUpsw = text ? text.trim() : ''
  const isImgUpsw = /image/.test(mime)
  const isVidUpsw = /video/.test(mime)
  const isAudUpsw = /audio/.test(mime)

  if (!isImgUpsw && !isVidUpsw && !isAudUpsw && !captionUpsw) {
    return m.reply(
      `*Cara pakai .${command}:*\n` +
      `• Reply/kirim *gambar/video/audio* + \`.${command} [caption]\`\n` +
      `• Atau teks biasa: \`.${command} teks status\``
    )
  }

  await m.reply('⏳ Mengunggah ke status WhatsApp...')

  try {
    // [UPSW AUDIENCE] Audience (statusJidList) & pemilihan socket hidup kini
    // ditangani helper uploadStatusWA (lib/upswStatus.js) — sumber kebenaran
    // tunggal, dipakai juga oleh .done1–.done10. Perilaku tetap sama:
    // store.contacts PN-only (buang @lid) + nomor bot, fallback bila kosong.
    if (isImgUpsw) {
      const buffer = await quoted.download()
      if (!buffer || !buffer.length) return m.reply('❌ Gagal mengunduh media.')
      await uploadStatusWA(NXL, { image: buffer, caption: captionUpsw })
    } else if (isVidUpsw) {
      const buffer = await quoted.download()
      if (!buffer || !buffer.length) return m.reply('❌ Gagal mengunduh media.')
      await uploadStatusWA(NXL, { video: buffer, caption: captionUpsw })
    } else if (isAudUpsw) {
      const buffer = await quoted.download()
      if (!buffer || !buffer.length) return m.reply('❌ Gagal mengunduh media.')
      const isPttUpsw = !!(qmsg && qmsg.ptt) || /ptt/i.test(mime)
      await uploadStatusWA(NXL, { audio: buffer, mimetype: 'audio/mp4', ptt: isPttUpsw })
    } else {
      await uploadStatusWA(NXL, { text: captionUpsw })
    }

    await m.reply('✅ Berhasil diunggah ke status WhatsApp!')
  } catch (e) {
    console.error('[UPSW ERROR]', e?.message || e)
    await m.reply('❌ Gagal mengunggah status: ' + (e?.message || e))
  }
}
break

case 'sendstatus': {
  if (!isCreator) return reply(mess.owner)
  const fs = require("fs")
  const [groupId, ...contentARR] = args
  if (!groupId || !contentARR.length) return m.reply("❌ Format tidak valid.")

  let contentDecoded
  try {
    contentDecoded = JSON.parse(decodeURIComponent(contentARR.join(' ')))
  } catch (e) {
    return m.reply("❌ Gagal decode konten.")
  }

  if (groupId === 'ALL') {
    let allGroups = await NXL.groupFetchAllParticipating()
    let groupIds = Object.keys(allGroups)
    await m.reply(`📢 Mengirim ke *${groupIds.length}* grup...`)
    // [PATCH B] Broadcast lewat engine terpadu — kini dapat dihentikan (.stopswgc)
    // dan otomatis berhenti saat disconnect (fix T2-16 unstoppable loop).
    const _res = await runBroadcast({
      lockFlag: 'statusswgc',
      stopFlag: 'stopswgc',
      targets: groupIds,
      delayMs: 1500,
      sendOne: async (conn, gid) => { await groupStatus(gid, { ...contentDecoded }) },
      cleanup: () => {
        for (let key of ['image', 'video', 'audio']) {
          let filePath = contentDecoded?.[key]?.url
          if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath)
        }
      }
    })
    if (_res.rejected) return m.reply(`⚠️ Broadcast status sedang berjalan, tunggu selesai atau hentikan dengan .stopswgc`)
    return m.reply(`✅ Selesai!\n📤 Sukses: *${_res.sukses}* | ❌ Gagal: *${_res.gagal}*`)
  }

  let sent = await groupStatus(groupId, contentDecoded)

  for (let key of ['image', 'video', 'audio']) {
    let filePath = contentDecoded?.[key]?.url
    if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath)
  }

  m.reply(sent
    ? `✅ Berhasil dikirim ke grup: ${groupId}`
    : `❌ Gagal kirim ke grup: ${groupId}`
  )
}
break
case 'setautoswgc': {
  if (!isCreator) return reply(mess.owner)
  const fs = require("fs")
  const { fromBuffer } = require("file-type")
  const path = require("path")

  if (args[0] && !isNaN(parseInt(args[0])) && !m.quoted && !text.replace(args[0], '').trim()) {
    const jeda = parseInt(args[0])
    if (jeda < 1000) return m.reply("Minimal jeda 1000ms (1 detik).")
    global.JedaSwgc = jeda
    return m.reply(`✅ Jeda autoswgc diset ke *${jeda}ms*`)
  }

  let content = {}
  let buffer, ext, tempFile

  if (m.quoted) {
    try {
      buffer = await m.quoted.download()
      if (!buffer) return m.reply("❌ Gagal mengambil media quoted.")
      ext = (await fromBuffer(buffer))?.ext || 'bin'
      tempFile = path.join(__dirname, `tmp_swgc_set_${Date.now()}.${ext}`)
      fs.writeFileSync(tempFile, buffer)

      const quotedType = m.quoted.mtype || Object.keys(m.quoted.message || {})[0] || ''
      if (/image/.test(quotedType)) {
        content.image = { url: tempFile }
        if (text) content.caption = text
      } else if (/video/.test(quotedType)) {
        content.video = { url: tempFile }
        if (text) content.caption = text
      } else if (/audio/.test(quotedType)) {
        if (text) {
          fs.unlinkSync(tempFile)
          return m.reply("Audio tidak boleh disertai caption.")
        }
        content.audio = { url: tempFile }
        content.ptt = false
      } else {
        fs.unlinkSync(tempFile)
        return m.reply("Reply harus berupa image/video/audio.")
      }
    } catch (e) {
      return m.reply("❌ Media tidak valid atau gagal diproses.")
    }
  } else if (text) {
    content.text = text
  } else {
    return m.reply([
      `*Penggunaan .setautoswgc:*`,
      `• Set jeda: .setautoswgc 5000`,
      `• Set konten teks: .setautoswgc teks pesan`,
      `• Set konten media: reply foto/video lalu .setautoswgc`,
      `• Set media + caption: reply foto lalu .setautoswgc caption`,
      `\n📦 Konten tersimpan: ${global.autoSwgcContent ? '✅ Ada' : '❌ Belum diset'}`,
      `⏱ Jeda: ${global.JedaSwgc}ms`
    ].join('\n'))
  }

  global.autoSwgcContent = content
  m.reply(`✅ Konten autoswgc berhasil disimpan!\n⏱ Jeda aktif: *${global.JedaSwgc}ms*\n\nJalankan dengan *.autojpmswgc*`)
}
break

case 'autojpmswgc': {
  if (!isCreator) return reply(mess.owner)
  const fs = require("fs")

  if (!global.autoSwgcContent) {
    return m.reply("❌ Konten belum diset!\nGunakan *.setautoswgc* dulu untuk menyimpan konten.")
  }

  const content = global.autoSwgcContent
  const jeda = global.JedaSwgc || 5000

  let allGroups
  try {
    allGroups = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  const blacklist = loadBlacklistSwgc()
  const groupIds = Object.keys(allGroups).filter(gid => !blacklist.includes(gid))

  if (!groupIds.length) return m.reply("❌ Tidak ada grup valid.")

  await m.reply(`🚀 Mulai autojpmswgc ke *${groupIds.length}* grup\n⏱ Jeda: *${jeda}ms* per grup\n\nKirim *.stopswgc* untuk menghentikan.`)

  // [PATCH B] Broadcast lewat engine terpadu (run-lock statusswgc + stop + finally cleanup)
  const _res = await runBroadcast({
    lockFlag: 'statusswgc',
    stopFlag: 'stopswgc',
    targets: groupIds,
    delayMs: () => global.JedaSwgc || 5000,
    sendOne: async (conn, gid) => { await groupStatus(gid, { ...content }) },
    cleanup: () => {
      for (const key of ['image', 'video', 'audio']) {
        const filePath = content?.[key]?.url
        if (filePath && fs.existsSync(filePath)) fs.unlinkSync(filePath)
      }
    }
  })

  if (_res.rejected) return m.reply(`⚠️ Broadcast SWGC sedang berjalan, tunggu selesai atau hentikan dengan .stopswgc`)

  m.reply(`${_res.stopped ? '⛔ Dihentikan!' : '✅ Selesai!'}\n📤 Sukses: *${_res.sukses}*\n❌ Gagal: *${_res.gagal}*\n📊 Total: *${_res.index}/${groupIds.length}*`
  )
}
break

case 'stopswgc': {
  if (!isCreator) return reply(mess.owner)
  global.stopswgc = true
  m.reply("⛔ Menghentikan autojpmswgc...")
}
break

case 'jedapush': {
  if (!isCreator) return reply(mess.owner)
  if (!text || isNaN(parseInt(text))) return m.reply(`*Contoh :* .jedapush 5000\n\nJeda saat ini: *${global.JedaPushkontak}ms*`)
  const jeda = parseInt(text)
  if (jeda < 1000) return m.reply("Minimal jeda 1000ms (1 detik).")
  global.JedaPushkontak = jeda
  m.reply(`✅ Jeda push kontak diset ke *${jeda}ms*`)
}
break

case 'jedajpm': {
  if (!isCreator) return reply(mess.owner)
  if (!text || isNaN(parseInt(text))) return m.reply(`*Contoh :* .jedajpm 4000\n\nJeda saat ini: *${global.JedaJpm}ms*`)
  const jeda = parseInt(text)
  if (jeda < 1000) return m.reply("Minimal jeda 1000ms (1 detik).")
  global.JedaJpm = jeda
  m.reply(`✅ Jeda JPM diset ke *${jeda}ms*`)
}
break

case 'listgrup':
case 'listgroup': {
  if (!isCreator) return reply(mess.owner)
  try {
    const allGroups = await NXL.groupFetchAllParticipating()
    const entries = Object.entries(allGroups)
    if (!entries.length) return m.reply("Bot tidak terdaftar di grup manapun.")
    let teks = `*Daftar Grup Bot*\n${"─".repeat(25)}\n`
    entries.forEach(([id, meta], i) => {
      teks += `${i + 1}. *${meta.subject || id}*\n   └ ${id}\n   └ Member: ${meta.participants?.length || 0}\n`
    })
    teks += `\nTotal: *${entries.length}* grup`
    m.reply(teks)
  } catch (e) {
    m.reply("Gagal mengambil daftar grup: " + e.message)
  }
}
break

case 'setjpm': {
  if (!isCreator) return reply(mess.owner)

  if (!text && !m.quoted) return m.reply(
    `*Penggunaan .setjpm:*\n` +
    `• Set teks: .setjpm teks pesan\n` +
    `• Set foto+caption: reply foto lalu .setjpm caption\n` +
    `• Set foto saja: reply foto lalu .setjpm\n\n` +
    `📦 Konten tersimpan: ${global.jpmContent ? '✅ Ada' : '❌ Belum diset'}\n` +
    `⏱ Jeda: ${global.JedaJpm}ms`
  )

  let content = {}
  if (m.quoted && /image/.test(mime)) {
    try {
      const mediaPath = await NXL.downloadAndSaveMediaMessage(qmsg)
      content.image = fs.readFileSync(mediaPath)
      if (text) content.caption = text
      fs.unlinkSync(mediaPath)
    } catch (e) {
      return m.reply("❌ Gagal mengambil media: " + e.message)
    }
  } else if (text) {
    content.text = text
  } else {
    return m.reply("Kirim teks atau reply foto untuk menyimpan konten JPM.")
  }

  global.jpmContent = content
  m.reply(`✅ Konten JPM berhasil disimpan!\n⏱ Jeda aktif: *${global.JedaJpm}ms*\n\nJalankan dengan *.autojpm*`)
}
break

case 'autojpm': {
  if (!isCreator) return reply(mess.owner)

  if (!global.jpmContent) {
    return m.reply("❌ Konten belum diset!\nGunakan *.setjpm* dulu untuk menyimpan konten.")
  }

  const jpmC = global.jpmContent
  const jpmJeda = global.JedaJpm || 4000

  let allGroupsJpm
  try {
    allGroupsJpm = await global.getGroupsCached()
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  let blacklistJpm = []
  try { blacklistJpm = loadBlacklistJpm() } catch { blacklistJpm = [] }
  const blacklistJpmIds = blacklistJpm.map(v => v.id || v)
  const groupIdsJpm = Object.keys(allGroupsJpm).filter(id => !blacklistJpmIds.includes(id))

  if (!groupIdsJpm.length) return m.reply("❌ Tidak ada grup valid.")

  const skippedJpm = Object.keys(allGroupsJpm).length - groupIdsJpm.length
  await m.reply(`🚀 Mulai autojpm ke *${groupIdsJpm.length}* grup\n⏱ Jeda: *${jpmJeda}ms*${skippedJpm > 0 ? `\n⛔ Skip blacklist: *${skippedJpm}* grup` : ''}\n\nKirim *.stopjpm* untuk menghentikan.`)

  // [PATCH B] Broadcast lewat engine terpadu (kini ber-lock statusjpm, fix T2-13 concurrent)
  const _res = await runBroadcast({
    lockFlag: 'statusjpm',
    stopFlag: 'stopjpm',
    targets: groupIdsJpm,
    delayMs: () => global.JedaJpm || 4000,
    sendOne: async (conn, gid) => { _trackJpmMsg(await conn.sendMessage(gid, jpmC, { quoted: FakeChannel })) }
  })

  if (_res.rejected) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai atau hentikan dengan .stopjpm`)

  m.reply(`${_res.stopped ? '⛔ Dihentikan!' : '✅ Selesai!'}\n📤 Sukses: *${_res.sukses}*\n❌ Gagal: *${_res.gagal}*\n📊 Total: *${_res.index}/${groupIdsJpm.length}*`)
}
break



case 'jpmswgc': {
  if (!isCreator) return reply(mess.owner)

  if (!text && !m.quoted) return m.reply(
    `*Penggunaan .jpmswgc:*\n` +
    `• Kirim teks: .jpmswgc teks pesan\n` +
    `• Kirim foto+caption: reply foto lalu .jpmswgc caption\n\n` +
    `_Pesan akan dikirim sebagai status grup ke semua grup._`
  )

  let jpmswgcContent = {}
  let jpmswgcTempFile = null

  if (m.quoted && /image|video/.test(mime)) {
    try {
      jpmswgcTempFile = await NXL.downloadAndSaveMediaMessage(qmsg)
      if (/image/.test(mime)) {
        jpmswgcContent.image = { url: jpmswgcTempFile }
      } else {
        jpmswgcContent.video = { url: jpmswgcTempFile }
      }
      if (text) jpmswgcContent.caption = text
    } catch (e) {
      return m.reply("❌ Gagal mengambil media: " + e.message)
    }
  } else if (text) {
    jpmswgcContent.text = text
  } else {
    return m.reply("Kirim teks atau reply media.")
  }

  let allGroupsSwgc
  try {
    allGroupsSwgc = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    if (jpmswgcTempFile && fs.existsSync(jpmswgcTempFile)) fs.unlinkSync(jpmswgcTempFile)
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  const groupIdsSwgc = Object.keys(allGroupsSwgc)

  if (!groupIdsSwgc.length) {
    if (jpmswgcTempFile && fs.existsSync(jpmswgcTempFile)) fs.unlinkSync(jpmswgcTempFile)
    return m.reply("❌ Tidak ada grup.")
  }

  await m.reply(`🚀 Mengirim JPMSWGC ke *${groupIdsSwgc.length}* grup...`)

  // [PATCH B] Broadcast lewat engine terpadu (fix T2-05: stop-check + socket-check + lock)
  const _res = await runBroadcast({
    lockFlag: 'statusswgc',
    stopFlag: 'stopswgc',
    targets: groupIdsSwgc,
    delayMs: () => global.JedaSwgc || 5000,
    sendOne: async (conn, gid) => { await groupStatus(gid, { ...jpmswgcContent }) },
    cleanup: () => { if (jpmswgcTempFile && fs.existsSync(jpmswgcTempFile)) fs.unlinkSync(jpmswgcTempFile) }
  })

  if (_res.rejected) return m.reply(`⚠️ Broadcast SWGC sedang berjalan, tunggu selesai atau hentikan dengan .stopswgc`)

  m.reply(`✅ Selesai!\n📤 Sukses: *${_res.sukses}*\n❌ Gagal: *${_res.gagal}*\n📊 Total: *${groupIdsSwgc.length}*`)
}
break

case 'jaserht': {
  if (!isCreator) return reply(mess.owner)
  if (global.statusjpm) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai!`)
  if (!text) return m.reply(`*Contoh :* .jaserht pesannya\n\nFitur ini mengirim JPM + tag semua member grup.`)

  let jaserhtPath
  if (/image/.test(mime)) {
    try { jaserhtPath = await NXL.downloadAndSaveMediaMessage(qmsg) } catch {}
  }

  let allGroupsHt
  try {
    allGroupsHt = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  let blacklistHt = []
  try { blacklistHt = loadBlacklistJpm() } catch { blacklistHt = [] }
  const blacklistHtIds = blacklistHt.map(v => v.id || v)
  const filteredHt = Object.keys(allGroupsHt).filter(id => !blacklistHtIds.includes(id))
  const skippedHt = Object.keys(allGroupsHt).length - filteredHt.length

  const jenisHt = jaserhtPath ? "teks & foto" : "teks"
  await m.reply(`⏳ Memproses JASERHT ${jenisHt} ke *${filteredHt.length}* grup...\n${skippedHt > 0 ? `⛔ *${skippedHt}* grup di-skip (blacklist)` : ''}`)

  // [PATCH B] Broadcast lewat engine terpadu (set lock statusjpm, fix T2-14; mentions dipertahankan di sendOne)
  const _res = await runBroadcast({
    lockFlag: 'statusjpm',
    stopFlag: 'stopjpm',
    targets: filteredHt,
    delayMs: () => global.JedaJpm || 4000,
    sendOne: async (conn, gid) => {
      const members = allGroupsHt[gid]?.participants?.map(e => e.jid || e.id) || []
      const htContent = jaserhtPath
        ? { image: fs.readFileSync(jaserhtPath), caption: text, mentions: members }
        : { text, mentions: members }
      _trackJpmMsg(await conn.sendMessage(gid, htContent, { quoted: FakeChannel }))
    },
    cleanup: () => { if (jaserhtPath && fs.existsSync(jaserhtPath)) fs.unlinkSync(jaserhtPath) }
  })

  if (_res.rejected) return m.reply(`⚠️ JPM sedang berjalan, tunggu sampai selesai!`)

  m.reply(`✅ JASERHT ${jenisHt} selesai!\n📤 Sukses: *${_res.sukses}/${filteredHt.length}*\n${skippedHt > 0 ? `⛔ Di-skip blacklist: *${skippedHt}*` : ''}`)
}
break

case 'notifgrup': {
  if (!isCreator) return reply(mess.owner)
  if (global.notifGrup === undefined) global.notifGrup = true

  const subCmd = args[0]?.toLowerCase()

  if (subCmd === 'on') {
    if (global.notifGrup === true) return reply(`> [ NXL BOT ]\n\`INFO\`\n*Notif Grup sudah ON*\n<>`)
    global.notifGrup = true
    return reply(`> [ NXL BOT ]\n\`BERHASIL\`\n*Notif Grup dinyalakan*\n_Perubahan grup akan dikirim kembali_\n<>`)
  }

  if (subCmd === 'off') {
    if (global.notifGrup === false) return reply(`> [ NXL BOT ]\n\`INFO\`\n*Notif Grup sudah OFF*\n<>`)
    global.notifGrup = false
    return reply(`> [ NXL BOT ]\n\`BERHASIL\`\n*Notif Grup dimatikan*\n_Bot tidak akan kirim notif perubahan grup_\n<>`)
  }


  const status = global.notifGrup !== false ? '🟢 ON' : '🔴 OFF'
  try {
    await NXL.relayMessage(m.chat, {
      interactiveMessage: proto.Message.InteractiveMessage.create({
        contextInfo: {
          isForwarded: true,
          forwardedNewsletterMessageInfo: { newsletterJid: global.idsal || '', newsletterName: wm, serverMessageId: -1 }
        },
        body: proto.Message.InteractiveMessage.Body.create({
          text: `╭─「 *🔔 NOTIF GRUP* 」\n│\n│  Status : *${status}*\n│\n│  Notifikasi saat ada yang:\n│  • Ganti nama grup\n│  • Update deskripsi grup\n│  • Reset link grup\n╰─「 *${wm}* 」`
        }),
        footer: proto.Message.InteractiveMessage.Footer.create({ text: `© ${wm}` }),
        header: proto.Message.InteractiveMessage.Header.create({ title: `🔔 Notif Grup`, hasMediaAttachment: false }),
        nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
          buttons: [
            { name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: '🟢 ON', id: '.notifgrup on' }) },
            { name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: '🔴 OFF', id: '.notifgrup off' }) }
          ]
        })
      })
    }, {})
  } catch (e) {
    reply(`> [ NXL BOT ]\n\`NOTIF GRUP\`\n*Status : ${status}*\n\n_Ketik .notifgrup on/off_\n<>`)
  }
}
break

case 'autojoingc': {
  if (!isCreator) return reply(mess.owner)

  if (global.autoJoinGc === undefined) global.autoJoinGc = false
  if (global.autoJoinGcFilter === undefined) global.autoJoinGcFilter = false

  const subCmd = args[0]?.toLowerCase()

  if (subCmd === 'on') {
    if (global.autoJoinGc) return m.reply(`> [ NXL BOT ]\n\`INFO\`\n*AutoJoin GC sudah ON*\n<>`)
    global.autoJoinGc = true
    autoJoinGcSchedulerStart()
    return m.reply(`> [ NXL BOT ]\n\`BERHASIL\`\n*AutoJoin GC dinyalakan*\n_Jeda: ~100-120 menit per join_\n<>`)
  }
  if (subCmd === 'off') {
    if (!global.autoJoinGc) return m.reply(`> [ NXL BOT ]\n\`INFO\`\n*AutoJoin GC sudah OFF*\n<>`)
    global.autoJoinGc = false
    autoJoinGcSchedulerStop()
    return m.reply(`> [ NXL BOT ]\n\`BERHASIL\`\n*AutoJoin GC dimatikan*\n_Scheduler dihentikan. Queue tetap tersimpan._\n<>`)
  }
  if (subCmd === 'clear') {
    _ajSaveQueue({ queue: [], nextAt: 0 })
    autoJoinGcSchedulerStop()
    return m.reply(`> [ NXL BOT ]\n\`BERHASIL\`\n*Queue AutoJoinGC dikosongkan*\n<>`)
  }

  // Tampilkan status lengkap
  const statusNow = global.autoJoinGc ? '🟢 ON' : '🔴 OFF'
  const filterNow = global.autoJoinGcFilter ? '🟢 ON' : '🔴 OFF'
  const info = autoJoinGcGetStatus()

  let queueList = ''
  if (info.queue.length > 0) {
    queueList = `\n│\n│  *Sedang Menunggu:*\n`
    for (let i = 0; i < info.queue.length; i++) {
      queueList += `│  ${i + 1}. ${info.queue[i].invite}\n`
    }
  }

  const nextJoinText = info.running && info.nextIn > 0
    ? `\n│\n│  *Next Join:*\n│  ${info.nextInFormatted} lagi`
    : info.queueSize > 0
      ? `\n│\n│  *Scheduler:* PAUSED (autojoingc off)`
      : ''

  const statsText = `\n│\n│  *Processed Today:*\n│  ✅ Success: ${info.stats.success}\n│  ❌ Failed: ${info.stats.failed}\n│  ⏭️ Skipped: ${info.stats.skipped}`

  const bodyText = `╭─「 *⚙️ AUTO JOIN GC* 」\n│\n│  Auto Join : *${statusNow}*\n│  Filter    : *${filterNow}*\n│\n│  Queue     : *${info.queueSize}/${info.maxQueue}*${queueList}${nextJoinText}${statsText}\n│\n│  _Jeda: 90 menit + random 10-30 menit_\n│\n│  *.autojoingc on/off*\n│  *.autojoingc clear*\n│  *.autojoingcfilter on/off*\n│  *.addjoinfilter* • *.deljoinfilter*\n╰─「 *${wm}* 」`

  try {
    const panelMsg = {
      interactiveMessage: proto.Message.InteractiveMessage.create({
        contextInfo: {
          isForwarded: true,
          forwardedNewsletterMessageInfo: {
            newsletterJid: global.idsal || '',
            newsletterName: wm,
            serverMessageId: -1
          }
        },
        body: proto.Message.InteractiveMessage.Body.create({ text: bodyText }),
        footer: proto.Message.InteractiveMessage.Footer.create({ text: `© ${wm}` }),
        header: proto.Message.InteractiveMessage.Header.create({ title: `⚙️ Auto Join GC`, hasMediaAttachment: false }),
        nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
          buttons: [
            { name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: '🟢 ON', id: '.autojoingc on' }) },
            { name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: '🔴 OFF', id: '.autojoingc off' }) }
          ]
        })
      })
    }
    await NXL.relayMessage(m.chat, panelMsg, {})
  } catch (e) {
    m.reply(bodyText)
  }
}
break





case 'autojoingcfilter': {
  if (!isCreator) return reply(mess.owner)
  if (global.autoJoinGcFilter === undefined) global.autoJoinGcFilter = false

  const sub = args[0]?.toLowerCase()

  if (sub === 'on') {
    if (global.autoJoinGcFilter) return m.reply(`> [ NXL BOT ]\n\`INFO\`\n*Join Filter sudah ON*\n<>`)
    global.autoJoinGcFilter = true
    return m.reply(`> [ NXL BOT ]\n\`BERHASIL\`\n*Join Filter AKTIF*\n_Bot hanya auto join dari grup/nomor yang ada di daftar filter._\n_Gunakan .addjoinfilter untuk menambah whitelist._\n<>`)
  }
  if (sub === 'off') {
    if (!global.autoJoinGcFilter) return m.reply(`> [ NXL BOT ]\n\`INFO\`\n*Join Filter sudah OFF*\n<>`)
    global.autoJoinGcFilter = false
    return m.reply(`> [ NXL BOT ]\n\`BERHASIL\`\n*Join Filter DIMATIKAN*\n_Bot akan auto join dari link manapun._\n<>`)
  }


  const filterNow2 = global.autoJoinGcFilter ? '🟢 AKTIF' : '🔴 NONAKTIF'
  const jfList = loadJoinFilter()
  try {
    const panelMsg = {
      interactiveMessage: proto.Message.InteractiveMessage.create({
        contextInfo: {
          isForwarded: true,
          forwardedNewsletterMessageInfo: {
            newsletterJid: global.idsal || '',
            newsletterName: wm,
            serverMessageId: -1
          }
        },
        body: proto.Message.InteractiveMessage.Body.create({
          text: `╭─「 *🔒 JOIN FILTER (WHITELIST)* 」\n│\n│  Status  : *${filterNow2}*\n│  Total   : *${jfList.length} entri*\n│\n│  _Jika aktif, bot hanya auto join_\n│  _dari grup/nomor di daftar ini._\n│\n│  • *.addjoinfilter* — tambah\n│  • *.deljoinfilter* — hapus\n│  • *.listjoinfilter* — lihat daftar\n╰─「 *${wm}* 」`
        }),
        footer: proto.Message.InteractiveMessage.Footer.create({ text: `© ${wm}` }),
        header: proto.Message.InteractiveMessage.Header.create({
          title: `🔒 Join Filter`,
          hasMediaAttachment: false
        }),
        nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
          buttons: [
            { name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: '🟢 Aktifkan Filter', id: '.autojoingcfilter on' }) },
            { name: 'quick_reply', buttonParamsJson: JSON.stringify({ display_text: '🔴 Matikan Filter', id: '.autojoingcfilter off' }) }
          ]
        })
      })
    }
    await NXL.relayMessage(m.chat, panelMsg, {})
  } catch (e) {
    m.reply(`> [ NXL BOT ]\n\`JOIN FILTER\`\n*Status : ${filterNow2}*\n*Total  : ${jfList.length} entri*\n\n_Ketik .autojoingcfilter on/off_\n<>`)
  }
}
break

case 'addjoinfilter': {
  if (!isCreator) return reply(mess.owner)

  let filterList2
  try { filterList2 = loadJoinFilter() } catch { filterList2 = [] }


  if (text) {
    const rawId = text.trim()

    let jid = rawId
    if (/^\d+$/.test(rawId)) jid = rawId + '@s.whatsapp.net'

    if (filterList2.find(v => v.id === jid)) {
      return m.reply(`> [ NXL BOT ]\n\`INFO\`\n*${jid} sudah ada di Join Filter*\n<>`)
    }
    filterList2.push({ id: jid, name: jid, type: jid.endsWith('@g.us') ? 'group' : 'number' })
    saveJoinFilter(filterList2)
    return m.reply(`> [ NXL BOT ]\n\`BERHASIL\`\n*Ditambahkan ke Join Filter:*\n📌 ${jid}\n\n_Total filter: ${filterList2.length} entri_\n<>`)
  }


  let grupData
  try {
    grupData = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    return m.reply(`❌ Gagal mengambil daftar grup: ${e.message}`)
  }
  const grupList = Object.values(grupData)
  if (grupList.length < 1) return m.reply('Bot tidak ada di grup manapun.')

  let rows = grupList.map(u => ({
    title: u.subject || 'Unknown',
    description: `ID: ${u.id}`,
    id: `.addjoinfilter-response ${u.id}`
  }))

  await NXL.sendMessage(m.chat, {
    buttons: [{
      buttonId: 'action',
      buttonText: { displayText: 'Pilih Grup' },
      type: 4,
      nativeFlowInfo: {
        name: 'single_select',
        paramsJson: JSON.stringify({
          title: 'Pilih Grup untuk Whitelist',
          sections: [{ title: `© Powered By ${ownername}`, rows }]
        })
      }
    }],
    headerType: 1,
    viewOnce: true,
    text: `\nPilih grup yang akan masuk Join Filter (whitelist)\natau ketik: *.addjoinfilter 628xxx* untuk nomor\n`
  }, { quoted: m })
}
break

case 'addjoinfilter-response': {
  if (!isCreator) return reply(mess.owner)
  const gjid = text.trim()
  let res2
  try { res2 = await NXL.groupMetadata(gjid) } catch (e) {
    return m.reply(`Gagal ambil data grup!\nError: ${e.message}`)
  }
  let filterList3 = []
  try { filterList3 = loadJoinFilter() } catch { filterList3 = [] }
  if (filterList3.find(v => v.id === gjid)) {
    return m.reply(`Grup *${res2.subject}* sudah ada di Join Filter!`)
  }
  filterList3.push({ id: gjid, name: res2.subject, type: 'group' })
  saveJoinFilter(filterList3)
  m.reply(`✅ *${res2.subject}* ditambahkan ke Join Filter.\n\n_Total filter: ${filterList3.length} entri_`)
}
break

case 'deljoinfilter': {
  if (!isCreator) return reply(mess.owner)
  let filterList4 = []
  try { filterList4 = loadJoinFilter() } catch { filterList4 = [] }
  if (filterList4.length < 1) return m.reply('Daftar Join Filter kosong.')

  if (!text) {
    const list4 = filterList4.map((v, i) => `${i + 1}. *${v.name || v.id}*\n    ${v.id}`).join('\n\n')
    return m.reply(`*Daftar Join Filter:*\n\n${list4}\n\n_Ketik: .deljoinfilter <nomor urut>_\n_Contoh: .deljoinfilter 1_`)
  }

  const idx = parseInt(text.trim()) - 1
  if (isNaN(idx) || idx < 0 || idx >= filterList4.length) {
    return m.reply(`Nomor tidak valid! Masukan angka 1 - ${filterList4.length}`)
  }
  const removed2 = filterList4.splice(idx, 1)[0]
  saveJoinFilter(filterList4)
  m.reply(`✅ *${removed2.name || removed2.id}* dihapus dari Join Filter.\n\n_Sisa filter: ${filterList4.length} entri_`)
}
break

case 'listjoinfilter': {
  if (!isCreator) return reply(mess.owner)
  let filterList5 = []
  try { filterList5 = loadJoinFilter() } catch { filterList5 = [] }
  if (filterList5.length < 1) return m.reply(`> [ NXL BOT ]\n\`JOIN FILTER\`\n*Daftar whitelist kosong.*\n_Tambah dengan .addjoinfilter_\n<>`)

  const listTeks = filterList5.map((v, i) => {
    const icon = v.type === 'group' ? '👥' : '📱'
    return `${i + 1}. ${icon} *${v.name || v.id}*\n    \`${v.id}\``
  }).join('\n\n')

  const filterStatus = global.autoJoinGcFilter ? '🟢 AKTIF' : '🔴 NONAKTIF'
  m.reply(`╭─「 *📋 LIST JOIN FILTER* 」\n│  Status Filter: ${filterStatus}\n│  Total: *${filterList5.length} entri*\n╰──────────────────\n\n${listTeks}`)
}
break

case 'blswgc': {
  if (!isCreator) return m.reply(mess.owner)
  const list = loadBlacklistSwgc()
  if (list.includes(text)) return m.reply("Grup sudah ada di blacklist.")
  list.push(text)
  saveBlacklistSwgc(list)
  m.reply(`✅ Grup *${text}* ditambahkan ke blacklist swgc.`)
}
break

case 'delblswgc': {
  if (!isCreator) return reply(mess.owner)
  if (!text) return m.reply(`Contoh: .delblswgc 120363xxxxxx@g.us`)
  let list = loadBlacklistSwgc()
  if (!list.includes(text)) return m.reply("Grup tidak ada di blacklist.")
  list = list.filter(v => v !== text)
  saveBlacklistSwgc(list)
  m.reply(`✅ Grup *${text}* dihapus dari blacklist swgc.`)
}
break

case 'listblswgc': {
  if (!isCreator) return reply(mess.owner)
  const list = loadBlacklistSwgc()
  if (!list.length) return m.reply("Blacklist swgc kosong.")
  m.reply(`*Blacklist SWGC:*\n${list.map((v, i) => `${i + 1}. ${v}`).join('\n')}`)
}
break

case "listproduk": {
  const produk = JSON.parse(fs.readFileSync("./database/produk.json"))
  if (produk.length < 1) return m.reply("Belum ada produk.")

  let teks = `*Daftar Produk ${ownername}*\n${"─".repeat(25)}\n\n`
  produk.forEach((p, i) => {
    teks += `*${i + 1}. ${p.nama}*\n`
    teks += `💰 Harga : Rp${Number(p.harga).toLocaleString("id-ID")}\n`
    teks += `📦 Stok  : ${p.stok > 0 ? p.stok + " pcs" : "Habis"}\n`
    teks += `📝 Info  : ${p.deskripsi}\n\n`
  })
  teks += `_Total ${produk.length} produk tersedia_`

  return m.reply(teks)
}
break

case "addproduk": {
  if (!isCreator) return m.reply(mess.owner)
  if (!text) return m.reply(
    `*Format Tambah Produk:*\n` +
    `.addproduk Nama | Harga | Stok | Deskripsi\n\n` +
    `*Contoh:*\n` +
    `.addproduk Script Bot | 50000 | 10 | Script WA bot lengkap`
  )

  const parts = text.split("|").map(s => s.trim())
  if (parts.length < 4) return m.reply("Format salah. Gunakan: Nama | Harga | Stok | Deskripsi")

  const [nama, harga, stok, deskripsi] = parts
  if (isNaN(harga) || isNaN(stok)) return m.reply("Harga dan stok harus angka.")

  let gambar = null
  if (/image/.test(mime)) {
    const mediaPath = await NXL.downloadAndSaveMediaMessage(qmsg)
    gambar = fs.readFileSync(mediaPath).toString("base64")
    fs.unlinkSync(mediaPath)
  }

  const produk = JSON.parse(fs.readFileSync("./database/produk.json"))
  const id = Date.now()
  produk.push({ id, nama, harga: Number(harga), stok: Number(stok), deskripsi, gambar })
  safeWriteJSON("./database/produk.json", produk)

  return m.reply(`Produk *${nama}* berhasil ditambahkan!\nID: ${id}`)
}
break

case "delproduk": {
  if (!isCreator) return m.reply(mess.owner)
  if (!text) return m.reply("Masukkan ID atau nomor urut produk.\nContoh: .delproduk 1")

  let produk = JSON.parse(fs.readFileSync("./database/produk.json"))
  if (produk.length < 1) return m.reply("Belum ada produk.")

  let target
  if (!isNaN(text)) {
    const idx = Number(text) - 1
    if (idx < 0 || idx >= produk.length) return m.reply("Nomor produk tidak valid.")
    target = produk[idx]
    produk.splice(idx, 1)
  } else {
    target = produk.find(p => String(p.id) === text.trim())
    if (!target) return m.reply("Produk tidak ditemukan.")
    produk = produk.filter(p => String(p.id) !== text.trim())
  }

  safeWriteJSON("./database/produk.json", produk)
  return m.reply(`Produk *${target.nama}* berhasil dihapus.`)
}
break

case "setpromo": {
  if (!isCreator) return m.reply(mess.owner)
  if (!text) return m.reply(
    `*Format Setpromo:*\n` +
    `.setpromo target | interval\n\n` +
    `*Target:* grup / channel / kontak\n` +
    `*Interval:* dalam menit\n\n` +
    `*Contoh:*\n` +
    `.setpromo grup | 60`
  )

  const parts = text.split("|").map(s => s.trim())
  if (parts.length < 2) return m.reply("Format salah. Gunakan: target | interval (menit)")

  const [target, intervalMenit] = parts
  const validTarget = ["grup", "channel", "kontak"]
  if (!validTarget.includes(target.toLowerCase())) return m.reply(`Target harus: ${validTarget.join(" / ")}`)
  if (isNaN(intervalMenit) || Number(intervalMenit) < 1) return m.reply("Interval minimal 1 menit.")

  let promo = {}
  try { const _raw = JSON.parse(fs.readFileSync("./database/promo.json")); promo = (!_raw || Array.isArray(_raw) || typeof _raw !== 'object') ? {} : _raw } catch { promo = { status: false } }
  promo.target = target.toLowerCase()
  promo.interval = Number(intervalMenit) * 60000
  if (!fs.existsSync('./database')) fs.mkdirSync('./database', { recursive: true })
  safeWriteJSON("./database/promo.json", promo)

  return m.reply(`✅ Promo diset ke *${target}* setiap *${intervalMenit} menit*.`)
}
break

case "autopromo": {
  if (!isCreator) return m.reply(mess.owner)

  const _rawPromo = JSON.parse(fs.readFileSync("./database/promo.json"))
  const promo = (!_rawPromo || Array.isArray(_rawPromo) || typeof _rawPromo !== 'object') ? {} : _rawPromo
  const produk = JSON.parse(fs.readFileSync("./database/produk.json"))
  if (produk.length < 1) return m.reply("Belum ada produk. Tambahkan dulu dengan .addproduk")
  if (!promo.target || !promo.interval) return m.reply("Belum ada setting promo. Gunakan .setpromo dulu.")

  if (global.intervalPromo) {
    clearInterval(global.intervalPromo)
    delete global.intervalPromo
    promo.status = false
    safeWriteJSON("./database/promo.json", promo)
    return m.reply("Autopromo dimatikan.")
  }

  promo.status = true
  safeWriteJSON("./database/promo.json", promo)
  await m.reply(`Autopromo dinyalakan!\nTarget: *${promo.target}* | Interval: *${promo.interval / 60000} menit*`)

  const kirimPromo = async () => {
    // [PATCH A] Cegah tumpang-tindih tick: jika tick sebelumnya masih berjalan
    // (loop lebih lama dari interval), lewati tick ini (root cause F6-01).
    if (global._autopromoRunning) return
    // [PATCH A] Pakai socket hidup; jika koneksi mati, lewati tick. Interval juga
    // sudah di-clear saat disconnect — ini lapisan pertahanan tambahan (root cause RC-2).
    const _conn = global.getLiveConn ? global.getLiveConn() : null
    if (!_conn) return
    global._autopromoRunning = true
    try {
    const dataProduk = JSON.parse(fs.readFileSync("./database/produk.json"))
    if (dataProduk.length < 1) return

    const p = dataProduk[Math.floor(Math.random() * dataProduk.length)]
    const teks = `*PROMO ${ownername}*\n${"─".repeat(20)}\n\n*${p.nama}*\n💰 Harga : Rp${Number(p.harga).toLocaleString("id-ID")}\n📦 Stok  : ${p.stok > 0 ? p.stok + " pcs" : "Habis"}\n📝 Info  : ${p.deskripsi}\n\n_Hubungi owner untuk order!_`

    let targetList = []

    if (promo.target === "grup") {
      try {
        const allGroups = await Promise.race([
          _conn.groupFetchAllParticipating(),
          new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
        ])
        targetList = Object.keys(allGroups)
      } catch { targetList = [] }
    } else if (promo.target === "channel") {
      const all = [...(global.store?.chats?.array || [])]
      targetList = all.filter(c => c.id?.endsWith("@newsletter")).map(c => c.id)
    } else if (promo.target === "kontak") {
      const all = [...(global.store?.chats?.array || [])]
      targetList = all.filter(c => c.id?.endsWith("@s.whatsapp.net")).map(c => c.id)
    }

    for (const id of targetList) {
      // [PATCH A] Hentikan loop bila koneksi putus di tengah broadcast promo.
      if (!global._connAlive) break
      try {
        if (p.gambar) {
          // Kirim polos tanpa kartu externalAdReply (preview link dari URL di caption)
          await _conn.sendMessage(id, {
            image: Buffer.from(p.gambar, "base64"),
            caption: teks
          })
        } else {
          // Teks polos -> Baileys auto-generate preview link thumbnail (identik JPM utama)
          await _conn.sendMessage(id, {
            text: teks
          })
        }
      } catch {
      }
      // [GANTI] Jeda ikut global setjeda JPM (settings.js / .jedajpm), bukan hardcode 3 dtk
      await sleep(global.JedaJpm || 4000)
    }
    } finally {
      // [PATCH A] Selalu lepas guard overlap, apa pun yang terjadi.
      global._autopromoRunning = false
    }
  }

  global.intervalPromo = setInterval(kirimPromo, promo.interval)
  await kirimPromo()
}
break

case 'nxlai': {
  if (!args[0]) return reply(`> [ NXL BOT ]
\`PENGGUNAAN SALAH\`
*contoh ${command} on/off*
<>`);

  if (args[0] === 'on') {
    if (global.db.users[m.sender].NXL) return reply(`> [ NXL BOT ]
\`PENGGUNAAN BENAR\`
*sudah on sebelumnya*
<>`);
    global.db.users[m.sender].NXL = true
    reply(`> [ NXL BOT ]
\`PENGGUNAAN BENAR\`
*berhasil ke mode on*
<>`);
  } else if (args[0] === 'off') {
    if (!global.db.users[m.sender].NXL) return reply(`> [ NXL BOT ]
\`PENGGUNAAN BENAR\`
*sudah off sebelumnya*
<>`);
    global.db.users[m.sender].NXL = false
    reply(`> [ NXL BOT ]
\`PENGGUNAAN BENAR\`
*berhasil ke mode off*
<>`);
  }
}
break

case 'imagine': {
  if (!text) return reply(`> [ NXL BOT ]
\`PENGGUNAAN SALAH\`
*contoh: .imagine kucing lucu anime*
<>`)
  try {
    const imageUrl = `${global.apiPollinations}/prompt/${encodeURIComponent(text)}?nologo=true&width=512&height=512`
    await NXL.sendMessage(from, {
      image: { url: imageUrl },
      caption: `🎨 *Generated by ${ownername}*\n_${text}_`
    }, { quoted: m })
  } catch (err) {
    reply('Gagal generate gambar: ' + err.message)
  }
}
break

case "mute": {
  if (!isCreator) return m.reply(mess.owner)

  let mutePath = './database/mutegrup.json'
  let muteList = []
  try { muteList = JSON.parse(fs.readFileSync(mutePath, 'utf8')) } catch { muteList = [] }

  if (m.isGroup) {
    if (!isAdmins && !isCreator) return m.reply("Hanya admin grup atau owner yang dapat menonaktifkan bot.")
    if (muteList.includes(from)) return m.reply("Bot sudah dinonaktifkan di grup ini.")
    muteList.push(from)
    fs.writeFileSync(mutePath, JSON.stringify(muteList, null, 2))
    return m.reply(`Bot telah dinonaktifkan di grup ini.\nKetik .unmute untuk mengaktifkan kembali.`)
  }

  const allGroups = await NXL.groupFetchAllParticipating()
  const groupEntries = Object.entries(allGroups)
  if (groupEntries.length === 0) return m.reply("Bot tidak terdaftar di grup manapun.")

  const rows = groupEntries.map(([id, meta]) => ({
    title: meta.subject || id,
    description: muteList.includes(id) ? "Dinonaktifkan" : "Aktif",
    id: `.mute-toggle ${id}`
  }))


  const chunkSize = 24
  const sections = []
  for (let i = 0; i < rows.length; i += chunkSize) {
    sections.push({
      title: `Nonaktifkan Bot — ${ownername} (${Math.floor(i/chunkSize)+1})`,
      rows: rows.slice(i, i + chunkSize)
    })
  }

  await NXL.sendMessage(m.chat, {
    buttons: [{
      buttonId: 'mute_select',
      buttonText: { displayText: 'Pilih Grup' },
      type: 4,
      nativeFlowInfo: {
        name: 'single_select',
        paramsJson: JSON.stringify({
          title: 'Pilih Grup untuk Mute/Unmute',
          sections
        })
      }
    }],
    headerType: 1,
    viewOnce: true,
    text: `Nonaktifkan Bot per Grup\nSilakan pilih grup yang ingin dinonaktifkan atau diaktifkan.\n\nStatus: Dinonaktifkan / Aktif`
  }, { quoted: m })
}
break

case "mute-toggle": {
  const groupId = text.trim()
  if (!isCreator) return m.reply(mess.owner)
  if (!groupId || !groupId.endsWith('@g.us')) return m.reply("ID grup tidak valid.")

  let mutePath = './database/mutegrup.json'
  let muteList = []
  try { muteList = JSON.parse(fs.readFileSync(mutePath, 'utf8')) } catch { muteList = [] }

  let groupName2 = groupId
  try { const meta = await NXL.groupMetadata(groupId); groupName2 = meta.subject } catch {}

  if (muteList.includes(groupId)) {
    muteList = muteList.filter(id => id !== groupId)
    fs.writeFileSync(mutePath, JSON.stringify(muteList, null, 2))
    return m.reply(`Bot telah diaktifkan kembali di:\n${groupName2}`)
  } else {
    muteList.push(groupId)
    fs.writeFileSync(mutePath, JSON.stringify(muteList, null, 2))
    return m.reply(`Bot telah dinonaktifkan di:\n${groupName2}`)
  }
}
break

case "unmute": {
  if (!isCreator) return m.reply(mess.owner)

  let mutePath = './database/mutegrup.json'
  let muteList = []
  try { muteList = JSON.parse(fs.readFileSync(mutePath, 'utf8')) } catch { muteList = [] }

  if (m.isGroup) {
    if (!isAdmins && !isCreator) return m.reply("Hanya admin grup atau owner yang dapat mengaktifkan kembali bot.")
    if (!muteList.includes(from)) return m.reply("Bot tidak sedang dinonaktifkan di grup ini.")
    muteList = muteList.filter(id => id !== from)
    fs.writeFileSync(mutePath, JSON.stringify(muteList, null, 2))
    return m.reply("Bot telah diaktifkan kembali di grup ini.")
  }

  if (muteList.length === 0) return m.reply("Tidak ada grup yang sedang dinonaktifkan.")

  const rows = []
  for (const gid of muteList) {
    let gname = gid
    try { const meta = await NXL.groupMetadata(gid); gname = meta.subject } catch {}
    rows.push({
      title: gname,
      description: "Ketuk untuk mengaktifkan kembali",
      id: `.mute-toggle ${gid}`
    })
  }

  await NXL.sendMessage(m.chat, {
    buttons: [{
      buttonId: 'unmute_select',
      buttonText: { displayText: 'Pilih Grup' },
      type: 4,
      nativeFlowInfo: {
        name: 'single_select',
        paramsJson: JSON.stringify({
          title: 'Pilih Grup untuk Diaktifkan Kembali',
          sections: [{
            title: `Aktifkan Kembali Bot — ${ownername}`,
            rows: rows.slice(0, 24)
          }]
        })
      }
    }],
    headerType: 1,
    viewOnce: true,
    text: `Aktifkan Kembali Bot\nBerikut adalah daftar grup yang sedang dinonaktifkan.\nSilakan pilih grup yang ingin diaktifkan kembali.`
  }, { quoted: m })
}
break

case "muteinfo": {
  if (!isCreator) return m.reply(mess.owner);

  const mutePath = './database/mutegrup.json';
  let muteList = [];

  try {
    muteList = JSON.parse(fs.readFileSync(mutePath, 'utf8'));
  } catch {
    muteList = [];
  }


  if (m.isGroup) {
    const isMuted = muteList.includes(from);
    const status = isMuted
      ? "NONAKTIF — bot tidak merespons di grup ini."
      : "AKTIF — bot berjalan normal.";
    return m.reply(`Status bot di grup ini:\n${status}`);
  }


  if (muteList.length === 0) {
    return m.reply("Semua grup dalam kondisi aktif, tidak ada yang dinonaktifkan.");
  }


  let info = `Daftar Grup yang Dinonaktifkan\n${"─".repeat(25)}\n`;

  for (const gid of muteList) {
    let gname = gid;
    try {
      const meta = await NXL.groupMetadata(gid);
      gname = meta.subject;
    } catch {
      gname = `Grup Tidak Diketahui (${gid.split('@')[0]})`;
    }
    info += `• ${gname}\n`;
  }

  return m.reply(info.trim());
}
break;


case "warn": {
  if (!m.isGroup) return m.reply("Command ini hanya untuk grup.")
  if (!isAdmins && !isCreator) return m.reply("Hanya admin / owner yang bisa warn.")

  // [WARN] Resolusi identifier apa pun (lid/id/jid/nomor) menjadi participant +
  // JID nomor asli (@s.whatsapp.net). SELALU utamakan JID kanonik nomor asli,
  // tidak pernah menyimpan angka LID mentah. Identitas ini identik dengan yang
  // dihitung blok auto-delete warn sehingga key simpan == key baca.
  const resolveMember = (jid) => {
    if (!jid) return { jid: null, part: null }
    const rawNum = jid.replace(/@.*$/, '')
    let part = null
    if (participants && participants.length) {
      part = participants.find(p =>
        (p.lid && p.lid.replace(/@.*$/, '') === rawNum) ||
        (p.id  && p.id.replace(/@.*$/, '')  === rawNum) ||
        (p.jid && p.jid.replace(/@.*$/, '') === rawNum)
      ) || null
    }
    let phone = null
    if (part) {
      if (part.jid && part.jid.includes('@s.whatsapp.net')) phone = part.jid.replace(/@.*$/, '') + '@s.whatsapp.net'
      else if (part.id && part.id.includes('@s.whatsapp.net')) phone = part.id.replace(/@.*$/, '') + '@s.whatsapp.net'
    }
    if (!phone) phone = rawNum + '@s.whatsapp.net'
    return { jid: phone, part }
  }

  let rawTarget = null
  if (m.quoted) {
    rawTarget = m.quoted.sender
  } else if (m.mentionedJid && m.mentionedJid.length > 0) {
    rawTarget = m.mentionedJid[0]
  } else if (text) {
    const mentionMatch = text.match(/@([0-9]{8,15})/)
    if (mentionMatch) rawTarget = mentionMatch[1] + '@s.whatsapp.net'
  }

  const { jid: warnTarget, part: _targetPart } = resolveMember(rawTarget)
  console.log(`[WARN CMD] raw:${rawTarget} → resolved:${warnTarget}`)

  if (!warnTarget) return m.reply(`Cara pakai:\n- Reply pesan member: .warn\n- Tag member: .warn @nomor`)
  const _wtNum = warnTarget.split('@')[0]
  if (_wtNum === _botJid.split('@')[0]) return m.reply("Tidak bisa warn bot sendiri.")
  // [PLAN A] Owner & admin kebal warn — tolak dengan pesan jelas (bukan simpan diam-diam).
  if ((global.owner || []).some(o => o.replace(/[^0-9]/g, '') === _wtNum)) return m.reply("Tidak bisa warn owner bot.")
  if (_targetPart && (_targetPart.admin === 'admin' || _targetPart.admin === 'superadmin')) return m.reply("Tidak bisa warn admin grup.")

  let warnPath = './database/warndata.json'
  let warnData = {}
  try {
    const raw = JSON.parse(fs.readFileSync(warnPath, 'utf8'))
    warnData = Array.isArray(raw) ? {} : raw
  } catch { warnData = {} }

  if (!warnData[from]) warnData[from] = {}
  if (!warnData[from][warnTarget]) warnData[from][warnTarget] = 0

  warnData[from][warnTarget]++
  const warnCount = warnData[from][warnTarget]
  const maxWarn = 3

  safeWriteJSON(warnPath, warnData)

  if (warnCount >= maxWarn) {
    warnData[from][warnTarget] = 0
    safeWriteJSON(warnPath, warnData)

    await NXL.sendMessage(m.chat, {
      text: `*WARN ${maxWarn}/${maxWarn}*\n@${warnTarget.split('@')[0]} telah mencapai batas warn!\nMember dikick dari grup.`,
      mentions: [warnTarget]
    }, { quoted: m })

    try {
      // Kick memakai id participant asli (yang dikenali WA) bila ada, fallback ke JID.
      await NXL.groupParticipantsUpdate(m.chat, [_targetPart?.id || warnTarget], "remove")
    } catch (e) {
      await m.reply("Gagal kick: " + e.message)
    }
  } else {
    await NXL.sendMessage(m.chat, {
      text: `*WARN ${warnCount}/${maxWarn}*\n@${warnTarget.split('@')[0]} mendapat peringatan.\n${warnCount === 2 ? 'Satu warn lagi = KICK!' : 'Hati-hati, jangan melanggar aturan grup.'}`,
      mentions: [warnTarget]
    }, { quoted: m })
  }
}
break

case "warnlist": {
  if (!m.isGroup) return m.reply("Command ini hanya untuk grup.")
  if (!isAdmins && !isCreator) return m.reply("Hanya admin / owner.")

  let warnPath = './database/warndata.json'
  let warnData = {}
  try { const _r = JSON.parse(fs.readFileSync(warnPath, 'utf8')); warnData = Array.isArray(_r) ? {} : _r } catch { warnData = {} }

  const groupWarn = warnData[from] || {}
  const entries = Object.entries(groupWarn).filter(([, v]) => v > 0)

  if (entries.length === 0) return m.reply("Tidak ada member yang punya warn di grup ini.")

  let list = `*Daftar Warn — ${groupName}*\n${"─".repeat(25)}\n`
  entries.forEach(([uid, count], i) => {
    list += `${i + 1}. @${uid.split('@')[0]} -> ${count}/3\n`
  })

  await NXL.sendMessage(from, {
    text: list,
    mentions: entries.map(([uid]) => uid)
  }, { quoted: m })
}
break

case "resetwarn": {
  if (!m.isGroup) return m.reply("Command ini hanya untuk grup.")
  if (!isAdmins && !isCreator) return m.reply("Hanya admin / owner yang bisa reset warn.")

  const toSWA2 = (jid) => {
    if (!jid) return null
    const rawNum = jid.replace(/@.*$/, '')
    if (participants && participants.length) {
      // [FIX WARN] Resolusi identik toSWA agar target reset cocok dengan key
      // yang tersimpan (JID kanonik / nomor asli participant).
      const found = participants.find(p =>
        (p.lid && p.lid.replace(/@.*$/, '') === rawNum) ||
        (p.id  && p.id.replace(/@.*$/, '')  === rawNum) ||
        (p.jid && p.jid.replace(/@.*$/, '') === rawNum)
      )
      if (found) {
        if (found.jid) return found.jid.replace(/@.*$/, '') + '@s.whatsapp.net'
        if (found.id && found.id.includes('@s.whatsapp.net')) return found.id
      }
    }
    return rawNum + '@s.whatsapp.net'
  }

  let rawTarget = null
  if (m.quoted) {
    rawTarget = m.quoted.sender
  } else if (m.mentionedJid && m.mentionedJid.length > 0) {
    rawTarget = m.mentionedJid[0]
  } else if (text) {
    const mentionMatch = text.match(/@([0-9]{8,15})/)
    if (mentionMatch) rawTarget = mentionMatch[1] + '@s.whatsapp.net'
  }

  const warnTarget = toSWA2(rawTarget)
  console.log(`[RESETWARN CMD] raw:${rawTarget} → resolved:${warnTarget}`)
  if (!warnTarget) return m.reply(`Cara pakai:\n- Reply pesan member: .resetwarn\n- Tag member: .resetwarn @nomor`)

  let warnPath = './database/warndata.json'
  let warnData = {}
  try { const _r = JSON.parse(fs.readFileSync(warnPath, 'utf8')); warnData = Array.isArray(_r) ? {} : _r } catch { warnData = {} }

  const _groupWarn = warnData[from] || {}
  const targetNum = warnTarget.split('@')[0]
  const _existingKey = Object.keys(_groupWarn).find(k => k.split('@')[0] === targetNum)

  if (!_existingKey || _groupWarn[_existingKey] === 0) {
    return await NXL.sendMessage(from, {
      text: `@${warnTarget.split('@')[0]} tidak punya warn.`,
      mentions: [warnTarget]
    }, { quoted: m })
  }

  warnData[from][_existingKey] = 0
  safeWriteJSON(warnPath, warnData)

  await NXL.sendMessage(from, {
    text: `✅ Warn @${warnTarget.split('@')[0]} berhasil direset.`,
    mentions: [warnTarget]
  }, { quoted: m })
}
break



case "tagadmin": {
  if (!m.isGroup) return m.reply(mess.group)
  if (!isCreator && !isAdmins) return m.reply(mess.admin)
  if (!text) return m.reply(`*Contoh:* ${command} pesannya`)
  let teksTag = text + "\n\n"
  const adminListTag = groupAdmins.filter(e => e !== botNumber && e !== m.sender)
  if (!adminListTag.length) return m.reply('⚠️ Tidak ada admin lain di grup ini.')
  adminListTag.forEach(e => { teksTag += `@${e.split("@")[0]}\n` })
  await NXL.sendMessage(m.chat, { text: teksTag, mentions: adminListTag }, { quoted: m })
}
break

case "fitnah": case "fakereply": {
  if (!isCreator) return m.reply(mess.owner)
  if (!text) return m.reply(`*Contoh:* ${command} 6281xxx|pesan palsu|balasan`)
  let fitnahParts = text.split('|')
  if (fitnahParts.length < 3) return m.reply(`Format: nomor|pesan user|balasan bot`)
  let fitnahTarget = ''
  if (m.mentionedJid?.[0]) { fitnahTarget = m.mentionedJid[0] }
  else { let n = fitnahParts[0].replace(/[^0-9]/g,''); if(!n.startsWith('62'))n='62'+n; fitnahTarget=n+'@s.whatsapp.net' }
  if (!fitnahTarget) return m.reply('Target tidak valid')
  let fitnahNama = fitnahTarget.split('@')[0]
  await NXL.sendMessage(m.chat, { text: fitnahParts[fitnahParts.length-1], contextInfo: { quotedMessage: { conversation: fitnahParts[fitnahParts.length-2] }, stanzaId: 'FAKE_'+Date.now(), participant: fitnahTarget } })
}
break

case "artinama": {
  if (!text) return m.reply(`*Contoh:* ${command} nama`)
  const artiList = ['Baik hati','Berjiwa pemimpin','Kreatif','Setia','Pemberani','Cerdas','Penyabar','Penuh semangat','Mulia','Berhati lembut','Pekerja keras','Beruntung']
  let artiResult = `✨ *Arti Nama: ${text}*\n\n`
  text.split('').forEach(l => { if(l.trim()) artiResult += `*${l.toUpperCase()}* — ${artiList[Math.floor(Math.random()*artiList.length)]}\n` })
  m.reply(artiResult)
}
break

case "cekkodam": {

  const _resolveFunJid = (jid) => {
    if (!jid) return null
    const rawNum = jid.replace(/@.*$/, '')
    if (participants && participants.length) {
      if (jid.includes('@lid') || !jid.includes('@s.whatsapp.net')) {
        const found = participants.find(p => (p.lid || p.id || '').replace(/@.*$/, '') === rawNum)
        if (found && found.jid) return found.jid.replace(/@.*$/, '') + '@s.whatsapp.net'
        if (found && found.id && found.id.includes('@s.whatsapp.net')) return found.id
      }
      const found = participants.find(p => (p.jid || p.id || '').replace(/@.*$/, '') === rawNum)
      if (found) return (found.jid || found.id).replace(/@.*$/, '') + '@s.whatsapp.net'
    }
    return rawNum + '@s.whatsapp.net'
  }
  let kodamRaw = m.mentionedJid?.[0] || (m.quoted ? m.quoted.sender : null) || (text ? text.replace(/[^0-9]/g,'') + '@s.whatsapp.net' : null) || m.sender
  const kodamTarget = _resolveFunJid(kodamRaw)
  const kodamNum = kodamTarget ? kodamTarget.split('@')[0] : (text || pushname)
  const namaKodam = ['Naga Hitam Selatan','Macan Putih Bersurai','Ular Sakti Bukit Tinggi','Khodam Nyi Roro Kidul','Singa Padang Pasir','Garuda Emas Nusantara','Buaya Putih Kalimantan','Harimau Loreng Sumatera','Elang Jawa Bermahkota','Kebo Bule Keraton','Macan Kumbang Hutan Larangan','Ratu Lebah Madu','Serigala Bulan Purnama','Kucing Candramawa','Burung Hantu Penjaga Malam']
  const levelKodam = ['Lemah (baru bangun)','Menengah (mulai mengikuti)','Kuat (setia menjaga)','Sangat Kuat (turun-temurun)','Legendaris (pusaka leluhur)']
  const sifatKodam = ['suka menjaga rezeki','pelindung dari santet','pembuka aura pengasihan','penangkal energi negatif','penambah wibawa','pembawa keberuntungan judi (jangan!)','penjaga saat tidur','pendamping perjalanan jauh']
  const pickK = a => a[Math.floor(Math.random()*a.length)]
  await NXL.sendMessage(m.chat, {
    text: `🔮 *CEK KHODAM*\n\n👤 @${kodamNum}\n\n🐉 Khodam: *${pickK(namaKodam)}*\n⚡ Level: *${pickK(levelKodam)}*\n📜 Sifat: _${pickK(sifatKodam)}_\n\n_⚠️ Hiburan semata, jangan dipercaya._`,
    mentions: kodamTarget ? [kodamTarget] : []
  }, { quoted: m })
}
break

case "cekbeban": case "cekbucin": case "cekfemboy": case "cekgay": case "cekjodoh": case "cekjones": case "cekkaya": case "cekmasadepan": case "ceksange": case "cekstress": case "cekwibu": {

  const _resolveFunJid = (jid) => {
    if (!jid) return null
    const rawNum = jid.replace(/@.*$/, '')
    if (participants && participants.length) {
      if (jid.includes('@lid') || !jid.includes('@s.whatsapp.net')) {
        const found = participants.find(p => (p.lid || p.id || '').replace(/@.*$/, '') === rawNum)
        if (found && found.jid) return found.jid.replace(/@.*$/, '') + '@s.whatsapp.net'
        if (found && found.id && found.id.includes('@s.whatsapp.net')) return found.id
      }
      const found = participants.find(p => (p.jid || p.id || '').replace(/@.*$/, '') === rawNum)
      if (found) return (found.jid || found.id).replace(/@.*$/, '') + '@s.whatsapp.net'
    }
    return rawNum + '@s.whatsapp.net'
  }
  let cekRaw = m.mentionedJid?.[0] || (m.quoted ? m.quoted.sender : null) || (text ? text.replace(/[^0-9]/g,'') + '@s.whatsapp.net' : null) || m.sender
  const cekTargetJid = _resolveFunJid(cekRaw)
  const cekNum = cekTargetJid ? cekTargetJid.split('@')[0] : (text || pushname)
  const cekPersen = Math.floor(Math.random()*101)
  const cekLabels = {cekbeban:'Beban',cekbucin:'Bucin',cekfemboy:'Femboy',cekgay:'Gay',cekjodoh:'Jodoh',cekjones:'Jomblo Ngenes',cekkaya:'Kaya',cekmasadepan:'Masa Depan Cerah',ceksange:'Sange',cekstress:'Stress',cekwibu:'Wibu'}

  const cekKomentar = {
    cekjodoh: [
      ['Yaelah jauh banget, mending lupain aja bro 😭','Ini mah ditolak sebelum nembak udah keliatan 💀','Kalian tuh kayak minyak sama air, gabakal nyatu 😅','Jangankan jodoh, dilirik aja kayaknya enggak deh 🥲','Udah lah jangan dipaksain, sakit tau nungguin yang gabakal balik 😭','Chemistry-nya minus, ini mah skip aja next 👋','Kayaknya doi udah punya gebetan lain deh, sabar ya 😔','Realistis aja bro, ini bukan jalanmu 🙏','Mending fokus duit dulu daripada ngarepin doi 💸','Ditembak pun bakal dijadiin temen doang nih 😬','Jodoh kalian beda server, beda benua, beda galaksi 🌌','Friendzone garansi seumur hidup nih 🤝','Move on gih, masih banyak ikan di laut 🐟','Yah segini doang? Mending nyerah duluan aja 😭','Kalian cocok jadi... ya cuma kenalan aja 😅'],
      ['Masih ada secercah harapan sih, tapi tipis bgt 😅','Bisa diusahain tapi siap-siap mental ya bro 💪','Jalannya terjal, mendaki, berliku, capek 😮‍💨','50:50 tapi kayaknya lebih condong ke gagal deh 🥲','Coba pdkt dulu, siapa tau ada keajaiban ✨','Belum mateng nih, perlu masak lebih lama 🍳','Ada feeling dikit, tapi jangan kege-eran dulu 😏','Lumayan, tapi jangan berharap kebanyakan ya 🙏','Perlu effort ekstra keras nih kalo mau lanjut 😤','Masih abu-abu, belum jelas arahnya kemana 🌫️','Kadang nyambung kadang awkward, gitu deh 😬','Bisa jadi sih, asal jangan modus doang 👀','Pelan-pelan aja, jangan buru-buru nembak 🐢','Ada potensi tapi masih kecil banget bro 🤏','Coba deketin lewat temennya dulu deh 😎'],
      ['Eh lumayan cocok juga ternyata kalian 👀','Boleh nih dilanjut, ada feel-nya 😌','Cukup nyambung kok, tinggal pdkt dikit lagi 💬','Ada potensi gede kalo dijaga bener-bener 🌱','Wah ini mah layak diperjuangin sih 💪','Setengah jalan udah keliatan cocoknya 😎','Chemistry mulai kebangun nih, lanjut bro! 🔥','Tinggal modal nekat aja buat nembak 😤','Doi keliatan ngasih kode juga loh kayaknya 👀','Asik nih, vibesnya udah mulai sama 🎶','Jangan disia-siain, kesempatan langka ini 🍀','Udah 60% nih, dikit lagi gas pol 🚀','Kalian tuh manis juga kalo bareng 🍯','Worth it diperjuangin, jangan ragu 💯','Mulai ada chemistry, pdkt terus jangan kendor 😏'],
      ['Wih cocok banget nih, tinggal dijaga aja 😎','Hampir sempurna sumpah, gas lamar! 🔥','Kalian tuh satu frekuensi parah 📡','Tinggal nunggu momen tepat buat nembak 💍','Ini mah udah pasti jadi kalo serius 💯','Restu tinggal selangkah lagi bro 🙏','Doi kayaknya udah nungguin kamu nembak 😳','Chemistry-nya udah mateng banget nih 🍲','Jangan lama-lama, keburu diembat orang 😤','Pasangan goals nih kalo jadian 💑','Vibesnya udah kayak udah pacaran aja 😏','Tinggal beli cincin sama booking gedung 💒','Ini mah jodoh yang Tuhan udah atur kayaknya ✨','Serius deh, ini sayang banget kalo dilepas 🥺','90% jadi nih, gercep bro! ⚡'],
      ['Buset jodoh dari server sebelah ini mah 🔥','Tinggal nunggu undangan nikah doang 😭','Server udah merestui, ortu tinggal di-iya-in 💍','Soulmate level dewa, ga ada tandingannya 💯','Langsung lamar aja gausah mikir panjang 🚀','Ini mah ditakdirin bareng dari lahir bro ✨','Kalian tuh puzzle yang akhirnya ketemu 🧩','Gaada alasan buat ga jadian, GAS! 🔥','Match made in heaven sumpah 😭💕','Tinggal sebar undangan aja nih 💌','Jodoh sehidup semati level maksimal 💀💕','Ini mah belahan jiwa yang ilang ketemu lagi 🥹','100% perfect, jangan sampe lepas! 💎','Romeo Juliet kalah sama kalian 🌹','Langsung ke KUA aja gausah pacaran lama 😭🔥']
    ],
    cekgay: [
      ['Aman bro, lurus kayak jalan tol 😎','Straight parah, ga ada belok-beloknya 👍','100% cowok sejati, tenang aja 💪','Gaada gelagat sama sekali, sehat mental 😌','Lempeng banget kayak penggaris baja 📏','Normal jaya, lanjutkan hidupmu bro 🫡','Bersih dari virus pelangi, aman 🛡️','Manly maksimal, no debat 😤','Tenang, kamu masih tim cewek kok 😏','Lurus terus sampai jannah bro 🙏','Gaada yang perlu dikhawatirin, clear 👌','Cowok banget nih, ga ada drama 💯','Standar laki normal, santuy aja 😎','Zero indikasi, full maskulin 🦁','Aman sentosa, lanjut gibah cewek 😂'],
      ['Eh ada gelagat dikit nih, hati-hati 🤔','Mencurigakan dikit sih, tapi masih wajar 👀','Hmm agak ragu, tapi belum parah kok 😅','Waspada level rendah, jaga jarak sama temen cowok 😏','Masih bisa diluruskan asal rajin ke gym 💪','Tipis-tipis sih, mungkin cuma salah liat 🤏','Ada sinyal dikit tapi belum confirm 📡','Jangan kebanyakan skinship sama bro lu 😬','Sedikit melenceng, balik ke jalur cepetan 🛣️','Kadang suka muji cowok ya? Hayoo 👀','Belum kemana-mana sih, masih aman 😌','Ati-ati lambai tangan lu mulai gemulai 💅','Indikasi kecil, perbanyak nonton bola deh ⚽','Masih bisa diselamatin nih, ayo bro 🙏','Gelagatnya samar, tapi tetap waspada 🕵️'],
      ['Setengah-setengah nih, bingung sendiri ya? 👀','Galau identitas detected, introspeksi bro 🤔','50:50 parah, plin-plan banget 😅','Wah mulai bimbang nih kayaknya 🌫️','Antara iya dan enggak, abu-abu banget ⚖️','Kadang straight kadang belok, gimana sih 😬','Mulai kegoyah nih pendirianmu bro 😳','Setengah jalan ke pelangi nih kayaknya 🌈','Bingung milih tim ya? Santuy dulu 😏','Sinyalnya campur aduk, ga jelas 📡','Tengah-tengah, bisa kemana aja arahnya 🔀','Hmm, ada potensi belok nih 50% 👀','Jangan-jangan kamu masih nyari jati diri 🤔','Standby mode, belum nentuin pilihan 😅','Ragu-ragu gini malah bahaya loh bro 😬'],
      ['Wih lumayan tinggi loh, mulai keliatan 😳','Gelagatnya makin jelas nih bro 👀','Hampir full color nih hasilnya 🌈','Ada yang perlu kita obrolin nih kayaknya 😏','Tinggi juga ya ternyata, hayoo ngaku 😳','Mulai dominan sisi pelanginya nih 🏳️‍🌈','Bro lu yakin masih tim cewek? 😅','Udah 70% nih, tinggal dikit lagi 😭','Skinship sama temen cowok kebanyakan ya? 👀','Gemulai detected, makin kentara 💅','Wah ini mah udah mulai ga bisa boong 😳','Sinyalnya kuat banget nih ke arah sana 📡','Ngaku aja bro, udah ketauan kok 😏','Hampir confirm nih, dikit lagi 🌈','Mulai susah disangkal nih hasilnya 😭'],
      ['Wanjirr full pelangi detected 🌈','Bro... kita perlu ngobrol serius nih 😳','Udah ga bisa disangkal lagi sumpah 😭','Full color stadium akhir parah 🏳️‍🌈','100% tim sebelah, welcome bro 😂','Pelangi sejati, ga ada obatnya 🌈🔥','Ini mah udah ketauan dari cara jalan 💅','Gas lah ngaku, sekampung udah tau 😭','Maksimal banget hasilnya, juara 🏆🌈','Udah ga usah pura-pura straight lagi 😏','Confirm 100%, selamat datang di klub 🎉','Lambai tangan paling gemulai sekecamatan 💅','Ini mah pelangi turun temurun 🌈👑','Sumpah ga nyangka setinggi ini 😭','Full rainbow, ga ada celah buat boong 🏳️‍🌈🔥']
    ],
    cekbeban: [
      ['Santai, kamu mah berguna banget kok 👍','Mandiri parah, ga ngerepotin siapa-siapa 💪','Malah jadi tumpuan orang banyak nih 🙌','Beban? Boro-boro, kamu tuh aset 💎','Gaada beban-bebannya, malah ngebantu 😎','Orang kayak kamu nih yang dicari-cari 🫡','Berkat ada kamu, semua jadi gampang 🙏','Zero beban, full kontribusi bro 💯','Kamu tuh solusi, bukan masalah 😌','Diandelin terus nih sama orang-orang 💪','Tipe orang yang bikin tim makin solid 🤝','Ga pernah nyusahin, malah disayang 🥰','Mandiri sejak lahir kayaknya nih 😎','Bikin bangga keluarga nih kamu 👏','Berguna di mana-mana, mantap! 🔥'],
      ['Ngerepotin dikit sih kadang 😅','Suka minta tolong mulu ya hayoo 👀','Beban tipis-tipis lah, masih wajar 🤏','Kadang nyusahin tapi masih bisa ditolerir 😌','Ada lah dikit, tapi ga parah kok 🙂','Sesekali jadi beban, tapi ga sering 😬','Masih dalam batas wajar sih bebannya 👍','Minta tolong boleh, asal jangan keseringan 😏','Lumayan ngerepotin pas lagi butuh doang 😅','Beban level pemula, masih bisa diapus 🧹','Kadang bikin repot tapi tau diri kok 🙂','Ga gede-gede amat sih bebannya 🤷','Sesekali doang jadi beban, santuy 😌','Tipis kok, masih bisa diandelin juga 👍','Beban part-time nih kayaknya 😅'],
      ['Lumayan jadi beban juga nih 🤔','Setengah mandiri setengah nyusahin 😬','Mulai sering ngerepotin orang nih 👀','50% beban keluarga, hayoo perbaiki 😅','Belajar mandiri dong, jangan ngandelin terus 😤','Mulai berat nih bebanmu buat sekitar 😩','Standar beban menengah, lumayan 🤷','Kadang bikin orang capek juga sih 😮‍💨','Ga mandiri-mandiri amat nih kamu 😬','Mulai bikin pusing orang sekitar 🤕','Beban naik level nih, awas 📈','Setengah jalan jadi benalu nih 🌿','Mulai ngerepotin tapi belum parah 😅','Perlu intropeksi nih, kebanyakan minta tolong 🙏','50:50 antara berguna sama nyusahin ⚖️'],
      ['Beban banget nih kamu, capek orang 😭','Sering nyusahin sekitar, sadar dong 😩','Tinggi kadar bebannya, aduh 📈','Mulai bikin capek semua orang 😮‍💨','Hampir full beban nih, perbaiki bro 😭','Numpang hidup mulu nih kayaknya 😬','Bikin pusing satu rumah nih 🤕','Beban level tinggi, mandiri dikit napa 😤','Orang-orang mulai ngehindarin nih 🏃','Hidupmu nyusahin banyak pihak 😭','Beban berat sebelah nih, aduh 😩','Tinggi banget, tolong introspeksi 🙏','Bikin orang ngelus dada terus 😮‍💨','Hampir juara beban nih kamu 🥈','Berat nih beban yang kamu kasih 😭'],
      ['Beban sejati level dewa parah 💀','Hidup numpang terus, kapan mandirinya 😭','Beban keluarga, RT, RW, sekecamatan 🔥','Pure beban tanpa kompromi, juara 🏆','Juara umum beban nasional nih 🥇','Bikin capek sedunia, rekor baru 😭','Beban abadi, ga ada obatnya 💀','Numpang hidup level maksimal 😩','Sekampung kewalahan ngurus kamu 🔥','Beban terberat sepanjang sejarah 📜','Tolong banget mandiri, kasian sekitar 🙏😭','Beban warisan turun temurun nih 👑','100% benalu, ga ada celah berguna 🌿💀','Bikin satu negara ngelus dada 😮‍💨','Maksimal beban, selamat juara 1 🏆😭']
    ],
    cekbucin: [
      ['Santuy, kamu masih waras kok 😎','Kepala dingin, hati stabil, mantap 🧊','Aman dari virus bucin, sehat 💪','Cinta secukupnya, ga lebay 👍','Gaada bucin-bucinnya, keren 😌','Masih bisa mikir pake logika nih 🧠','Cinta boleh, harga diri jalan terus 🫡','Zero bucin, full self respect 💯','Doi chat? Santai aja dibales 😏','Ga ngemis perhatian, salut 👏','Hidup ga cuma soal doi, bagus 🌟','Waras maksimal, ga gampang baper 😎','Cinta sehat, ga toxic, mantul 🙌','Masih punya prinsip nih kamu 💪','Adem, ga gampang dimainin perasaan 🧊'],
      ['Bucin tipis-tipis nih mulai 😅','Kadang kangen dikit sih ngaku 👀','Masih bisa dikontrol kok bucinnya 🙂','Bucin level pemula, masih aman 🐣','Ada gejala ringan nih, hati-hati 😬','Mulai sering ngecek chat doi ya? 📱','Tipis sih, tapi awas nambah 🤏','Kadang senyum sendiri liat chat 😊','Bucin part-time, belum parah 😅','Mulai baper dikit-dikit nih 🥺','Masih wajar kok, santuy aja 😌','Sesekali kepikiran doi, normal 🙂','Bucin level 1, lanjut waspada 😏','Ada rasa tapi masih bisa nahan 💪','Tipis-tipis kayak tisu nih 🧻'],
      ['Lumayan bucin juga nih ternyata 👀','Setengah waras setengah bucin 😬','Mulai kepikiran doi terus nih 🥺','50% otak isinya doi doang 🧠💕','Chat dibales langsung senyum lebar 😊','Bucin menengah, mulai keliatan 😏','Sering banget ngecek WA doi ya 📱','Mulai rela-relain demi doi nih 😅','Setengah jalan jadi bucin sejati 💕','Perhatian ke doi mulai berlebih 👀','Bucin naik level nih, awas 📈','Mulai posesif dikit ya hayoo 😏','50:50 antara waras sama gila cinta ⚖️','Doi nomor satu, yang lain nomor sekian 🥺','Mulai susah fokus gara-gara doi 😵‍💫'],
      ['Bucin parah nih, sadar woy 😭','Hidup buat doi doang ya kamu 🥺','Tinggi kadar bucinnya, aduh 📈','Chat doi dibales 0.1 detik nih 📱💨','Hampir full bucin, bahaya 😩','Rela begadang demi nungguin chat 😴','Doi batuk, kamu yang panik 😭','Bucin level tinggi, susah diobatin 💊','Udah ga bisa hidup tanpa doi nih 🥺','Posesif maksimal nih kayaknya 👀','Sehari ga chat doi langsung lemes 😫','Bucin berat, mikir logika udah ilang 🧠❌','Demi doi rela ngapain aja nih 😭','Hampir juara bucin, dikit lagi 🥈','Cinta buta level lanjut nih 🙈'],
      ['Bucin akut stadium 4 parah 💀','Tiap detik mikirin doi, gila 😭','Bucin sejati tanpa obat, juara 🏆','Rela jual ginjal demi doi nih 😭🔥','Raja/Ratu bucin sedunia, selamat 👑','Doi napas, kamu yang takjub 😍','Hidup mati cuma buat doi nih 💀💕','Bucin maksimal, logika udah RIP 🪦','Sehari ga liat doi langsung sakaw 😵','Cinta buta total, ga ada obatnya 🙈🔥','Bucin warisan turun temurun nih 👑','100% budak cinta, ga ada sisa 😭','Demi doi rela ngelakuin hal gila 💀','Juara 1 bucin nasional, prokk 👏😭','Sumpah ini bucin terparah sepanjang masa 🏆💕']
    ],
    cekkaya: [
      ['Dompet lagi kering kerontang nih 😭','Saldo tinggal recehan buat parkir 🪙','Akhir bulan vibes banget nih 📉','Kaya hati tapi miskin harta 🥲','Lagi prihatin ya bro, sabar 🙏','Rekening nangis liat saldonya 😭','Makan mie tiap hari nih kayaknya 🍜','Mau jajan aja mikir 1000x dulu 😔','Dompet setipis kartu ATM 💳','Lagi mode survival nih hidupnya 🥲','Kantong bolong terus nih 🕳️','Bokek maksimal, sabar ya bro 😭','Sultan? Boro-boro, recehan aja susah 🪙','Lagi puasa jajan nih kayaknya 😅','Ekonomi lagi resesi parah nih 📉'],
      ['Cukup buat makan aja sih 😅','Pas-pasan lah, ga lebih ga kurang 🤷','Ga kaya ga miskin, standar UMR 💼','Lumayan buat jajan secukupnya 🙂','Ekonomi stabil tapi ga mewah 😌','Bisa hidup layak lah, cukup 👍','Standar rakyat menengah nih 🏠','Bisa nabung dikit-dikit kok 🐷','Hidup hemat tapi aman 😎','Cukup buat kebutuhan sehari-hari 🙂','Ga kekurangan, ga kelebihan juga 🤷','Dompet aman level standar 💳','Bisa ngopi sesekali lah ☕','Ekonomi sehat tapi biasa aja 😌','Lumayan, ga sampe ngutang kok 👍'],
      ['Lumayan berada juga nih kamu 👀','Sultan menengah detected 😎','Dompet aman terkendali nih 💳','Bisa traktir temen sesekali 🍔','Ekonomi mapan nih kayaknya 💼','Mulai keliatan tajirnya nih 👀','Rekening lumayan tebal nih 💰','Bisa beli yang dimau tanpa mikir lama 🛍️','Hidup nyaman level menengah atas 😌','Setengah jalan ke crazy rich nih 📈','Lumayan loh, bisa healing tiap bulan ✈️','Dompet sehat walafiat 💪','Mulai bisa flexing dikit nih 😏','Ekonomi naik kelas nih kayaknya 📈','Berkecukupan banget nih hidupnya 🙌'],
      ['Tajir juga nih ternyata kamu 😎','Sultan mulai keliatan jelas nih 👑','Rekening tebal parah nih 💰','Bisa beli apa aja yang dimau 🛍️','Hampir crazy rich nih kayaknya 📈','Flexing terus nih kayaknya bisa 😏','Hidup mewah level tinggi 🏖️','Dompet ga ada matinya nih 💳','Healing ke luar negeri mah santuy ✈️','Tinggal dikit lagi jadi konglomerat 💎','Tajir melintir nih kamu 🤑','Beli gadget baru tiap bulan santuy 📱','Sultan beneran nih kayaknya 👑','Duit ngalir terus kayaknya nih 💸','Hampir tembus crazy rich nih 🚀'],
      ['Sultan sejati level dewa parah 🔥','Crazy rich Asia detected 💰','Uang bukan masalah buat lu bro 😎','Beli pulau aja bisa nih kayaknya 🏝️','Konglomerat tanpa tanding, juara 👑','Duit segunung, ga abis 7 turunan 💎','Tajir maksimal, sekelas Sultan 🤑','Mau beli apa aja tinggal gesek 💳🔥','Rekening ga ada batasnya nih 💰','Crazy rich beneran nih sumpah 🚀','Hartanya ga keitung saking banyaknya 💎','Sultan akhir zaman detected 👑🔥','Beli negara aja bisa kayaknya 🌍','Juara 1 orang terkaya grup ini 🏆','Tajir level dewa, ga ada lawan 💸👑']
    ],
    cekjones: [
      ['Laku banget kok, gausah khawatir 😎','Banyak yang naksir nih diam-diam 👀','Jauh dari kata jomblo, tenang 💯','PDKT mulu tiap minggu nih 🔥','Laris manis kayak gorengan 🍤','Antrian gebetan panjang nih 📋','Ga bakal jomblo lama-lama kamu 😌','Pesona kamu ga ketolong, mantap ✨','Banyak yang ngantri jadi pacar 🙌','Tinggal pilih aja mau yang mana 😏','Charming parah, wajar laku 😎','Gebetan dimana-mana nih 🌹','Ga pernah sepi dari yang naksir 💕','Tipe yang diperebutin nih 🔥','Aman dari status jomblo, santuy 👍'],
      ['Jones tipis-tipis nih 😅','Kadang sepi kadang ada yang deketin 🤷','Masih ada lah yang naksir dikit 👀','Jomblo sementara aja kok 🙂','Belum parah kok jonesnya 😌','Sesekali ada yang chat duluan 📱','Ada harapan, jangan nyerah 💪','Masih ada yang ngelirik kok 👀','Jones level pemula, santuy 🐣','Kadang dapet kode dikit nih 😏','Belum sepi-sepi amat sih 🙂','Ada yang modus tapi masih malu 😅','Jomblo tapi masih ada peminat 👍','Tipis-tipis nih jonesnya 🤏','Masih ada secercah harapan cinta ✨'],
      ['Lumayan jones juga nih 👀','Setengah laku setengah sepi 😬','Mulai jarang ada yang deketin 😅','50% peluang dapet gebetan 🤷','Jones menengah nih kayaknya 📊','Chat masuk cuma pas ada perlu 📱','Mulai sepi nih dunia percintaan 🌫️','Setengah jalan jadi jomblo abadi 😬','Yang naksir mulai langka nih 🔍','Mulai keliatan jonesnya nih 👀','50:50 antara laku sama sepi ⚖️','Mulai jarang di-notice nih 😔','Jones naik level, awas 📈','Gebetan? Adanya cuma di mimpi 😴','Mulai susah dapet perhatian nih 😬'],
      ['Jones parah nih, sabar ya 😭','Udah lama ga ada yang naksir 😩','Tinggi kadar jonesnya nih 📈','Notif chat cuma dari ojol 🛵','Hampir jones abadi nih 😭','Dunia percintaan sepi total 🌑','Gebetan terakhir kapan ya? 🤔','Jones level tinggi, semangat! 💪','Yang naksir entah kemana semua 🔍','Hampir resmi jadi jomblo ngenes 😭','Sepi banget nih asmara kamu 🦗','Tinggi banget jonesnya, sabar 🙏','Chat doi? Doi siapa? Ga ada 😭','Hampir juara jomblo nih 🥈','Jones berat sebelah nih 😩'],
      ['Jones abadi level dewa parah 💀','Jomblo sejak dalam kandungan 😭','Jones tanpa harapan, sabar bro 🙏','Kucing aja ninggalin lu nih 🐱😭','Raja/Ratu jomblo sedunia 👑','Jomblo akut stadium akhir 💀','Ga ada yang naksir 7 turunan 😭','Jones maksimal, ga ada obatnya 💊','Asmara sepi kayak kuburan 🪦','Jomblo abadi warisan leluhur 👑😭','100% sendiri, ga ada peminat 🦗','Notif WA cuma dari operator 📱😭','Jones terparah sepanjang sejarah 📜','Juara 1 jomblo nasional, prokk 🏆😭','Sumpah ini jones terabadi sedunia 💀👑']
    ],
    cekstress: [
      ['Santai banget hidupnya, adem 😎','Pikiran tenang kayak air danau 🧘','Zero stress, hidup damai 😌','Hidup tanpa beban nih kayaknya 🌴','Tenang maksimal, bahagia terus 😊','Vibesnya chill abis nih 🌊','Pikiran jernih, hati lapang 🙏','Ga ada drama, hidup mulus 😎','Healing tiap hari kayaknya nih 🌸','Bahagia level maksimal nih 😄','Ga gampang panik, mantap 💪','Hidup slow living nih kayaknya 🍃','Adem ayem, ga ada gangguan 😌','Pikiran ringan kayak kapas ☁️','Tenang sentosa, bahagia selalu 🌈'],
      ['Stress tipis-tipis nih 😅','Kadang pusing dikit aja 🤕','Masih bisa rileks kok 😌','Stress ringan, masih aman 🙂','Ada tekanan kecil tapi wajar 😬','Sesekali overthinking dikit 🤔','Masih bisa dikontrol kok 💪','Pusing level pemula nih 😅','Kadang kepikiran tapi cepet ilang 🙂','Stress musiman aja nih 🍂','Belum parah kok, santuy 😌','Ada beban dikit tapi enteng 🤏','Pikiran agak penuh tapi oke 🙂','Stress tipis kayak tisu 🧻','Masih bisa ketawa kok 😄'],
      ['Lumayan stress juga nih 🤔','Setengah waras setengah pusing 😬','Mulai overthinking banyak hal 😵‍💫','50% beban pikiran nih 🧠','Stress menengah, hati-hati 😮‍💨','Mulai susah tidur ya kayaknya 😴','Pikiran mulai penuh nih 🌫️','Setengah jalan ke burnout 😬','Mulai gampang kesel nih 😤','Butuh healing dikit nih kayaknya 🌸','Stress naik level, awas 📈','Mulai capek mental nih 😮‍💨','50:50 antara waras sama mumet ⚖️','Pikiran mulai kemana-mana 🌀','Mulai butuh me time nih 🧘'],
      ['Stress parah nih, healing dong 😭','Pikiran kemana-mana ga karuan 🌀','Tinggi kadar stressnya nih 📈','Butuh healing ASAP nih 🌸','Hampir burnout nih, istirahat 😩','Susah tidur mikirin banyak hal 😴','Mental mulai lelah banget 😮‍💨','Stress level tinggi, take a break 🛑','Kepala penuh, butuh refreshing 🤯','Hampir tumbang nih mentalnya 😭','Overthinking parah nih kayaknya 🌀','Pikiran overload, kurangi beban 🧠','Butuh liburan urgent nih 🏖️','Hampir di ujung kesabaran nih 😩','Stress berat, jaga kesehatan ya 🙏'],
      ['Stress level dewa, healing SEKARANG! 💀','Pikiran udah mau meledak nih 🤯','Stress akut butuh liburan panjang 🏖️🔥','Otak overload total, bahaya 🧠💥','Mental health emergency nih 🚨','Burnout maksimal, stop dulu woy 🛑','Kepala udah kayak mau pecah 🤯','Stress terparah, tolong istirahat 🙏😭','Pikiran chaos total nih 🌀💀','Udah di batas maksimal nih mentalnya 😭','Tolong healing sebelum ambruk 🏥','Stress level akhir zaman 💀','Otak butuh restart total nih 🔄','Juara 1 paling stress nih, sabar 🏆😭','Mental darurat, please rehat dulu 🚨🙏']
    ],
    ceksange: [
      ['Adem banget, pikiran bersih 😇','Suci kayak air zamzam nih 💧','Zero gairah, iman kuat 🙏','Pikiran jernih banget nih 😌','Imannya mantap, salut 🫡','Bersih dari pikiran kotor 😇','Adem ayem, hati tenang 🧘','Ga ada pikiran aneh-aneh 👍','Suci maksimal nih kayaknya 😇','Iman tebal, pikiran lurus 🙏','Pikiran sebening embun pagi 💧','Aman dari godaan nih 🛡️','Hati bersih, niat lurus 😌','Ga kepikiran yang macem-macem 👍','Suci level malaikat nih 😇'],
      ['Sange tipis-tipis nih 😅','Kadang khilaf dikit aja 👀','Masih bisa nahan kok 💪','Level rendah, masih aman 🙂','Ada gejala ringan nih 😬','Sesekali kepikiran tapi cepet istighfar 🙏','Masih bisa dikontrol kok 😌','Khilaf pemula nih 🐣','Kadang liar dikit tapi nahan 😅','Tipis-tipis, masih wajar 🤏','Belum parah kok, santuy 😌','Sesekali aja munculnya 😬','Masih bisa nunduk-nunduk 🙏','Gejala ringan, perbanyak dzikir 📿','Tipis kayak tisu nih 🧻'],
      ['Lumayan sange juga nih 👀','Setengah suci setengah khilaf 😬','Mulai ga fokus nih kayaknya 😵‍💫','50% pikiran kemana-mana 🌀','Sange menengah, istighfar dong 🙏','Mulai sering kepikiran nih 👀','Pikiran mulai liar setengah 😬','Setengah jalan ke khilaf total 😅','Mulai susah fokus ibadah 😔','Butuh dzikir lebih banyak nih 📿','Sange naik level, awas 📈','Mulai goyah imannya nih 😬','50:50 antara suci sama khilaf ⚖️','Pikiran mulai ga karuan 🌀','Mulai butuh wudhu nih 💧'],
      ['Sange parah nih, istighfar 😳','Pikiran mulai liar banget 🌀','Tinggi kadarnya nih, astaghfirullah 😭','Butuh istighfar banyak nih 🙏','Hampir khilaf total nih 😩','Pikiran ga karuan parah nih 😵‍💫','Iman mulai goyah berat nih 😬','Sange level tinggi, wudhu sana 💧','Kepala penuh pikiran liar 🌀','Hampir lepas kontrol nih 😭','Astaghfirullah tinggi banget 🙏','Pikiran kotor mendominasi nih 😩','Butuh siraman rohani urgent 🕌','Hampir juara khilaf nih 🥈','Sange berat, perbanyak puasa 🙏'],
      ['Sange level dewa, ISTIGHFAR woy! 💀','Pikiran udah ga karuan total 🌀😭','Sange akut butuh wudhu segalon 💧🔥','Astaghfirullah parah banget nih 🙏💀','Juara umum khilaf nasional 🏆😭','Iman tinggal seuprit nih, bahaya 😭','Pikiran liar maksimal, tobat woy 🙏','Sange stadium akhir, puasa sana 💀','Kepala isinya yang gitu-gitu doang 🌀','Hampir lepas total nih kontrolnya 😭','Tolong banyak-banyak dzikir nih 📿😭','Khilaf level akhir zaman 💀','Pikiran kotor juara satu nih 🏆','Astaghfirullahaladzim parah amat 🙏😭','Sange terparah sepanjang masa, tobat! 💀🔥']
    ],
    cekmasadepan: [
      ['Masa depan masih buram nih 😅','Perlu kerja keras lagi nih bro 💪','Belum keliatan arahnya nih 🌫️','Banyak PR ke depan nih 📚','Jalan masih panjang, semangat 🙏','Mulai dari nol lagi nih kayaknya 😬','Perlu effort lebih keras nih 😤','Masih abu-abu nih nasibnya 🌫️','Belum jelas mau kemana arahnya 🤔','Butuh usaha ekstra banget nih 💪','Masa depan masih misteri nih 🔮','Sabar, semua butuh proses bro 🙏','Belum nemu jalan yang pas nih 🛤️','Perlu lebih fokus nih kayaknya 🎯','Masih jauh dari kata sukses 😅'],
      ['Ada secercah harapan nih 🤔','Mulai keliatan dikit arahnya 👀','Perlu usaha ekstra tapi mungkin 💪','Lumayan ada arah nih kayaknya 🛤️','50:50 nasibnya nih 🤷','Mulai nemu jalan dikit-dikit 🌱','Ada potensi tapi harus kerja keras 😤','Setengah jalan menuju cerah 🌅','Mulai ada titik terang nih ✨','Harapan mulai muncul nih 🌱','Belum pasti tapi ada peluang 🎲','Lumayan, jangan nyerah dulu 💪','Mulai keliatan progressnya 📈','Ada kemungkinan sukses nih 🙂','Perlu konsisten dikit lagi 🔁'],
      ['Lumayan cerah kok masa depannya 👀','Prospek bagus nih kayaknya 📈','Ada potensi sukses gede nih 🌟','Jalan mulai kebuka lebar nih 🛤️','Cukup menjanjikan nih kayaknya 😌','Mulai keliatan terang nih 🌅','Setengah jalan menuju sukses 💪','Ada bakat sukses nih kayaknya ✨','Prospek karir oke nih 💼','Lumayan, tinggal konsisten 🔁','Masa depan mulai bersinar nih ✨','Ada peluang gede menanti 🚪','Cukup cerah, lanjutkan bro 😎','Mulai ke arah yang bener nih 🎯','Potensi gede nih, jangan kendor 💪'],
      ['Cerah banget masa depannya 😎','Sukses udah di depan mata nih 👀','Tinggal konsisten aja bro 🔁','Prospek gemilang nih kayaknya ✨','Hampir pasti sukses nih 💯','Karir bakal melejit nih 🚀','Masa depan cerah membentang 🌅','Tinggal selangkah menuju sukses 👣','Bakal jadi orang nih kayaknya 💼','Prospek cerah, gas terus 🔥','Sukses tinggal nunggu waktu ⏰','Hampir di puncak nih kayaknya 🏔️','Masa depan terang benderang ☀️','Tinggal eksekusi aja nih 🎯','Sukses udah ngantri nih buat kamu 📋'],
      ['Masa depan gemilang level dewa 🔥','Sukses dunia akhirat nih 💯','Calon orang besar nih kamu 😎','Bersinar terang masa depannya ✨','Crazy rich masa depan detected 👑','Bakal jadi sultan nih kayaknya 💰','Masa depan cerah maksimal 🌟','Sukses level dewa menanti 🚀','Calon CEO nih kayaknya 💼👑','Masa depan secerah matahari ☀️🔥','Bakal sukses besar-besaran nih 💎','Juara 1 masa depan cerah 🏆','Gemilang tanpa tanding nih ✨','Calon orang sukses sejati 👑','Masa depan paling cerah sedunia 🌟🔥']
    ],
    cekwibu: [
      ['Normal kok, gak wibu 😎','Belum kena virus anime nih 🛡️','Aman dari waifu, santuy 👍','Hidup di dunia nyata nih 🌍','Zero wibu, sehat mental 😌','Belum keracunan anime nih 💉','Masih waras dari otaku 🧠','Ga ngoleksi figure nih kayaknya 👍','Hidup normal tanpa waifu 😎','Aman dari budaya wibu 🛡️','Belum nonton anime ya kayaknya 📺','Masih tim dunia nyata nih 🌍','Ga ngomong bahasa Jepang nih 🇯🇵❌','Normal jaya, lanjutkan 🫡','Bersih dari virus weeb 😌'],
      ['Wibu tipis-tipis nih 😅','Nonton anime kadang-kadang 📺','Masih casual kok nontonnya 🙂','Wibu level pemula nih 🐣','Baru mulai keracunan dikit 💉','Sesekali nonton anime mainstream 😅','Belum parah kok wibunya 🙂','Tau Naruto sama One Piece doang 🍥','Wibu musiman aja nih 🍂','Masih tahap awal nih 🌱','Kadang nonton tapi belum gila 😌','Tipis-tipis nih wibunya 🤏','Baru kenal anime populer 📺','Belum punya waifu tetap 😅','Wibu pemula, masih bisa balik 🔙'],
      ['Lumayan wibu juga nih 👀','Setengah normal setengah wibu 😬','Mulai punya waifu nih kayaknya 💕','50% otak isi anime 🧠','Wibu menengah nih kayaknya 📊','Mulai ngoleksi nih kayaknya 🖼️','Setengah jalan jadi otaku 😬','Mulai paham istilah Jepang 🇯🇵','Nonton anime tiap musim nih 📺','Wibu naik level, awas 📈','Mulai ngidol seiyuu nih 🎤','50:50 antara normal sama weeb ⚖️','Mulai sebut waifu di chat 💕','Anime knowledge lumayan nih 📚','Mulai keliatan wibunya nih 👀'],
      ['Wibu parah nih kayaknya 😭','Kamar penuh poster anime 🖼️','Tinggi kadar wibunya nih 📈','Ngomong campur bahasa Jepang nih 🇯🇵','Hampir full wibu nih 😩','Waifu lebih penting dari makan 💕','Koleksi figure udah banyak nih 🗿','Wibu level tinggi nih kayaknya 📊','Nonton anime > kehidupan sosial 📺','Hampir jadi otaku sejati nih 😭','Budget jajan buat merch anime 💸','Wibu berat nih kayaknya 😩','Hampir lupa dunia nyata nih 🌍❌','Sebut "kawaii" tiap 5 menit 😆','Hampir juara wibu nih 🥈'],
      ['Wibu sejati level dewa 🔥','Waifu lebih nyata dari pacar 😭','Wibu akut stadium akhir 🌸💀','Nihongo jouzu desu ne~ 🇯🇵','Raja/Ratu wibu sedunia 👑','Otaku sejati, ga ada obatnya 💀','Kamar udah kayak Akihabara 🏯','Wibu maksimal, hidup buat anime 📺🔥','Waifu udah dianggep istri nih 💍😭','Koleksi figure satu gudang 🗿👑','Wibu turun temurun nih 👑','Hidup, mati, napas buat anime 💀🌸','Juara 1 wibu nasional, prokk 🏆','Otaku level akhir zaman 💀','Wibu terparah sepanjang masa, sugoi! 🌸🔥']
    ],
    cekfemboy: [
      ['Manly banget, gaada gejala 😎','Cowok sejati nih, mantap 💪','Maskulin level tinggi nih 🦁','Aman terkendali bro 👍','Zero femboy, full laki 😤','Macho parah nih kayaknya 💪','Gaada sisi gemulai sama sekali 😎','Laki tulen nih, no debat 🫡','Maskulin maksimal nih 🦁','Bersih dari aura uwu 🛡️','Cowok banget, ga ada drama 😎','Manly sejak lahir kayaknya 💪','Ga ada lembut-lembutnya nih 😤','Full testosteron nih kayaknya 🦁','Aman dari status femboy 👍'],
      ['Femboy tipis-tipis nih 😅','Ada sisi lembut dikit aja 👀','Masih maskulin kok mostly 💪','Level rendah, masih aman 🙂','Gejala ringan nih kayaknya 😬','Sesekali keliatan lembut dikit 😅','Belum parah kok femboynya 🙂','Manly tapi kadang uwu dikit 😏','Tipis-tipis sih sisinya 🤏','Masih dominan cowoknya 💪','Gejala awal nih kayaknya 🌱','Kadang gemulai dikit aja 💅','Belum kemana-mana kok 😌','Femboy pemula nih 🐣','Tipis kayak tisu nih 🧻'],
      ['Lumayan femboy juga nih 👀','Setengah manly setengah lembut 😬','Mulai keliatan sisi uwunya 💅','50:50 hasilnya nih 🤷','Femboy menengah nih kayaknya 📊','Mulai dominan sisi lembutnya 👀','Setengah jalan jadi femboy 😬','Mulai gemulai nih kayaknya 💅','Aura uwu mulai muncul nih 🌸','50% manly 50% cantik ⚖️','Mulai keliatan nih sisinya 👀','Femboy naik level, awas 📈','Setengah cowok setengah uwu 🌸','Mulai sering dipuji cantik nih 😳','Lembutnya mulai dominan nih 💅'],
      ['Femboy tinggi nih kayaknya 😳','Sisi lembut mulai dominan banget 💅','Tinggi juga hasilnya nih 📈','Hampir full uwu nih 🌸','Mulai dominan sifat lembutnya 😳','Lebih cantik dari cewek nih 💅','Aura uwu kuat banget nih 🌸','Femboy level tinggi nih 📊','Hampir full femboy nih 😳','Mulai susah disangkal nih 💅','Sisi cantiknya mendominasi 🌸','Uwu detected makin kentara 😳','Hampir juara femboy nih 🥈','Lembut maksimal nih kayaknya 💅','Tinggi banget aura femboynya 🌸'],
      ['Femboy level dewa parah 🔥','Lebih cantik dari cewek beneran 😳','Femboy sejati detected 🌸💯','Uwu maksimal level dewa 😭','Juara umum femboy nasional 👑','Cantiknya ngalahin model nih 💅🔥','Femboy stadium akhir nih 🌸','Aura uwu tak tertandingi 👑','100% femboy sejati nih 💅','Hampir ga ada sisi cowoknya 🌸😭','Femboy paling cantik sedunia 👑','Uwu turun temurun nih 🌸','Juara 1 femboy nasional, prokk 🏆','Cantik level dewa, ga ada lawan 💅👑','Femboy terparah sepanjang masa, uwu! 🌸🔥']
    ]
  }
  const tierIdx = Math.min(4, Math.floor(cekPersen / 21))
  const komenArr = cekKomentar[command] || [['Hasil keluar nih.'],['Lumayan.'],['Standar aja.'],['Lumayan tinggi.'],['Maksimal!']]
  const komentar = (komenArr[tierIdx] || komenArr[komenArr.length-1])[Math.floor(Math.random() * (komenArr[tierIdx]||komenArr[komenArr.length-1]).length)]
  await NXL.sendMessage(m.chat, {
    text: `🔍 *Cek ${cekLabels[command]||command}*\n\n👤 @${cekNum}\n📊 Hasil: *${cekPersen}%*\n\n💬 ${komentar}`,
    mentions: cekTargetJid ? [cekTargetJid] : []
  }, { quoted: m })
}
break

case "kecocokanpasangan": {
  if (!text) return m.reply(`*Contoh:* ${command} nama1 & nama2`)
  const cocokNames = text.split(/[&,]/).map(n=>n.trim()).filter(Boolean)
  if (cocokNames.length<2) return m.reply(`*Contoh:* ${command} Andi & Siti`)
  const cocokPersen = Math.floor(Math.random()*101)
  const cocokTier = [
    ['Aduh kayaknya beda dunia deh kalian 😅','Cuma sebatas kenal aja nih kayaknya 🥲','Belum ada chemistry sama sekali nih 😬','Mending temenan aja deh, jangan dipaksa 🤝','Susah kalau dipaksain, sakit nanti 😭','Ini mah kayak minyak sama air, gabakal nyatu 💧','Beda frekuensi parah nih kalian 📡','Jangan dipaksain, ntar nyesel loh 🙏','Kayaknya salah satu udah punya yang lain deh 😔','Realistis aja, ini bukan jalan kalian 😅','Cocok jadi musuh bebuyutan kayaknya 😂','Mending cari yang lain aja deh 👋','Ga nyambung blas nih kalian 😬','Dipaksain pun bakal kandas di tengah jalan 🚧','Skip aja next, masih banyak yang lain 🐟'],
    ['Masih bisa diusahain sih, tapi berat 🤔','Ada harapan kecil nih, tipis bgt 🤏','Perlu pdkt lebih dalam dulu nih 💬','Lumayan, tapi jangan kege-eran dulu 😏','50:50 condong ke ragu nih ⚖️','Bisa jadi asal usaha keras 💪','Belum mateng nih, masak lagi 🍳','Ada feel dikit tapi masih samar 🌫️','Coba deketin lebih intens dulu 👀','Perlu effort ekstra nih kalo serius 😤','Masih abu-abu, belum jelas arahnya 🌫️','Bisa diperjuangin tapi siap mental ya 💪','Kadang nyambung kadang awkward 😬','Pelan-pelan aja, jangan buru-buru 🐢','Ada potensi kecil, jangan nyerah 🌱'],
    ['Eh lumayan cocok juga ternyata 👀','Ada potensi bagus nih kalian 🌱','Boleh lah dilanjut, ada feel-nya 😌','Cukup nyambung kok, lanjut pdkt 💬','Tinggal usaha dikit lagi nih 💪','Chemistry mulai kebangun nih 🔥','Setengah jalan udah cocok nih 😎','Worth it diperjuangin nih 💯','Vibesnya mulai sama nih kalian 🎶','Asik nih, ada harapan gede 🍀','Mulai keliatan cocoknya nih 😏','Jangan disia-siain, lumayan loh 👀','Udah 60% nih, gas terus 🚀','Manis juga kalo bareng kalian 🍯','Lanjut bro, jangan kendor pdkt-nya 💪'],
    ['Wih cocok banget nih kalian 😎','Hampir sempurna sumpah pasangannya 🔥','Tinggal dijaga aja nih hubungannya 💕','Kalian satu frekuensi parah 📡','Restu tinggal selangkah lagi 🙏','Chemistry udah mateng banget nih 🍲','Tinggal nunggu momen nembak 💍','Pasangan goals nih kalo jadian 💑','Jangan lama-lama, keburu diembat 😤','Vibesnya kayak udah pacaran 😏','90% jadi nih, gercep! ⚡','Serius deh, sayang kalo dilepas 🥺','Cocok abis, tinggal eksekusi 🎯','Tinggal beli cincin nih kayaknya 💍','Hampir pasti jadi nih kalian 💯'],
    ['Cocok banget kalian, gas nikah 🔥','Tinggal nunggu undangan doang 😭','Server udah merestui nih 😎','Soulmate level dewa, juara 💯','Langsung lamar aja gausah mikir 🚀','Match made in heaven sumpah 😭💕','Jodoh dari lahir nih kayaknya ✨','Gaada alasan ga jadian, GAS! 🔥','Tinggal sebar undangan aja nih 💌','Belahan jiwa yang ketemu lagi 🥹','100% perfect, jangan dilepas 💎','Romeo Juliet kalah sama kalian 🌹','Langsung ke KUA aja nih 😭🔥','Puzzle yang akhirnya nyatu 🧩','Cinta sehidup semati level maksimal 💕👑']
  ]
  const cocokIdx = Math.min(4, Math.floor(cocokPersen / 21))
  const cocokKomen = cocokTier[cocokIdx][Math.floor(Math.random()*cocokTier[cocokIdx].length)]
  m.reply(`💕 *Kecocokan Pasangan*\n\n👤 ${cocokNames[0]} 💘 ${cocokNames[1]}\n📊 Kecocokan: *${cocokPersen}%*\n\n💬 ${cocokKomen}`)
}
break

case "faktadunia": case "faktaunik": {
  const faktaArr = ['Madu tidak pernah basi.','Sidik jari koala identik dengan manusia.','Otak manusia 20% oksigen tubuh.','Lumba-lumba tidur satu mata terbuka.','Jantung paus biru sebesar mobil.','DNA manusia 60% sama dengan pisang.','Rusia lebih luas dari Pluto.','Kecoak hidup tanpa kepala seminggu.','Bintang laut tidak punya otak.','Kucing tidur 70% hidupnya.','Petir 5x lebih panas dari matahari.','Bayi 300 tulang, dewasa 206.']
  m.reply(`🌍 *${command === 'faktadunia' ? 'Fakta Dunia' : 'Fakta Unik'}*\n\n${faktaArr[Math.floor(Math.random()*faktaArr.length)]}`)
}
break



case "jumlahuser": { m.reply(`👥 *Total User:* ${Object.keys(global.db?.users||{}).length}`) }
break

case "meme": {
  try { const r=await axios.get(`${global.apiMemeApi}/gimme`); if(r.data?.url) await NXL.sendMessage(m.chat,{image:{url:r.data.url},caption:`😂 ${r.data.title||'Meme'}`},{quoted:m}); else m.reply('❌ Gagal') } catch { m.reply('❌ Gagal mengambil meme') }
}
break

case "pakustad": {
  const nasihat=['Ilmu tanpa amal ibarat pohon tanpa buah.','Sabar bukan tentang lama menunggu, tapi perilaku saat menunggu.','Doa tanpa usaha bohong, usaha tanpa doa sombong.','Jangan menunda kebaikan.','Orang paling kaya adalah yang paling sedikit kebutuhannya.','Ketika merasa sendiri, ingat Allah selalu bersama.','Jangan mengeluh, 80% orang tidak peduli.','Hidup ini singkat, jangan habiskan untuk dendam.']
  m.reply(`🕌 *Nasihat*\n\n"${nasihat[Math.floor(Math.random()*nasihat.length)]}"`)
}
break

case "planet": {
  if (!text) return m.reply(`*Contoh:* ${command} mars`)
  const planetData={merkurius:{d:'Terkecil, terdekat Matahari',s:'-180°C / 430°C'},venus:{d:'Terpanas di tata surya',s:'462°C'},bumi:{d:'Satu-satunya berisi kehidupan',s:'-89°C / 57°C'},mars:{d:'Planet merah',s:'-87°C / -5°C'},jupiter:{d:'Terbesar',s:'-108°C'},saturnus:{d:'Cincin paling terkenal',s:'-139°C'},uranus:{d:'Rotasi miring 98°',s:'-197°C'},neptunus:{d:'Terjauh dari Matahari',s:'-201°C'}}
  const pl=planetData[text.toLowerCase()]; if(!pl) return m.reply(`Pilihan: ${Object.keys(planetData).join(', ')}`)
  m.reply(`🪐 *${text}*\n📝 ${pl.d}\n🌡️ ${pl.s}`)
}
break

case "quotesanime": {
  const animeQ=[{c:'Naruto',q:'Jika tidak suka takdir, berjuanglah mengubahnya!'},{c:'Luffy',q:'Orang paling bebas di lautan adalah Raja Bajak Laut!'},{c:'Levi',q:'Pilih yang paling tidak akan kamu sesali.'},{c:'Itachi',q:'Yang penting aku tahu siapa diriku.'},{c:'Saitama',q:'Terus berusaha melampaui batasmu.'},{c:'Eren',q:'Jika tidak berjuang, tidak bisa menang!'}]
  const aq=animeQ[Math.floor(Math.random()*animeQ.length)]
  m.reply(`🎌 *Quotes Anime*\n\n"${aq.q}"\n— *${aq.c}*`)
}
break

case "tafsirmimpi": {
  if (!text) return m.reply(`*Contoh:* ${command} ular`)
  const mimpiDB={ular:'Musuh mendekat',air:'Rezeki datang',terbang:'Keinginan tercapai',jatuh:'Perlu hati-hati',menikah:'Perubahan besar',hujan:'Rahmat',ikan:'Rezeki melimpah',kucing:'Ada yang iri',api:'Amarah perlu dikontrol',laut:'Perjalanan jauh',gunung:'Hambatan teratasi'}
  const mk=Object.keys(mimpiDB).find(k=>text.toLowerCase().includes(k))
  m.reply(`🌙 *Tafsir Mimpi: ${text}*\n\n🔮 ${mk?mimpiDB[mk]:'Ada hal baru akan terjadi. Tetap berdoa.'}`)
}
break

case "sfw":
case "sfwrandom": {
  const sfwList = ['waifu','neko','shinobu','megumin','bully','cuddle','cry','hug','awoo','kiss','lick','pat','smug','bonk','yeet','blush','smile','wave','highfive','handhold','nom','bite','glomp','slap','kick','happy','wink','poke','dance','cringe']
  const pick = sfwList[Math.floor(Math.random() * sfwList.length)]
  try {
    const r = await axios.get(`${global.apiNekosBest}/api/v2/${pick}`)
    const u = r.data?.results?.[0]?.url
    if (u) await NXL.sendMessage(m.chat, { image: { url: u }, caption: `🌸 *SFW Random — ${pick}*\n_Powered by ${ownername}_` }, { quoted: m })
    else m.reply('❌ Gagal ambil gambar')
  } catch { m.reply('❌ Gagal ambil gambar') }
}
break

case "waifu":
case "sfwwaifu": {
  try {
    const r = await axios.get(`${global.apiNekosBest}/api/v2/waifu`)
    const u = r.data?.results?.[0]?.url
    if (u) await NXL.sendMessage(m.chat, { image: { url: u }, caption: `🎀 *Waifu*\n_Powered by ${ownername}_` }, { quoted: m })
    else m.reply('❌ Gagal')
  } catch { m.reply('❌ Gagal') }
}
break

case "cekgempa": {
  try { const r=await axios.get(`${global.apiBmkg}/DataMKG/TEWS/autogempa.json`); const g=r.data?.Infogempa?.gempa; m.reply(`🌍 *Gempa Terkini*\n\n📅 ${g.Tanggal} ${g.Jam}\n📍 ${g.Wilayah}\n📏 M${g.Magnitude}\n📐 ${g.Kedalaman}\n⚠️ ${g.Potensi||'-'}`) } catch { m.reply('❌ Gagal ambil data BMKG') }
}
break

case "cekcuaca": {
  if (!text) return m.reply(`*Contoh:* ${command} Jakarta`)
  try { const r=await axios.get(`${global.apiOpenweather}/data/2.5/weather?q=${encodeURIComponent(text)}&appid=060a6bcfa19809c2cd4d97a212b19273&units=metric&lang=id`); const w=r.data; m.reply(`🌤️ *Cuaca ${w.name}*\n\n🌡️ ${w.main.temp}°C\n💧 ${w.main.humidity}%\n🌬️ ${w.wind.speed} m/s\n☁️ ${w.weather[0].description}`) } catch { m.reply('❌ Kota tidak ditemukan') }
}
break

case "cekkalender": {
  const now=new Date(); m.reply(`📅 *Hari Ini*\n\n${now.toLocaleDateString('id-ID',{weekday:'long',year:'numeric',month:'long',day:'numeric',timeZone:'Asia/Jakarta'})}\n⏰ ${now.toLocaleTimeString('id-ID',{timeZone:'Asia/Jakarta'})} WIB`)
}
break

case 'igstalk': {
  if (!text) return reply(`Masukkan Username Instagram\n\nContoh: ${prefix + command} cristiano`)
  NXL.sendMessage(m.chat, { react: { text: '🕒', key: m.key }})

  try {
    const SIGNING_KEY_HEX = "792525efde6d921d6055a5d62dcebd39c8b5364e99fa87c5adf0e89391266d9c"
    const TS_BASELINE = 1773148641059
    const API_BASE = `${global.apiFastdl}/api/v1/instagram`
    const CORS_PROXY = `${global.apiSiputzxCors}/`

    async function callEndpoint(endpoint, body) {
      const ts = Date.now()
      const key = Buffer.from(SIGNING_KEY_HEX, "hex")
      const _s = require("crypto").createHmac("sha256", key).update(JSON.stringify(body) + ts).digest("hex")
      const payload = { ...body, ts, _ts: TS_BASELINE, _tsc: 0, _sv: 2, _s }

      const res = await fetch(`${CORS_PROXY}${API_BASE}/${endpoint}`, {
        method: "POST",
        headers: {
          "accept": "application/json",
          "origin": `${global.webFastdl}`,
          "referer": `${global.webFastdl}/`,
          "content-type": "application/json"
        },
        body: JSON.stringify(payload)
      })

      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return await res.json()
    }

    const username = text
      .replace(/^https?:\/\/(www\.)?instagram\.com\//, "")
      .replace(/^@/, "")
      .replace(/\/.*$/, "")
      .trim()

    if (!/^[a-zA-Z0-9._]{1,30}$/.test(username)) {
      return reply("Format username tidak valid.")
    }

    const [profileRes, userInfoRes, storiesRes] = await Promise.all([
      callEndpoint("profile", { username }).catch(() => ({})),
      callEndpoint("userInfo", { username }).catch(() => ({})),
      callEndpoint("stories", { username }).catch(() => ({}))
    ])

    const u = userInfoRes?.result?.[0]?.user || {}
    const p = profileRes?.result || {}
    const stories = Array.isArray(storiesRes?.result) ? storiesRes.result : []

    const fullName    = u.full_name || p.full_name || '-'
    const bio         = u.biography || p.biography || '-'
    const isVerified  = u.is_verified ?? p.is_verified ? '✅' : '❌'
    const isPrivate   = u.is_private ?? p.is_private ? '🔒 Private' : '🌐 Public'
    const followers   = (u.follower_count ?? '?').toLocaleString?.() ?? u.follower_count ?? '?'
    const following   = (u.following_count ?? '?').toLocaleString?.() ?? u.following_count ?? '?'
    const posts       = (u.media_count ?? '?').toLocaleString?.() ?? u.media_count ?? '?'
    const profilePic  = u.hd_profile_pic_url_info?.url || u.profile_pic_url_hd || u.profile_pic_url || p.profile_pic_url || null
    const extUrl      = u.external_url || p.external_url || null

    const teks = `
┌──「 *INSTAGRAM STALKING* 」
▢ *📛 Name:* ${fullName}
▢ *🔖 Username:* @${username}
▢ *✅ Verified:* ${isVerified}
▢ *🔒 Status:* ${isPrivate}
▢ *👥 Followers:* ${followers}
▢ *🫂 Following:* ${following}
▢ *🏝️ Posts:* ${posts}
▢ *📌 Bio:* ${bio}${extUrl ? `\n▢ *🔗 Link:* ${extUrl}` : ''}
▢ *🌍 Profile:* ${global.igShort}/${username}
▢ *📺 Active Stories:* ${stories.length}
└────────────`

    if (profilePic) {
      await NXL.sendMessage(m.chat, {
        image: { url: profilePic },
        caption: teks
      }, { quoted: m })
    } else {
      await NXL.sendMessage(m.chat, { text: teks }, { quoted: m })
    }

    NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key }})

  } catch (err) {
    console.error(err)
    NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key }})
    reply(`Gagal mengambil data. Pastikan username *Instagram* valid.`)
  }
}
break

case 'ttstalk':
case 'tiktokstalk': {
  if (!text) return reply(`Masukkan Username TikTok\n\nContoh: ${prefix + command} RifkyShre`)
  NXL.sendMessage(m.chat, { react: { text: '🕒', key: m.key }})

  try {
    const username = text.replace(/^@/, "").trim()

    const html = await fetch(`${global.ttWeb}/@${encodeURIComponent(username)}`, {
      headers: {
        authority: "www.tiktok.com",
        "sec-ch-ua-mobile": "?1",
        "sec-ch-ua-platform": "Android",
        "user-agent": "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
      }
    }).then(a => a.text())

    const match =
      html.match(/<script id="__UNIVERSAL_DATA_FOR_REHYDRATION__"[^>]*>([\s\S]*?)<\/script>/) ||
      html.match(/<script id="SIGI_STATE"[^>]*>([\s\S]*?)<\/script>/)

    if (!match) return reply("Gagal mengambil data. Username tidak ditemukan.")

    const json = JSON.parse(match[1])
    const scope = json.__DEFAULT_SCOPE__ || json.DEFAULT_SCOPE
    const detail = scope?.["webapp.user-detail"] || scope?.["webapp.reflow.profile.initial"]
    const userInfo = detail?.userInfo

    let u, s
    if (userInfo?.user) {
      u = userInfo.user
      s = userInfo.stats || userInfo.statsV2 || {}
    } else if (json.UserModule?.users) {
      const id = Object.keys(json.UserModule.users)[0]
      u = json.UserModule.users[id]
      s = json.UserModule.stats?.[id] || {}
    } else {
      return reply("Data user tidak ditemukan.")
    }

    const r = {
      username: u.uniqueId || username,
      nickname: u.nickname || '-',
      verified: u.verified ? '✅' : '❌',
      privateAccount: u.privateAccount ? '🔒 Private' : '🌐 Public',
      region: u.region || '-',
      createdAt: u.createTime ? new Date(u.createTime * 1000).toLocaleDateString('id-ID') : '-',
      signature: u.signature || '-',
      bioLink: u.bioLink?.link || null,
      avatar: u.avatarLarger || u.avatarMedium || u.avatarThumb || null,
      followers: Number(s.followerCount || 0).toLocaleString('id-ID'),
      following: Number(s.followingCount || 0).toLocaleString('id-ID'),
      hearts: Number(s.heartCount ?? s.heart ?? 0).toLocaleString('id-ID'),
      videos: Number(s.videoCount || 0).toLocaleString('id-ID'),
      friends: Number(s.friendCount || 0).toLocaleString('id-ID'),
    }

    const teks = `
┌──「 *TIKTOK STALKING* 」
▢ *🔖 Username:* @${r.username}
▢ *📛 Nickname:* ${r.nickname}
▢ *✅ Verified:* ${r.verified}
▢ *🔒 Status:* ${r.privateAccount}
▢ *🌍 Region:* ${r.region}
▢ *📅 Bergabung:* ${r.createdAt}
▢ *👥 Followers:* ${r.followers}
▢ *🫂 Following:* ${r.following}
▢ *❤️ Likes:* ${r.hearts}
▢ *🎬 Videos:* ${r.videos}
▢ *👫 Friends:* ${r.friends}
▢ *📌 Bio:* ${r.signature}${r.bioLink ? `\n▢ *🔗 Link:* ${r.bioLink}` : ''}
▢ *🌐 Profile:* ${global.ttWeb}/@${r.username}
└────────────`

    if (r.avatar) {
      await NXL.sendMessage(m.chat, {
        image: { url: r.avatar },
        caption: teks
      }, { quoted: m })
    } else {
      await NXL.sendMessage(m.chat, { text: teks }, { quoted: m })
    }

    NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key }})

  } catch (err) {
    console.error(err)
    NXL.sendMessage(m.chat, { react: { text: '❌', key: m.key }})
    reply(`Gagal mengambil data. Pastikan username *TikTok* valid.`)
  }
}
break

case "stalkroblox": {
  if (!text) return m.reply(`*Contoh:* ${command} username`)
  try { const s=await axios.post(`${global.apiRoblox}/v1/usernames/users`,{usernames:[text]}); const u=s.data?.data?.[0]; if(!u)return m.reply('❌ Tidak ditemukan'); const p=await axios.get(`${global.apiRoblox}/v1/users/${u.id}`); const d=p.data; m.reply(`🎮 *Roblox: ${d.displayName}*\n\n🔗 @${d.name}\n🆔 ${d.id}\n📝 ${d.description||'-'}\n${d.isBanned?'⛔ BANNED':'✅ Aktif'}`) } catch { m.reply('❌ Gagal') }
}
break

case "npmjs": {
  if (!text) return m.reply(`*Contoh:* ${command} axios`)
  try { const r=await axios.get(`${global.apiNpmRegistry}/${encodeURIComponent(text)}`); const d=r.data; m.reply(`📦 *${d.name}* v${d['dist-tags']?.latest}\n\n📝 ${d.description||'-'}\n👤 ${d.author?.name||'-'}\n📜 ${d.license||'-'}\n📥 npm i ${d.name}`) } catch { m.reply('❌ Package tidak ditemukan') }
}
break

case "yts": {
  if (!text) return m.reply(`*Contoh:* ${command} lagu`)
  try { const r=await yts(text); const v=r.videos.slice(0,5); if(!v.length)return m.reply('❌ Tidak ditemukan'); let t=`🔍 *YT Search: ${text}*\n\n`; v.forEach((x,i)=>{t+=`${i+1}. *${x.title}*\n   ⏱️ ${x.timestamp} | 👁️ ${x.views?.toLocaleString()||'?'}\n   ${x.url}\n\n`}); m.reply(t) } catch { m.reply('❌ Gagal search YouTube') }
}
break

case "twitter": case "xdl": {
  if (!text) return m.reply(`*Contoh:* ${command} ${global.xWeb}/...`)
  if (!text.includes('x.com')&&!text.includes('twitter.com')) return m.reply('❌ Link harus dari x.com')
  try { const FormData=require('form-data'); let f=new FormData(); f.append('q',text); f.append('lang','en'); const {data}=await axios.post(`${global.apiSavetwitter}/api/ajaxSearch`,f,{headers:f.getHeaders(),timeout:15000}); const $=cheerio.load(data?.data||''); const u=$('a[href]').first().attr('href'); if(u) await NXL.sendMessage(m.chat,{video:{url:u},caption:'🐦 *Twitter/X*'},{quoted:m}); else m.reply('❌ Video tidak ditemukan') } catch { m.reply('❌ Gagal download Twitter') }
}
break

case "threads": case "threadsdl": {
  if (!text||!text.includes('threads.net')) return m.reply(`*Contoh:* ${command} ${global.threadsWeb}/...`)
  try { const r=await axios.get(`${global.apiSiputzx}/api/d/threads?url=${encodeURIComponent(text)}`); const d=r.data?.data||r.data; if(d?.video) await NXL.sendMessage(m.chat,{video:{url:d.video},caption:d.caption||''},{quoted:m}); else if(d?.image) await NXL.sendMessage(m.chat,{image:{url:d.image},caption:d.caption||''},{quoted:m}); else m.reply('❌ Media tidak ditemukan') } catch { m.reply('❌ Gagal download Threads') }
}
break

case "gdrive": {
  if (!text||!text.includes('drive.google.com')) return m.reply(`*Contoh:* ${command} ${global.driveWeb}/...`)
  const gid=text.match(/\/d\/(.+?)(?:\/|$)/)?.[1]||text.match(/id=(.+?)(?:&|$)/)?.[1]
  if (!gid) return m.reply('❌ ID file tidak ditemukan')
  m.reply(`📥 *Google Drive*\n\n🔗 Direct: ${global.driveWeb}/uc?export=download&id=${gid}`)
}
break

case "gitclone": {
  if (!text||!text.includes('github.com')) return m.reply(`*Contoh:* ${command} ${global.githubWeb}/user/repo`)
  try { const mt=text.match(/github\.com\/([^\/]+)\/([^\/\s]+)/); if(!mt) return m.reply('❌ URL tidak valid'); await NXL.sendMessage(m.chat,{document:{url:`${global.githubWeb}/${mt[1]}/${mt[2].replace('.git','')}/archive/refs/heads/main.zip`},mimetype:'application/zip',fileName:`${mt[2].replace('.git','')}.zip`},{quoted:m}) } catch { m.reply('❌ Gagal clone. Pastikan repo public.') }
}
break



case "caklontong": case "tebakhero": case "family100": case "tebakgambar": case "tebaklogo": case "tebakgame": case "tebakmakanan": case "lengkapikalimat": case "tebakbendera": case "siapakahaku": case "tebaklagu": case "sambungkata": case "tebakgenshin": case "tebakhewan": case "tebakinggris": case "tebakkalimat": case "tebakanime": case "tebakkata": case "susunkata": case "tebakjorok": case "tebaklirik": case "asahotak": case "tebakjkt": {
  if (!m.isGroup) return m.reply(mess.group)
  if (global._gameSessions?.[m.chat]) return m.reply('⚠️ Game aktif! Ketik *.nyerah* untuk menyerah.')
  const _gFiles={caklontong:'./game/caklontong.json',family100:'./game/family100.json',tebakgambar:'./game/tebakgambar.json',tebaklogo:'./game/tebaklogo.json',tebakhero:'./game/tebakhero.json',tebakgenshin:'./game/tebakgenshin.json',tebakgame:'./game/tebakgame.json',tebakmakanan:'./game/tebakmakanan.json',tebakbendera:'./game/tebakbendera.json',tebaklagu:'./game/tebaklagu.json',sambungkata:'./game/sambungkata.json',tebaklirik:'./game/tebaklirik.json',asahotak:'./game/asahotak.json',lengkapikalimat:'./game/lengkapikalimat.json',siapakahaku:'./game/siapakahaku.json',susunkata:'./game/susunkata.json',tebakkata:'./game/tebakkata.json',tebakanime:'./game/tebakanime.json',tebakkalimat:'./game/tebakkalimat.json',tebakjorok:'./game/tebakjorok.json',tebakinggris:'./game/tebakinggris.json',tebakhewan:'./game/tebakhewan.json',tebakjkt:'./game/tebakjkt.json'}
  const _gf=_gFiles[command]; if(!_gf||!fs.existsSync(_gf)) return m.reply('❌ Data game tidak tersedia.')
  try { const _gd=JSON.parse(fs.readFileSync(_gf,'utf-8')); if(!Array.isArray(_gd)||!_gd.length) return m.reply('❌ Data kosong.'); const _s=_gd[Math.floor(Math.random()*_gd.length)]; const _q=_s.soal||_s.deskripsi||'Tebak!'; const _ra=_s.jawaban||_s.name||_s.judul; if(!_ra) return m.reply('❌ Soal error.'); const _j=Array.isArray(_ra)?_ra.map(j=>String(j).toLowerCase()):[String(_ra).toLowerCase()]; const _cap=`🎮 *${command.toUpperCase()}*\n\n${command==='tebaklagu'?'Dengarkan audio di atas!':_q}\n\n⏳ 60 detik!\n📌 Reply pesan ini!\nKetik *.nyerah* untuk menyerah.`; let _sm; if(command==='tebaklagu'){await NXL.sendMessage(m.chat,{audio:{url:_q},mimetype:'audio/mpeg',ptt:true},{quoted:m});_sm=await NXL.sendMessage(m.chat,{text:_cap},{quoted:m})}else if(_s.gambar||_s.img){_sm=await NXL.sendMessage(m.chat,{image:{url:_s.gambar||_s.img},caption:_cap},{quoted:m})}else{_sm=await NXL.sendMessage(m.chat,{text:_cap},{quoted:m})}; if(!global._gameSessions) global._gameSessions={}; global._gameSessions[m.chat]={jawaban:_j,messageId:_sm.key.id,timeout:setTimeout(()=>{if(global._gameSessions?.[m.chat]){const _gc=global.getLiveConn&&global.getLiveConn();if(_gc){try{_gc.sendMessage(m.chat,{text:`⏰ Waktu habis! Jawaban: *${_j.join(' / ')}*`})}catch{}}delete global._gameSessions[m.chat]}},60000)} } catch(e) { m.reply('❌ Error: '+e.message) }
}
break

case "nyerah": {
  if (!global._gameSessions?.[m.chat]) return m.reply('❌ Tidak ada game aktif.')
  const _gs=global._gameSessions[m.chat]; clearTimeout(_gs.timeout); delete global._gameSessions[m.chat]
  m.reply(`🏳️ Menyerah! Jawaban: *${Array.isArray(_gs.jawaban)?_gs.jawaban.join(' / '):_gs.jawaban}*`)
}
break



case "jpm2": {
  if (!isCreator) return m.reply(mess.owner)
  if (global.statusjpm) return m.reply('⚠️ JPM sedang berjalan! Tunggu selesai atau hentikan dengan .stopjpm')
  if (!text) return m.reply(`*Contoh:* ${command} pesannya & bisa dengan foto juga`)
  if (!global.botReady) return m.reply('⏳ Bot baru reconnect, tunggu sebentar.')

  let jpm2Media = null
  if (/image/.test(mime)) {
    jpm2Media = await NXL.downloadAndSaveMediaMessage(qmsg)
  }

  // [PATCH B] Lock statusjpm kini dikelola oleh runBroadcast (akuisisi atomik + rilis finally).


  let jpm2Groups
  try {
    jpm2Groups = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    delete global.statusjpm
    if (jpm2Media && fs.existsSync(jpm2Media)) fs.unlinkSync(jpm2Media)
    return m.reply(`❌ Gagal fetch grup: ${e.message}`)
  }

  const jpm2Ids = Object.keys(jpm2Groups)


  let jpm2BL = []
  try { jpm2BL = loadBlacklistJpm() } catch { jpm2BL = [] }
  const jpm2BLIds = jpm2BL.map(v => v.id)
  const jpm2Filtered = jpm2Ids.filter(id => !jpm2BLIds.includes(id))
  const jpm2Skipped = jpm2Ids.length - jpm2Filtered.length

  const jpm2Content = jpm2Media
    ? { image: fs.readFileSync(jpm2Media), caption: text }
    : { text }

  const jpm2Chat = m.chat
  const jpm2Jenis = jpm2Media ? "teks & foto" : "teks"
  await m.reply(`⏳ JPM2 ${jpm2Jenis}\n📋 Target: *${jpm2Filtered.length}* grup\n${jpm2Skipped > 0 ? `⛔ Skip blacklist: *${jpm2Skipped}*` : ''}`)

  // [PATCH B] Broadcast lewat engine terpadu
  const _res = await runBroadcast({
    lockFlag: 'statusjpm',
    stopFlag: 'stopjpm',
    targets: jpm2Filtered,
    delayMs: () => global.JedaJpm || 5000,
    sendOne: async (conn, gid) => { _trackJpmMsg(await conn.sendMessage(gid, jpm2Content, { quoted: FakeChannel })) },
    cleanup: () => { if (jpm2Media && fs.existsSync(jpm2Media)) fs.unlinkSync(jpm2Media) }
  })

  if (_res.rejected) return m.reply('⚠️ JPM sedang berjalan! Tunggu selesai atau hentikan dengan .stopjpm')

  await NXL.sendMessage(jpm2Chat, { text: `✅ JPM2 ${jpm2Jenis} selesai!\nTerkirim: *${_res.sukses}/${jpm2Filtered.length}* grup.` }, { quoted: m })
}
break

case "jpmtesti": {
  if (!isCreator) return m.reply(mess.owner)
  if (global.statusjpm) return m.reply('⚠️ JPM sedang berjalan! Tunggu selesai atau hentikan dengan .stopjpm')
  if (!text) return m.reply(`*Contoh:* ${command} caption testimoni (wajib kirim/reply foto)`)
  if (!/image/.test(mime)) return m.reply('❌ Wajib menyertakan foto! Reply atau kirim foto dengan caption.')
  if (!global.botReady) return m.reply('⏳ Bot baru reconnect, tunggu sebentar.')

  // [PATCH B] Lock statusjpm dikelola oleh runBroadcast (akuisisi atomik + rilis finally).
  const testiMedia = await NXL.downloadAndSaveMediaMessage(qmsg)


  let testiGroups
  try {
    testiGroups = await Promise.race([
      NXL.groupFetchAllParticipating(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 20000))
    ])
  } catch (e) {
    delete global.statusjpm
    if (fs.existsSync(testiMedia)) fs.unlinkSync(testiMedia)
    return m.reply(`❌ Gagal fetch grup: ${e.message}`)
  }

  const testiIds = Object.keys(testiGroups)


  let testiBL = []
  try { testiBL = loadBlacklistJpm() } catch { testiBL = [] }
  const testiBLIds = testiBL.map(v => v.id)
  const testiFiltered = testiIds.filter(id => !testiBLIds.includes(id))

  const testiChat = m.chat
  await m.reply(`⏳ JPM Testi ke *${testiFiltered.length}* grup...`)

  // [PATCH B] Broadcast lewat engine terpadu
  const _res = await runBroadcast({
    lockFlag: 'statusjpm',
    stopFlag: 'stopjpm',
    targets: testiFiltered,
    delayMs: () => global.JedaJpm || 5000,
    sendOne: async (conn, gid) => {
      _trackJpmMsg(await conn.sendMessage(gid, {
        image: fs.readFileSync(testiMedia),
        caption: text,
        contextInfo: { isForwarded: true, mentionedJid: [m.sender], businessMessageForwardInfo: { businessOwnerJid: botNumber } }
      }, { quoted: FakeChannel }))
    },
    cleanup: () => { if (fs.existsSync(testiMedia)) fs.unlinkSync(testiMedia) }
  })

  if (_res.rejected) return m.reply('⚠️ JPM sedang berjalan! Tunggu selesai atau hentikan dengan .stopjpm')

  await NXL.sendMessage(testiChat, { text: `✅ JPM Testi selesai!\nTerkirim: *${_res.sukses}/${testiFiltered.length}* grup.` }, { quoted: m })
}
break





case "teslink":
  await NXL.sendMessage(m.chat, {
    text: `${global.waMe}/6287728163189`,
    linkPreview: null
  }, { quoted: m })
break







case "nsfw":
case "nsfwhentai": {

  try {
    const r = await axios.get(`${global.apiNekosapi}/v4/images/random?rating=safe&tags=exposed_girl_breasts&limit=1`)
    const item = Array.isArray(r.data) ? r.data[0] : r.data?.items?.[0] ?? r.data?.[0]
    const u = item?.image_url || item?.url || item?.imageUrl
    if (u) await NXL.sendMessage(m.chat, { image: { url: u }, caption: `${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwneko":
case "neko": {
  try {
    const r = await axios.get(`${global.apiPurrbot}/api/img/sfw/neko/img`)
    const u = r.data?.link
    if (u) await NXL.sendMessage(m.chat, { image: { url: u }, caption: `${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwass":
case "kitsune": {
  try {
    const r = await axios.get(`${global.apiNekosapi}/v4/images/random?rating=safe&tags=red_hair&limit=1`)
    const item = Array.isArray(r.data) ? r.data[0] : r.data?.items?.[0] ?? r.data?.[0]
    const u = item?.image_url || item?.url || item?.imageUrl
    if (u) await NXL.sendMessage(m.chat, { image: { url: u }, caption: `${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwboobs":
case "pat": {
  try {
    const r = await axios.get(`${global.apiPurrbot}/api/img/sfw/pat/gif`)
    const u = r.data?.link
    if (u) await NXL.sendMessage(m.chat, { video: { url: u }, gifPlayback: true, caption: `${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwpussy":
case "hug": {
  try {
    const r = await axios.get(`${global.apiPurrbot}/api/img/sfw/hug/gif`)
    const u = r.data?.link
    if (u) await NXL.sendMessage(m.chat, { video: { url: u }, gifPlayback: true, caption:`${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwahegao":
case "kiss": {
  try {
    const r = await axios.get(`${global.apiPurrbot}/api/img/sfw/kiss/gif`)
    const u = r.data?.link
    if (u) await NXL.sendMessage(m.chat, { video: { url: u }, gifPlayback: true, caption: `${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwbj":
case "cuddle": {
  try {
    const r = await axios.get(`${global.apiPurrbotV2}/v2/img/sfw/blush/gif`)
    const u = r.data?.link
    if (u) await NXL.sendMessage(m.chat, { video: { url: u }, gifPlayback: true, caption: `${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwtrap":
case "blush": {
  try {
    const r = await axios.get(`${global.apiPurrbot}/api/img/sfw/blush/gif`)
    const u = r.data?.link
    if (u) await NXL.sendMessage(m.chat, { video: { url: u }, gifPlayback: true, caption:`${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwyuri":
case "smile": {
  try {
    const r = await axios.get(`${global.apiPurrbotV2}/v2/img/nsfw/yuri/gif`)
    const u = r.data?.link
    if (u) await NXL.sendMessage(m.chat, { video: { url: u }, gifPlayback: true, caption: `${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "animedance":
case "dance": {
  try {
    const r = await axios.get(`${global.apiPurrbot}/api/img/sfw/dance/gif`)
    const u = r.data?.link
    if (u) await NXL.sendMessage(m.chat, { video: { url: u }, gifPlayback: true, caption: '💃 *Dance*' }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwecchi":
case "catgirl": {

  try {
    const r = await axios.get(`${global.apiNekosapi}/v4/images/random?rating=safe&tags=bikini&limit=1`)
    const item = Array.isArray(r.data) ? r.data[0] : r.data?.items?.[0] ?? r.data?.[0]
    const u = item?.image_url || item?.url || item?.imageUrl
    if (u) await NXL.sendMessage(m.chat, { image: { url: u }, caption: `${command}` }, { quoted: m })
    else m.reply('❌ Gagal mengambil gambar')
  } catch { m.reply('❌ Gagal mengambil gambar') }
}
break

case "nsfwmenu":
case "nsfwlist": {
  const menuNsfw = `🔞 *NSFW MENU*

╔══════════════════╗
║  Ketik salah satu:
╠══════════════════╣
║ • nsfw / nsfwhentai
║ • nsfwass
║ • nsfwboobs
║ • nsfwpussy
║ • nsfwneko
║ • nsfwahegao
║ • nsfwbj
║ • nsfwtrap
║ • nsfwyuri
║ • nsfwecchi
╚══════════════════╝

_Pastikan NSFW sudah diaktifkan admin grup_`
  m.reply(menuNsfw)
}
break

case 'afk': {
  if (!global.db.users) global.db.users = {}
  if (!global.db.users[m.sender]) global.db.users[m.sender] = {}
  let user = global.db.users[m.sender]
  user.afkTime = +new Date
  user.afkReason = text || ''
  reply(`💤 *${m.pushName}* Telah Afk${text ? ': ' + text : ''}`)
}
break

case 'antiswgc': {
    if (!m.isGroup) return reply('Fitur ini hanya dapat digunakan di dalam grup!')
    if (!isBotAdmins) return reply('Bot harus menjadi admin terlebih dahulu untuk menggunakan fitur ini!')
    if (!isAdmins && !isCreator) return reply('Fitur ini hanya dapat digunakan oleh Admin Grup atau Owner Bot!')

    if (!fs.existsSync('./database/antiswgc.json')) {
        safeWriteJSON('./database/antiswgc.json', [])
    }
    let antiswgcList = JSON.parse(fs.readFileSync('./database/antiswgc.json', 'utf8'))

    if (args[0] === 'on') {
        if (antiswgcList.includes(m.chat)) return reply('Fitur Anti Status/Promosi Grup sudah aktif sebelumnya!')
        antiswgcList.push(m.chat)
        safeWriteJSON('./database/antiswgc.json', antiswgcList)
        reply('✅ *Anti Status Grup (antiswgc) Berhasil Diaktifkan!* Bot akan menghapus kiriman promosi Status/Story Grup dari bot/user lain.')
    } else if (args[0] === 'off') {
        if (!antiswgcList.includes(m.chat)) return reply('Fitur Anti Status/Promosi Grup belum aktif sebelumnya!')
        antiswgcList = antiswgcList.filter(jid => jid !== m.chat)
        safeWriteJSON('./database/antiswgc.json', antiswgcList)
        reply('❌ *Anti Status Grup (antiswgc) Berhasil Dinonaktifkan!*')
    } else {
        reply(`Silakan pilih opsi *on* atau *off*.\n\nContoh:\n*${prefix + command} on*\n*${prefix + command} off*`)
    }
}
break

case 'menuislami': case 'islami': {
    let teks = `🕋 *NXL - MENU ISLAMI* 🕋\n\n`
    teks += `• *${prefix}jadwalsholat [nama daerah]*\n`
    teks += `  _Melihat waktu adzan & sholat 5 waktu berdasarkan lokasi._\n\n`
    teks += `• *${prefix}infolibur*\n`
    teks += `  _Mengecek kalender libur nasional dan cuti bersama resmi._\n\n`
    teks += `• *${prefix}ping*\n`
    teks += `  _Mengecek kecepatan respon dan status server bot._\n\n`
    teks += `_Semoga fitur ini bermanfaat untuk harianmu!_`
    reply(teks)
}
break

case 'jadwalsholat': case 'adzan': case 'sholat': {
    if (!text) return reply(`Silakan masukkan nama kota atau daerah kamu!\n\n*Contoh:*\n${prefix + command} jakarta\n${prefix + command} surabaya`)

    await NXL.sendMessage(m.chat, { react: { text: `⏱️`, key: m.key }})

    try {

        const searchCity = await fetchJson(`${global.apiMyquran}/v2/sholat/kota/cari/${encodeURIComponent(text)}`)
        if (!searchCity.status || searchCity.data.length === 0) throw new Error("Kota atau daerah tidak ditemukan.")

        const idKota = searchCity.data[0].id
        const namaKota = searchCity.data[0].lokasi


        const date = new Date()
        const yyyy = date.getFullYear()
        const mm = String(date.getMonth() + 1).padStart(2, '0')
        const dd = String(date.getDate()).padStart(2, '0')

        const sholatData = await fetchJson(`${global.apiMyquran}/v2/sholat/jadwal/${idKota}/${yyyy}/${mm}/${dd}`)
        if (!sholatData.status) throw new Error("Gagal mengambil rincian jadwal sholat.")

        const j = sholatData.data.jadwal
        let hasil = `🕋 *JADWAL SHOLAT & ADZAN* 🕋\n\n`
        hasil += `📍 *Lokasi:* ${namaKota}\n`
        hasil += `📅 *Tanggal:* ${j.tanggal}\n\n`
        hasil += `• *Imsak:* ${j.imsak}\n`
        hasil += `• *Subuh:* ${j.subuh}\n`
        hasil += `• *Terbit:* ${j.terbit}\n`
        hasil += `• *Dhuha:* ${j.dhuha}\n`
        hasil += `• *Dzuhur:* ${j.dzuhur}\n`
        hasil += `• *Ashar:* ${j.ashar}\n`
        hasil += `• *Maghrib:* ${j.maghrib}\n`
        hasil += `• *Isya:* ${j.isya}\n\n`
        hasil += `_“Sesungguhnya shalat itu adalah fardhu yang ditentukan waktunya atas orang-orang yang beriman.” (QS. An-Nisa: 103)_`

        reply(hasil)
        await NXL.sendMessage(m.chat, { react: { text: "☑️", key: m.key }})
    } catch (error) {
        console.error(error)
        await NXL.sendMessage(m.chat, { react: { text: "✖️", key: m.key }})
        reply(`❌ *Gagal:* ${error.message || "Terjadi kesalahan sistem daerah."}`)
    }
}
break

case 'infolibur': case 'liburnasional': {

    await NXL.sendMessage(m.chat, { react: { text: `⏱️`, key: m.key }})

    try {
        const year = new Date().getFullYear()


        const response = await axios.get(`${global.apiLibur}/api?year=${year}`)

        if (!response.data || response.data.length === 0) {
            throw new Error("Gagal mengambil data dari server libur.deno.dev.")
        }

        let hasilLibur = `📅 *KALENDER HARI LIBUR NASIONAL ${year}* 📅\n\n`

        const currentMonth = new Date().getMonth() + 1
        let count = 0
        const dataLibur = response.data

        for (let libur of dataLibur) {

            const dateParts = libur.date.split('-')
            const holidayMonth = parseInt(dateParts[1])


            if (holidayMonth >= currentMonth) {

                const formatTgl = `${dateParts[2]}-${dateParts[1]}-${dateParts[0]}`

                hasilLibur += `• *${formatTgl}* : ${libur.name}\n`
                if (libur.is_national_holiday) {
                    hasilLibur += `  _(Hari Libur Nasional)_\n`
                } else {
                    hasilLibur += `  _(Cuti Bersama)_\n`
                }
                count++
            }
        }

        if (count === 0) {
            hasilLibur += `ℹ️ Sudah tidak ada sisa hari libur nasional di sisa tahun ini.`
        } else {
            hasilLibur += `\n_ (SKB 3 Menteri)_`
        }


        reply(hasilLibur)
        await NXL.sendMessage(m.chat, { react: { text: "☑️", key: m.key }})

    } catch (error) {
        console.error(error)
        await NXL.sendMessage(m.chat, { react: { text: "✖️", key: m.key }})
        reply(`❌ *Gagal memuat info libur:* ${error.message || "API Sedang Gangguan"}`)
    }
}
break

case 'ping': case 'speed': {

    const timestamp = require('performance-now')()
    const latensi = (require('performance-now')() - timestamp).toFixed(4)


    const uptimeBot = typeof runtime === 'function' ? runtime(process.uptime()) : `${(process.uptime() / 60).toFixed(2)} menit`

    let teksPing = `🏓 *PONG!*\n\n`
    teksPing += `• *Kecepatan:* ${latensi} ms\n`
    teksPing += `• *Aktifitas:* ${uptimeBot}\n`
    teksPing += `• *Status Server:* Normal & Stabil ✅\n\n`
    teksPing += `_Menggunakan infrastruktur sistem NodeJS v${process.versions.node}_`

    reply(teksPing)
}
break

case 'animebeauty':
case 'waifucantik': {
    await m.reply('Mencari gambar anime estetik di Pinterest... 📌');

    try {


        const query = 'anime girl aesthetic icon hd';
        const res = await pinterest(query);


        const imageUrl = res[Math.floor(Math.random() * res.length)];

        await NXL.sendMessage(m.chat, {
            image: { url: imageUrl },
            caption: `✨ *ANIME ARTWORK (PINTEREST)* ✨\n\n• *Keyword:* ${query}\n• *Status:* SFW Aman ✅`
        }, { quoted: m });

    } catch (error) {
        console.error(error);
        m.reply('Gagal mengambil gambar dari Pinterest.');
    }
}
break;

case "listcase":
case "cekcase": {
  try {
    const fs = require('fs');

    const fileContent = fs.readFileSync('./case.js', 'utf-8');


    const regex = /case\s+['"]([^'"]+)['"]/g;
    let match;
    let daftarCase = [];


    while ((match = regex.exec(fileContent)) !== null) {
      daftarCase.push(match[1]);
    }

    if (daftarCase.length === 0) return m.reply("❌ Tidak ditemukan case apa pun di file ini.");


    const listMenu = daftarCase.sort().map((menu, i) => `${i + 1}. .${menu}`).join('\n');

    const caption = `💻 *TOTAL FITUR BOT (CASE): ${daftarCase.length}* 💻\n\n` +
                    `Berikut list case yang terdaftar di file ini:\n` +
                    `_Gunakan ini untuk menyusun menu manual kamu, kak!_\n\n` +
                    listMenu;

    await NXL.sendMessage(m.chat, { text: caption }, { quoted: m });
  } catch (err) {
    console.error(err);
    m.reply("❌ Gagal membaca file case.js");
  }
}
break;

case 'anime': {
  try {
    const sfwTypes = ['img/sfw/neko/gif', 'img/sfw/hug/gif', 'img/sfw/pat/gif', 'img/sfw/kiss/gif', 'img/sfw/smile/gif']
    const pick = sfwTypes[Math.floor(Math.random() * sfwTypes.length)]
    const { data: res } = await axios.get(`${global.apiPurrbot}/api/${pick}`, { timeout: 15000 })
    const url = res?.link || res?.url
    if (!url) return m.reply('❌ Gagal mengambil gambar anime.')
    await NXL.sendMessage(m.chat, { image: { url }, caption: `🌸 *Anime SFW*` }, { quoted: m })
  } catch (e) {
    console.error(e);
    await NXL.sendMessage(m.chat, { text: "❌ Gagal ambil gambar anime, coba lagi nanti." }, { quoted: m });
  }
  }
  break;

case 'github': {
  const user = args[0];
  if (!user) return NXL.sendMessage(m.chat, { text: "Masukkan username GitHub.\nContoh: .github torvalds" }, { quoted: m });
  try {
    const { data } = await axios.get(`https://api.github.com/users/${encodeURIComponent(user)}`, { timeout: 15000, headers: { 'User-Agent': 'WhatsApp-Bot/1.0' } });
    const teks = `
*GitHub Stalker*

Username : ${data.login}
Nama     : ${data.name || "-"}
Bio      : ${data.bio || "-"}
Lokasi   : ${data.location || "-"}
Email    : ${data.email || "-"}
Blog     : ${data.blog || "-"}
Perusahaan: ${data.company || "-"}

Repo     : ${data.public_repos}
Followers: ${data.followers}
Following: ${data.following}
Dibuat   : ${data.created_at}
`.trim();
    await NXL.sendMessage(m.chat, { image: { url: data.avatar_url }, caption: teks }, { quoted: m });
  } catch (e) {
    if (e?.response?.status === 403) {
      await NXL.sendMessage(m.chat, { text: "⚠️ GitHub API sedang mencapai batas request, silakan coba beberapa menit lagi." }, { quoted: m });
    } else {
      await NXL.sendMessage(m.chat, { text: "❌ User tidak ditemukan atau API GitHub gagal." }, { quoted: m });
    }
  }
  break;
}

case 'done1': case 'done2': case 'done3': case 'done4': case 'done5':
case 'done6': case 'done7': case 'done8': case 'done9': case 'done10': {
  if (!isCreator) return reply(global.mess.owner)
  if (!m.quoted) return reply('⚠️ Reply formulir pembelian customer untuk menggunakan fitur ini.')

  try {
    // Determine device count from command
    let jumlahPerangkat = 1
    const cmdNum = command.replace('done', '')
    if (cmdNum && !isNaN(cmdNum)) {
      jumlahPerangkat = parseInt(cmdNum)
    }
    if (jumlahPerangkat < 1) jumlahPerangkat = 1

    // Sumber identitas customer: m.chat (JID percakapan pribadi), bukan
    // m.quoted.sender. m.quoted.sender berasal dari contextInfo.participant
    // yang bisa berupa LID dan hanya "dinormalisasi" via penggantian suffix
    // oleh library, bukan lookup nomor asli. m.chat pada chat pribadi sudah
    // dinormalisasi ke nomor asli oleh library saat pesan diterima.
    const customerJid = m.chat
    const customerNumber = customerJid.split('@')[0]
    if (!customerNumber || customerJid.endsWith('@g.us')) {
      return reply('⚠️ Fitur ini hanya bisa dipakai dengan me-reply formulir di dalam chat pribadi customer (bukan grup).')
    }

    const isRunningInsideCustomerChat = m.chat === customerJid

    // Parse form data from quoted message
    const formText = m.quoted.text || m.quoted.body || m.quoted.caption || ''
    if (!formText) return reply('⚠️ Pesan yang direply tidak mengandung teks formulir.')

    const sanitizeField = (val) => {
      if (!val) return val
      return val
        .replace(/`+/g, '')
        .replace(/\*+/g, '')
        .replace(/_+/g, ' ')
        .replace(/~+/g, '')
        .replace(/[\u200B-\u200D\uFEFF]/g, '')
        .replace(/\s+/g, ' ')
        .trim()
    }

    const getField = (text, patterns) => {
      for (const pattern of patterns) {
        const escaped = pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
        const regex = new RegExp(`${escaped}[^=:]*[=:]+\\s*(.+)`, 'i')
        const match = text.match(regex)
        if (match && match[1].trim()) return sanitizeField(match[1].trim())
      }
      return null
    }

    const namaPaket = getField(formText, ['NAMA PAKET', 'PAKET']) || '-'
    const aplikasiVpn = getField(formText, ['APLIKASI VPN', 'APLIKASI']) || '-'
    const sshV2ray = getField(formText, ['SSH ATAU V2RAY', 'SSH/V2RAY', 'SSH']) || '-'
    const serverRaw = getField(formText, ['SERVER'])
    const durasiRaw = getField(formText, ['BERAPA HARI', 'DURASI', 'HARI'])

    let namaCustomer = null
    const namaLines = formText.split('\n')
    for (const line of namaLines) {
      const trimmed = sanitizeField(line.trim())
      if (/^NAMA\s*[=:]/i.test(trimmed) && !/^NAMA\s+PAKET/i.test(trimmed)) {
        const val = trimmed.replace(/^NAMA\s*[=:]+\s*/i, '').trim()
        if (val) { namaCustomer = val; break }
      }
    }
    if (!namaCustomer) namaCustomer = `Pelanggan +${customerNumber}`

    if (!serverRaw) return reply('⚠️ Field SERVER tidak ditemukan di formulir.\nPastikan formulir mengandung: SERVER = SG/ID')
    if (!durasiRaw) return reply('⚠️ Field DURASI tidak ditemukan di formulir.\nPastikan formulir mengandung: DURASI = 30')

    const serverNorm = serverRaw.toUpperCase().trim()
    let serverCode = null
    if (serverNorm.includes('SG') || serverNorm.includes('SINGAP') || serverNorm.includes('SINGAPORE')) {
      serverCode = 'SG'
    } else if (serverNorm.includes('ID') || serverNorm.includes('INDO') || serverNorm.includes('INDONESIA')) {
      serverCode = 'ID'
    }
    if (!serverCode) return reply(`⚠️ Server "${serverRaw}" tidak dikenali.\nGunakan: SG atau ID`)

    const durasiMatch = durasiRaw.match(/(\d+)/)
    if (!durasiMatch) return reply(`⚠️ Durasi "${durasiRaw}" tidak valid.\nGunakan angka, contoh: 30`)
    const durasi = parseInt(durasiMatch[1])

    let pricelist = []
    try {
      pricelist = JSON.parse(fs.readFileSync('./database/pricelist.json', 'utf8'))
    } catch {
      return reply('⚠️ Database pricelist tidak ditemukan atau rusak.')
    }

    const priceEntry = pricelist.find(p =>
      p.server.toUpperCase() === serverCode &&
      p.durasi === durasi &&
      p.perangkat === jumlahPerangkat
    )

    if (!priceEntry) {
      return reply(
        `⚠️ Harga tidak ditemukan untuk kombinasi:\n` +
        `• Server: ${serverCode}\n` +
        `• Durasi: ${durasi} hari\n` +
        `• Perangkat: ${jumlahPerangkat} IP\n\n` +
        `Pastikan data ada di database/pricelist.json`
      )
    }

    const harga = priceEntry.harga
    const tanggalNow = moment().tz('Asia/Jakarta').format('DD MMMM YYYY')
    // [DONE] Masa aktif dihitung otomatis dari jumlah hari pada durasi
    // (7/30/60/90/365, dst.), bukan hardcode, kalender valid (auto ganti
    // bulan/tahun/kabisat via moment). Opsi B: hari aktivasi = hari ke-1,
    // sehingga memakai (durasi - 1) hari. Contoh: 28-07 + 7 Hari => 03-08.
    const tanggalBerlaku = moment().tz('Asia/Jakarta').add(durasi - 1, 'days').format('DD-MM-YYYY')

    const TRX_PATH = './database/trxcounter.json'
    let trxData = { trx: 0 }
    try {
      trxData = JSON.parse(fs.readFileSync(TRX_PATH, 'utf8'))
      if (typeof trxData.trx !== 'number') trxData.trx = 0
    } catch {
      trxData = { trx: 0 }
    }
    trxData.trx += 1
    const trxNumber = `TRX-${String(trxData.trx).padStart(4, '0')}`
    fs.writeFileSync(TRX_PATH, JSON.stringify(trxData, null, 2))

    await NXL.sendMessage(m.chat, { react: { text: '⏳', key: m.key } })

    const { generateTestimonyImage, formatRupiah, maskPhoneNumber } = require('./lib/testimony')
    const imgBuffer = generateTestimonyImage({
      customerNumber: customerNumber,
      server: serverCode,
      durasi: durasi,
      perangkat: jumlahPerangkat,
      harga: harga,
      tanggal: tanggalNow,
      trx: trxNumber,
      namaPaket: namaPaket,
      aplikasiVpn: aplikasiVpn,
      sshV2ray: sshV2ray,
      ptName: global.wm,
      brandName: global.ownername,
      ownerName: (global.owner && global.owner[0]) || ''
    })

    const caption = `*TRANSAKSI BERHASIL*\n\n` +
      `┌─────────────────────\n` +
      `│ *No:* ${trxNumber}\n` +
      `│ *Customer:* ${maskPhoneNumber(customerNumber)}\n` +
      `│ *Server:* ${serverCode === 'SG' ? 'Singapore' : 'Indonesia'}\n` +
      `│ *Durasi:* ${durasi} Hari\n` +
      `│ *Perangkat:* ${jumlahPerangkat} IP\n` +
      `│ *Paket:* ${namaPaket}\n` +
      `│ *Harga:* ${formatRupiah(harga)}\n` +
      `│ *Tanggal:* ${tanggalNow}\n` +
      `│ *Berlaku Sampai:* ${tanggalBerlaku}\n` +
      `└─────────────────────\n\n` +
      `Terima kasih telah berbelanja di *${global.ownername}* ✓\n` +
      `_Layanan VPN Premium Terpercaya_`

    const channelJid = global.idsal || ''
    let channelSent = false
    if (channelJid) {
      try {
        await NXL.sendMessage(channelJid, {
          image: imgBuffer,
          caption: caption
        })
        channelSent = true
      } catch (chErr) {
        console.log('[DONE] Gagal kirim ke channel:', chErr.message)
      }
    }

    // [DONE→UPSW] Setelah upload testimoni ke Channel WA, unggah gambar testimoni
    // yang sama sebagai Status WhatsApp memakai helper .upsw yang sudah diperbaiki
    // (lib/upswStatus.js) — reuse penuh, tanpa membuat kode upload status baru.
    // Sekuensial + di-await (bukan fire-and-forget) sehingga tidak ada duplicate
    // upload / race condition. Dibungkus try/catch agar kegagalan status tidak
    // membatalkan pengiriman pesan selesai ke customer.
    try {
      const { uploadStatusWA } = require('./lib/upswStatus')
      await uploadStatusWA(NXL, { image: imgBuffer, caption: caption })
    } catch (swErr) {
      console.log('[DONE] Gagal upload status WA:', swErr.message)
    }

    await NXL.sendMessage(m.chat, { react: { text: '✅', key: m.key } })

    const completionMsg = `✅ *TRANSAKSI SELESAI*\n\n` +
      `• No TRX: ${trxNumber}\n` +
      `• Customer: ${namaCustomer}\n` +
      `• Nomor: ${customerNumber}\n` +
      `• Server: ${serverCode === 'SG' ? 'Singapore' : 'Indonesia'}\n` +
      `• Durasi: ${durasi} Hari\n` +
      `• Perangkat: ${jumlahPerangkat} IP\n` +
      `• Paket: ${namaPaket}\n` +
      `• Harga: ${formatRupiah(harga)}\n` +
      `• Tanggal: ${tanggalNow}\n\n` +
      `📢 Channel: ${channelSent ? '✓ Terkirim' : '✗ Gagal/Tidak dikonfigurasi'}\n\n` +
      `_Gunakan .done${jumlahPerangkat} untuk ${jumlahPerangkat} perangkat_`

    try {
      let linkSection = ''
      if (global.linkchannel) {
        linkSection += `\n📢 Testimoni: ${global.linkchannel}`
      }
      if (global.linkGrup) {
        linkSection += `\n📣 Grup: ${global.linkGrup}`
      }

      const thankMsg = `✅ *Pesanan Kamu Sudah Selesai!*\n\n` +
        `Halo *${namaCustomer}*,\n` +
        `Pesanan VPN kamu sudah aktif:\n\n` +
        `• Server: ${serverCode === 'SG' ? 'Singapore' : 'Indonesia'}\n` +
        `• Durasi: ${durasi} Hari\n` +
        `• Perangkat: ${jumlahPerangkat} IP\n` +
        `• Tanggal: ${tanggalNow}\n` +
        `• Berlaku Sampai: ${tanggalBerlaku}\n\n` +
        `Terima kasih telah berbelanja di *${global.ownername}* 🙏\n` +
        `Jika ada kendala, silakan hubungi admin.` +
        (linkSection ? `\n${linkSection}` : '') +
        `\n\n_PT SONTOLOYO_`
      await NXL.sendMessage(customerJid, { text: thankMsg })
    } catch (custErr) {
      console.log('[DONE] Gagal kirim ucapan ke customer:', custErr.message)
    }

  } catch (err) {
    console.error('[DONE ERROR]', err)
    reply('❌ Terjadi error saat memproses .done: ' + (err.message || err))
  }
}
break
// [UPSW] Command .upsw/.upstatus/.upstatuswa dipindah & ditingkatkan (Patch A
// compliant + dukung text-only/direct-media) — definisi tunggal ada di atas
// dekat .sendstatus. Blok lama dihapus untuk menghindari duplicate case.
case "web2apk":
case "webtoapk":
case "apkbuilder": {
  const _W2A_API_BASE = `${global.apiSynoxCloud}/tools/web2apk`;
  const _W2A_REQUEST_TIMEOUT_MS = 120000;
  const _W2A_DOWNLOAD_TIMEOUT_MS = 60000;
  const _W2A_MAX_APPNAME_LENGTH = 50;
  const _W2A_URL_REGEX = /^https?:\/\/[^\s/$.?#].[^\s]*$/i;

  function _w2aBuildFileName(appName) {
    const safe = appName
      .trim()
      .replace(/[^a-zA-Z0-9\-_ ]/g, "")
      .replace(/\s+/g, "-")
      .toLowerCase();
    return `${safe || "app"}.apk`;
  }

  function _w2aNormalizeUrl(input) {
    if (!input) return null;
    const trimmed = input.trim();
    if (!_W2A_URL_REGEX.test(trimmed)) return null;
    return trimmed;
  }

  function _w2aParseInput(t) {
    const parts = t.split("|").map((s) => s.trim());
    return { appName: parts[0] || "", websiteUrl: parts[1] || "" };
  }

  function _w2aUsageText(cmd) {
    return (
      `📱 *Web to APK Converter*\n\n` +
      `*Cara pakai:*\n` +
      `.${cmd} <nama aplikasi> | <url website>\n\n` +
      `*Contoh:*\n` +
      `.${cmd} Synox Cloud Api | ${global.apiSynoxCloud}\n\n` +
      `*Batas panjang nama aplikasi:* ${_W2A_MAX_APPNAME_LENGTH} karakter\n` +
      `*Catatan:* proses build APK bisa memakan waktu 1-2 menit, mohon bersabar.`
    );
  }

  async function _w2aDownloadApkBuffer(url) {
    let res;
    try {
      res = await axios.get(url, {
        responseType: "arraybuffer",
        timeout: _W2A_DOWNLOAD_TIMEOUT_MS,
        validateStatus: (status) => status === 200,
      });
    } catch (err) {
      if (err.code === "ECONNABORTED") {
        throw new Error("Timeout saat mengunduh file APK hasil build.");
      }
      throw new Error(`Gagal mengunduh file APK: ${err.message}`);
    }
    const buffer = Buffer.from(res.data);
    if (!buffer.length) throw new Error("File APK hasil unduhan kosong.");
    return buffer;
  }

  async function _w2aGenerateApk(appName, websiteUrl) {
    let res;
    try {
      res = await axios.get(_W2A_API_BASE, {
        params: { appName, websiteUrl },
        timeout: _W2A_REQUEST_TIMEOUT_MS,
        responseType: "arraybuffer",
        validateStatus: () => true,
      });
    } catch (err) {
      if (err.code === "ECONNABORTED") {
        throw new Error("Timeout saat membuat APK. Proses build mungkin memakan waktu lebih lama dari batas yang ditentukan.");
      }
      throw new Error(`Gagal menghubungi server: ${err.message}`);
    }

    const contentType = String(res.headers["content-type"] || "").toLowerCase();

    if (res.status !== 200) {
      let errMsg = `HTTP ${res.status} - ${res.statusText || "Request gagal"}`;
      if (contentType.includes("application/json")) {
        try {
          const parsed = JSON.parse(Buffer.from(res.data).toString("utf-8"));
          errMsg = parsed?.message || parsed?.error || errMsg;
        } catch {}
      }
      throw new Error(errMsg);
    }

    const isBinaryApk =
      contentType.includes("android.package-archive") ||
      contentType.includes("application/octet-stream") ||
      contentType.includes("application/vnd.android");

    if (isBinaryApk) {
      const buffer = Buffer.from(res.data);
      if (!buffer.length) throw new Error("File APK hasil build kosong.");
      return { buffer, fileName: _w2aBuildFileName(appName) };
    }

    if (contentType.includes("application/json")) {
      let json;
      try {
        json = JSON.parse(Buffer.from(res.data).toString("utf-8"));
      } catch {
        throw new Error("Response API tidak dikenali (JSON tidak valid).");
      }

      if (json.status === false || (typeof json.statusCode === "number" && json.statusCode >= 400)) {
        throw new Error(json.message || json.error || "Gagal membuat APK.");
      }

      const result = json.result || json.data || json;
      const apkUrl =
        result?.url || result?.apk_url || result?.apkUrl || result?.download || result?.link || null;

      if (!apkUrl) throw new Error("URL file APK tidak ditemukan pada response API.");

      const buffer = await _w2aDownloadApkBuffer(apkUrl);
      return { buffer, fileName: _w2aBuildFileName(appName) };
    }

    throw new Error(`Format response tidak dikenali (content-type: ${contentType || "tidak diketahui"}).`);
  }

  if (!text || !text.trim()) {
    return m.reply(_w2aUsageText(command));
  }

  const { appName, websiteUrl } = _w2aParseInput(text);

  if (!appName) {
    return m.reply(`⚠️ Nama aplikasi tidak boleh kosong.\n\nContoh: .${command} MyApp | https://example.com`);
  }

  if (appName.length > _W2A_MAX_APPNAME_LENGTH) {
    return m.reply(`⚠️ Nama aplikasi terlalu panjang (${appName.length}/${_W2A_MAX_APPNAME_LENGTH} karakter).`);
  }

  if (!websiteUrl) {
    return m.reply(`⚠️ URL website tidak boleh kosong.\n\nContoh: .${command} MyApp | https://example.com`);
  }

  const normalizedUrl = _w2aNormalizeUrl(websiteUrl);
  if (!normalizedUrl) {
    return m.reply(`⚠️ URL *"${websiteUrl}"* tidak valid. Pastikan diawali dengan http:// atau https://`);
  }

  await NXL.sendMessage(
    m.chat,
    {
      text:
        `📱 Sedang membuat APK...\n\n` +
        `*Nama App:* ${appName}\n` +
        `*URL:* ${normalizedUrl}\n\n` +
        `_Proses build bisa memakan waktu 1-2 menit, mohon tunggu._`,
    },
    { quoted: m }
  );

  try {
    const { buffer, fileName } = await _w2aGenerateApk(appName, normalizedUrl);

    await NXL.sendMessage(
      m.chat,
      {
        document: buffer,
        fileName,
        mimetype: "application/vnd.android.package-archive",
        caption:
          `📱 *Web to APK*\n\n` +
          `✅ *Nama App:* ${appName}\n` +
          `🌐 *URL:* ${normalizedUrl}\n` +
          `📦 *File:* ${fileName}\n\n` +
          `⚠️ Install hanya dari sumber yang kamu percaya. Aktifkan "Install from Unknown Sources" di pengaturan Android jika diperlukan.`,
      },
      { quoted: m }
    );
  } catch (err) {
    await NXL.sendMessage(
      m.chat,
      { text: `❌ Gagal membuat APK.\n\n*Alasan:* ${err.message}` },
      { quoted: m }
    ).catch(() => {});
  }
}
break
case "deepseek": {
  if (!text) return reply(`contoh .${command} halo`)

  try {
    const ax = axios.create({
      baseURL: global.apiAskai,
      timeout: 60000
    })

    let csrfToken = ""
    let cookieJar = ""

    ax.interceptors.response.use(res => {
      try {
        const sc = res.headers?.["set-cookie"]
        if (sc) {
          const parsed = (Array.isArray(sc) ? sc : [sc]).map(c => c.split(";")[0].trim()).join("; ")
          cookieJar = [...new Map(`${cookieJar}; ${parsed}`.split(";").map(v => v.split("=").map(s => s.trim())))].map(([k, v]) => `${k}=${v}`).join("; ")
        }
      } catch (e) {}
      return res
    })

    const initRes = await ax.get("/deepseek-r1", {
      headers: { "user-agent": "Mozilla/5.0 (Linux; Android 10)" }
    })
    const $ = cheerio.load(initRes.data)
    csrfToken = $('meta[name="csrf-token"]').attr("content") || initRes.data.match(/csrfToken":"([^"]+)"/)?.[1] || ""

    const payload = {
      messages: [
        { role: "system", content: "You are DeepSeek R1, a helpful and friendly AI assistant." },
        { role: "user", content: text }
      ],
      modelName: "DeepSeek R1",
      currentPagePath: "/deepseek-r1"
    }

    const chatRes = await ax.post("/api/chat", payload, {
      headers: {
        "content-type": "application/json",
        cookie: cookieJar || "",
        origin: global.apiAskai,
        referer: `${global.apiAskai}/deepseek-r1`,
        "user-agent": "Mozilla/5.0 (Linux; Android 10)",
        "x-csrf-token": csrfToken
      }
    })

    const hasil = chatRes.data?.response || chatRes.data?.choices?.[0]?.message?.content || JSON.stringify(chatRes.data)
    reply(`${hasil}`)
  } catch (e) {
    reply(`Gagal mengambil jawaban: ${e.message}`)
  }
}
break

case "ai":
case "chatgpt": {
  if (!text) return m.reply(`Contoh pemakaian:\n.ai apa itu lubang hitam?\n.ai cuaca jakarta hari ini otomatis web search\n\nUntuk mulai obrolan baru: .resetai`)

  await NXL.sendMessage(m.chat, { react: { text: "", key: m.key } })

  let { key: editKey } = await NXL.sendMessage(m.chat, { text: "mengetik..." }, { quoted: m })

  let lastEditTime = 0
  let lastSentText = ""
  const EDIT_INTERVAL = 1500

  try {
    const finalText = await chatgpt(m.sender, text, {
      webSearch: true,
      onDelta: async (partialText) => {
        const now = Date.now()
        if (now - lastEditTime < EDIT_INTERVAL) return
        if (partialText === lastSentText) return
        lastEditTime = now
        lastSentText = partialText
        try {
          await NXL.sendMessage(m.chat, { text: partialText, edit: editKey })
        } catch {}
      }
    })

    await NXL.sendMessage(m.chat, { text: finalText, edit: editKey })
  } catch (e) {
    await NXL.sendMessage(m.chat, { text: `Gagal menghubungi OpenAI:\n${e.message}`, edit: editKey })
  }
}
break

case "resetai": {
  resetHistory(m.sender)
  m.reply("Obrolan AI kamu sudah direset, mulai percakapan baru.")
}
break
case "music":
case "musicgen": {
  if (!text) return m.reply(`Contoh pemakaian:\n.music lagu galau tentang hujan dan kenangan\n.music#instrumental piano sedih\n\nTambahkan #instrumental di akhir untuk lagu tanpa vokal`)

  const isInstrumental = /#instrumental/i.test(text)
  const prompt = text.replace(/#instrumental/i, '').trim()

  await NXL.sendMessage(m.chat, { react: { text: "", key: m.key } })
  await m.reply('Sedang membuat musik, proses ini bisa memakan waktu 1-3 menit, mohon tunggu...')

  try {
    const result = await generateMusic(prompt, { instrumental: isInstrumental })

    const audioRes = await axios.get(result.audioUrl, { responseType: 'arraybuffer' })
    const buffer = Buffer.from(audioRes.data)

    await NXL.sendMessage(
      m.chat,
      {
        audio: buffer,
        mimetype: 'audio/mpeg',
        fileName: `${result.title}.mp3`
      },
      { quoted: m }
    )
  } catch (e) {
    m.reply(`Gagal membuat musik:\n${e.message}`)
  }
}
break

case "douyin":
case "dy": {
  if (!text) return m.reply("Contoh pemakaian:\n.douyin https://www.douyin.com/video/xxxxx")

  const douyinRegex = /^(https?:\/\/)?(v\.|www\.|vt\.)?(douyin\.com|iesdouyin\.com)/i
  if (!douyinRegex.test(text)) return m.reply("URL tidak valid, gunakan link douyin.com atau iesdouyin.com")

  await NXL.sendMessage(m.chat, { react: { text: "", key: m.key } })

  try {
    const douyin = new DouyinDownloader()
    const result = await douyin.download(text)

    if (!result.status) return m.reply(`Gagal mengambil data:\n${result.error}`)

    const { title, thumbnail, medias } = result.data
    const videoMedia = medias.find(m => m.videoAvailable) || medias[0]
    const audioMedia = medias.find(m => m.audioAvailable && !m.videoAvailable)

    if (!videoMedia) return m.reply("Media video tidak ditemukan")

    const videoRes = await axios.get(videoMedia.url, { responseType: "arraybuffer" })
    const videoBuffer = Buffer.from(videoRes.data)

    await NXL.sendMessage(
      m.chat,
      {
        video: videoBuffer,
        caption: title || "Douyin Video",
        mimetype: "video/mp4"
      },
      { quoted: m }
    )

    if (audioMedia) {
      const audioRes = await axios.get(audioMedia.url, { responseType: "arraybuffer" })
      const audioBuffer = Buffer.from(audioRes.data)

      await NXL.sendMessage(
        m.chat,
        {
          audio: audioBuffer,
          mimetype: "audio/mpeg",
          fileName: `${title || "audio"}.mp3`
        },
        { quoted: m }
      )
    }
  } catch (e) {
    m.reply(`Terjadi kesalahan:\n${e.message}`)
  }
}
break
case "animesearch":
case "nimegami": {
  if (!text) return m.reply("Contoh pemakaian:\n.animesearch one piece")

  await NXL.sendMessage(m.chat, { react: { text: "", key: m.key } })

  try {
    const nimegami = new Nimegami()
    const results = await nimegami.search(text)

    if (!results.length) return m.reply(`Tidak ditemukan hasil untuk "${text}"`)

    let caption = `Hasil pencarian untuk "${text}"\n\n`
    results.forEach((item, i) => {
      caption += `${i + 1}. ${item.title}\n${item.link}\n\n`
    })
    caption += `Ketik .animedetail <link> untuk melihat detail dan link download`

    await m.reply(caption)
  } catch (e) {
    m.reply(`Terjadi kesalahan:\n${e.message}`)
  }
}
break

case "animedetail": {
  if (!text) return m.reply("Contoh pemakaian:\n.animedetail https://nimegami.id/xxxxx")

  await NXL.sendMessage(m.chat, { react: { text: "", key: m.key } })

  try {
    const nimegami = new Nimegami()
    const result = await nimegami.detail(text)

    let caption = `${result.title}\n\n`
    caption += `${result.synopsis || "Sinopsis tidak tersedia"}\n\n`
    if (result.downloads.length) {
      caption += `Link download:\n`
      result.downloads.forEach((d, i) => {
        caption += `${i + 1}. ${d.server}: ${d.url}\n`
      })
    } else {
      caption += `Tidak ada link download ditemukan`
    }

    await m.reply(caption)
  } catch (e) {
    m.reply(`Terjadi kesalahan:\n${e.message}`)
  }
}
break
case 'searchcode': {
  if (!text) return m.reply(`*PENGGUNAAN SALAH*\n\nFormat: .${command} [kata_kunci] | [url_repository]\nContoh: .${command} connection | https://github.com/fanny/tools`)

  let q = ""
  let repository = ""

  if (text.includes('|')) {
    const parts = text.split('|').map(str => str.trim())
    q = parts[0]
    repository = parts[1]
  } else {
    return m.reply(`*PENGGUNAAN SALAH*\n\nKamu harus menyertakan kata kunci pencarian dan URL repository dipisah dengan tanda "|"\n\nContoh: .${command} password | ${text.trim()}`)
  }
  
  if (!q) return m.reply('[-] Mohon masukkan kata kunci atau query pencarian di depan tanda "|"')
  if (!repository) return m.reply('[-] Mohon masukkan URL repository target di belakang tanda "|"')

  try {
    await m.reply(`[ PROCESS ] Sedang mencari "${q}" pada repository ${repository}`)

    const body = { 
      query: q, 
      repository, 
      case_sensitive: false, 
      snippet_mode: "auto", 
      max_results: 10, 
      context_lines: 2 
    }

    const res = await fetch("https://api.searchcode.com/api/v1/code_search?client=scraper", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (scraper)",
      },
      body: JSON.stringify(body),
      timeout: 15000,
    })

    if (!res.ok) {
      const errText = await res.text()
      return m.reply(`[ API ERROR ] ${res.status} - ${errText}`)
    }

    const data = await res.json()

    if (!data.results || data.results.length === 0) {
      return m.reply(`[-] Kode dengan kata kunci "${q}" tidak ditemukan pada repository tersebut`)
    }

    let txt = `*CODE SEARCH ENGINE*\n\n`
    txt += `* QUERY      : \`${q}\`\n`
    txt += `* REPOSITORY : ${repository}\n`
    txt += `* TOTAL      : ${data.results.length} Hasil ditemukan\n`
    txt += `====================================\n\n`

    data.results.forEach((item, index) => {
      txt += `[ ${index + 1} ] ${item.filename}\n`
      txt += `» PATH : \`${item.filepath}\`\n`
      txt += `» LINK : ${item.location}\n`
      txt += `» SNIPPET :\n`
      txt += `\`\`\`\n`
      
      if (item.lines && typeof item.lines === 'object') {
        Object.entries(item.lines).forEach(([lineNum, codeText]) => {
          txt += `Line ${lineNum}: ${codeText}\n`
        })
      } else {
        txt += `Tidak ada cuplikan baris kode.\n`
      }
      txt += `\`\`\`\n`
      txt += `------------------------------------\n\n`
    })

    txt += `_PT SONTOLOYO_`

    await m.reply(txt)

  } catch (e) {
    console.error(e)
    m.reply(`[ ERROR SYSTEM ] ${e.message}`)
  }
}
break
default:
if (budy.startsWith('=>')) {
if (!isCreator) return
function Return(sul) {
sat = JSON.stringify(sul, null, 2)
bang = util.format(sat)
if (sat == undefined) {
bang = util.format(sul)
}
return reply(bang)
}
try {
reply(util.format(eval(`(async () => { return ${budy.slice(3)} })()`)))
} catch (e) {
reply(String(e))
}
}

if (budy.startsWith('$')) {
if (!isCreator) return
exec(budy.slice(2), (err, stdout) => {
if (err) return reply(`${err}`)
if (stdout) return reply(stdout)
})
}
}

} catch (err) {
console.error(chalk.black(chalk.bgRed(' [ ERROR ] ')), chalk.red(util.format(err?.stack || err?.message || err)))
}
}
