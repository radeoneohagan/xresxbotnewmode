require('./settings');
const fs = require('fs');
const pino = require('pino');
const chalk = require('chalk');
const readline = require('readline');
const fetch = require('node-fetch');
const FileType = require('file-type');
const { exec } = require('child_process');
const { Boom } = require('@hapi/boom');
// [PERF] Sticker helpers di-lazy-load: hanya dimuat saat benar-benar dipakai
// (NXL.sendImageAsSticker/AV/Avatar/Video). Menghemat ~400-700ms startup karena
// memuat fluent-ffmpeg + @ffmpeg-installer + node-webpmux yang berat.
let _stickerLib = null
function _requireSticker() {
  if (!_stickerLib) _stickerLib = require("./lib/media/sticker.js")
  return _stickerLib
}

const { default: WAConnection, generateWAMessageFromContent,
prepareWAMessageMedia, useMultiFileAuthState, Browsers, DisconnectReason, makeInMemoryStore, makeCacheableSignalKeyStore, fetchLatestWaWebVersion, proto, PHONENUMBER_MCC, getAggregateVotesInPollMessage, downloadContentFromMessage } = require('@whiskeysockets/baileys');

const pairingCode = global.pairing_code || process.argv.includes('--pairing-code');
const rl = readline.createInterface({ input: process.stdin, output: process.stdout })
const question = (text) => new Promise((resolve) => rl.question(text, resolve))

const _cacheFiles = {
  owner:    './database/owner.json',
  antilink: './database/antilink.json',
  antilink2:'./database/antilink2.json',
  welcome:  './database/welcome.json',
  reseller: './database/reseller.json',
  contacts: './database/contacts.json',
}
const _cacheMtime = {}

function loadCaches() {
  if (!global.cache) global.cache = { owner:[], antilink:{}, antilink2:{}, welcome:{}, reseller:[], contacts:[] }
  for (const [key, path] of Object.entries(_cacheFiles)) {
    try {
      const mtime = fs.statSync(path).mtimeMs
      if (_cacheMtime[key] === mtime) continue
      _cacheMtime[key] = mtime
      global.cache[key] = JSON.parse(fs.readFileSync(path))
    } catch {  }
  }
}
loadCaches()
setInterval(loadCaches, 30 * 1000)

const DataBase = require('./lib/source/database');
const database = new DataBase();
(async () => {
	const loadData = await database.read()
	if (loadData && typeof loadData === 'object' && Object.keys(loadData).length === 0) {
		// [FIX C2] Hanya inisialisasi jika benar-benar baru (bukan hasil corruption)
		// database.read() sudah mencoba restore dari backup jika kosong
		global.db = {
			users: {},
			groups: {},
			database: {},
			settings : {},
			...(loadData || {}),
		}
		await database.write(global.db)
	} else {
		global.db = loadData
	}

	setInterval(async () => {
		if (global.db) {
			try { await database.write(global.db) }
			catch (e) { console.error(chalk.red('[DB WRITE ERROR]'), e.message) }
		}
	}, 30000)
})();

const { MessagesUpsert, Solving } = require('./lib/source/message')
const { isUrl, generateMessageTag, getBuffer, getSizeMedia, fetchJson, sleep } = require('./lib/function');

let _reconnectDelay = 3000
let _reconnectTimer = null
let _isConnecting = false

// [PATCH A] Connection epoch — identitas unik tiap instance socket.
// Setiap kali koneksi lama dibuang, epoch dinaikkan sehingga seluruh
// event handler milik socket lama otomatis menjadi "stale" (no-op).
// Ini menghilangkan cross-talk antar socket saat reconnect (root cause RC-1/T2-07).
let _connEpoch = 0

let _lastActivityAt = Date.now()
let _connectingSince = null

// [PATCH A] Status koneksi aktif. true hanya setelah 'open', false saat 'close'/dispose.
// Dipakai oleh callback berumur panjang (autopromo, game, broadcast) untuk
// memastikan tidak mengirim melalui socket yang sudah mati (root cause RC-2).
global._connAlive = false

// [PATCH A] Accessor tunggal socket-hidup. Mengembalikan socket HANYA jika koneksi
// benar-benar terbuka. Semua konsumen berumur panjang WAJIB memakai ini,
// bukan menyimpan referensi socket dari closure.
global.getLiveConn = () => (global._connAlive && global._nxlConn) ? global._nxlConn : null

function markActivity() {
	_lastActivityAt = Date.now()
}

setInterval(() => {
	const now = Date.now()

	if (_isConnecting && _connectingSince && (now - _connectingSince) > 2 * 60 * 1000) {
		console.log(chalk.red('[WATCHDOG] _isConnecting nyangkut >2 menit, reset paksa & reconnect.'))
		_isConnecting = false
		_connectingSince = null
		scheduleReconnect('Watchdog: stuck connecting state')
		return
	}

	const IDLE_LIMIT = (global.WATCHDOG_IDLE_MS || 30 * 60 * 1000)
	if (!_isConnecting && global.botReady && (now - _lastActivityAt) > IDLE_LIMIT) {
		console.log(chalk.yellow(`[WATCHDOG] Tidak ada aktivitas selama ${Math.round((now - _lastActivityAt)/60000)} menit, paksa reconnect untuk jaga-jaga.`))
		markActivity()
		try {
			if (global._nxlConn?.ws?.close) global._nxlConn.ws.close()
			else if (global._nxlConn?.end) global._nxlConn.end(new Error('watchdog idle reconnect'))
		} catch {}
		scheduleReconnect('Watchdog: idle timeout')
	}
}, 60 * 1000)

async function prefetchAllGroups(conn) {
	try {
		console.log(chalk.cyan('[PREFETCH] Memuat daftar grup ke cache...'))
		const groups = await Promise.race([
			conn.groupFetchAllParticipating(),
			new Promise((_, rej) => setTimeout(() => rej(new Error('prefetch timeout')), 30000))
		])
		global.allGroupsCache = groups
		global.allGroupsCacheTime = Date.now()
		console.log(chalk.cyan(`[PREFETCH] Cache grup siap: ${Object.keys(groups).length} grup.`))
	} catch (e) {
		console.log(chalk.yellow(`[PREFETCH] Gagal memuat cache grup: ${e.message}`))

	}
}

async function getGroupsCached(conn) {
	// [STALE-WHILE-REVALIDATE] Jika cache ADA, langsung return (0ms).
	// Jika cache expired → trigger background refresh, tapi TETAP return cache lama.
	// JPM langsung mulai tanpa menunggu fetch selesai.
	const CACHE_TTL = 10 * 60 * 1000

	const hasCache = global.allGroupsCache && Object.keys(global.allGroupsCache).length > 0
	const isFresh = hasCache && global.allGroupsCacheTime && (Date.now() - global.allGroupsCacheTime) < CACHE_TTL

	if (isFresh) {
		// Cache masih fresh — return instan
		return global.allGroupsCache
	}

	if (hasCache) {
		// Cache ada tapi expired — return langsung, refresh di background
		if (!global._groupCacheRefreshing) {
			global._groupCacheRefreshing = true
			conn.groupFetchAllParticipating()
				.then(groups => {
					global.allGroupsCache = groups
					global.allGroupsCacheTime = Date.now()
					console.log(`[CACHE] Grup di-refresh background: ${Object.keys(groups).length} grup`)
				})
				.catch(e => console.error('[CACHE] Background refresh gagal:', e?.message || e))
				.finally(() => { global._groupCacheRefreshing = false })
		}
		return global.allGroupsCache
	}

	// Tidak ada cache sama sekali — harus fetch (pertama kali / setelah restart)
	const groups = await Promise.race([
		conn.groupFetchAllParticipating(),
		new Promise((_, rej) => setTimeout(() => rej(new Error('groupFetchAllParticipating timeout')), 20000))
	])
	global.allGroupsCache = groups
	global.allGroupsCacheTime = Date.now()
	return groups
}

// [PATCH A] Hentikan seluruh SIDE-EFFECT yang terikat pada satu sesi koneksi.
// Dipanggil segera saat 'close' agar timer/interval/broadcast tidak terus
// bekerja memakai socket yang sudah mati selama jeda reconnect.
// Idempotent: aman dipanggil berkali-kali.
function stopConnectionScopedWork() {
	// Tandai koneksi tidak aktif lebih dulu supaya getLiveConn() langsung null.
	global._connAlive = false

	// Sinyal stop untuk seluruh broadcast loop (jpm, swgc, pushkontak).
	// stoppush sebelumnya TIDAK pernah di-set saat disconnect (root cause T2-12).
	global.stopjpm = true
	global.stopswgc = true
	global.stoppush = true

	// Bersihkan timer readiness (connection-scoped).
	try { if (global._botReadyTimer) { clearTimeout(global._botReadyTimer); global._botReadyTimer = null } } catch {}

	// Hentikan interval autopromo (root cause T2-03).
	try { if (global.intervalPromo) { clearInterval(global.intervalPromo); global.intervalPromo = null } } catch {}

	// Bersihkan seluruh timeout sesi game (root cause T2-04).
	try {
		if (global._gameSessions && typeof global._gameSessions === 'object') {
			for (const chatId of Object.keys(global._gameSessions)) {
				const gs = global._gameSessions[chatId]
				try { if (gs && gs.timeout) clearTimeout(gs.timeout) } catch {}
				delete global._gameSessions[chatId]
			}
		}
	} catch {}
}

// [PATCH A] Buang instance koneksi lama secara menyeluruh SEBELUM membangun yang baru.
// Menaikkan epoch (mematikan handler lama), menghentikan side-effect, menutup
// websocket lama, dan melepas referensi global agar GC dapat mengklaim
// socket + store lama (root cause RC-1: memory leak, listener yatim, socket hantu).
// Idempotent & aman untuk panggilan pertama (belum ada koneksi).
function disposeConnectionScope(reason = '') {
	// Naikkan epoch: seluruh handler socket lama seketika menjadi stale/no-op.
	_connEpoch++

	stopConnectionScopedWork()

	const oldConn = global._nxlConn
	const oldStore = global.store

	if (oldConn) {
		try {
			if (typeof oldConn.end === 'function') oldConn.end(undefined)
			else if (oldConn.ws && typeof oldConn.ws.close === 'function') oldConn.ws.close()
		} catch {}
	}

	// Lepas referensi eksternal agar cycle socket-ev-listener dan store-ev dapat di-GC.
	if (global._nxlConn === oldConn) global._nxlConn = null
	if (global.store === oldStore) global.store = null

	if (reason) console.log(chalk.gray(`[LIFECYCLE] Dispose koneksi lama (epoch ${_connEpoch}): ${reason}`))
}

function scheduleReconnect(label = '') {
	if (_isConnecting) return
	if (_reconnectTimer) clearTimeout(_reconnectTimer)
	console.log(chalk.yellow(`[RECONNECT] ${label} — retry dalam ${_reconnectDelay / 1000}s...`))
	_reconnectTimer = setTimeout(() => {
		_reconnectTimer = null
		startingBot()
	}, _reconnectDelay)

	_reconnectDelay = Math.min(_reconnectDelay * 2, 60000)
}

async function startingBot() {
	if (_isConnecting) return
	_isConnecting = true
	_connectingSince = Date.now()
	markActivity()

	// [PATCH A] Buang koneksi lama SEBELUM membangun yang baru.
	// Ini juga menaikkan epoch, sehingga socket yang akan dibuat di bawah
	// mendapat identitas baru (myEpoch) dan handler socket lama menjadi stale.
	disposeConnectionScope('startingBot: membangun sesi baru')
	const myEpoch = _connEpoch

	const store = makeInMemoryStore({ logger: pino().child({ level: 'silent', stream: 'store' }) })

	// [PERF] Paralel: fetch WA version (network) + load auth state (disk I/O) bersamaan.
	// Sebelumnya sequential (~200-5200ms total), kini overlap (~maks(version,auth)).
	let version = [2, 3000, 1035194821]
	const [_versionResult, { state, saveCreds }] = await Promise.all([
		(async () => {
			try {
				const res = await Promise.race([
					fetch('https://raw.githubusercontent.com/WhiskeySockets/Baileys/master/src/Defaults/baileys-version.json'),
					new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 5000))
				])
				const json = await res.json()
				if (Array.isArray(json.version)) return json.version
			} catch {}
			return null
		})(),
		useMultiFileAuthState('./session')
	])
	if (_versionResult) version = _versionResult

	const NXL = WAConnection({
		version,

		browser: Browsers.ubuntu('Chrome'),
		getMessage: async (key) => {
			if (store) {
				const msg = await store.loadMessage(key.remoteJid, key.id, undefined)
				return msg?.message || undefined
			}
			return { conversation: 'hallo' }
		},
		printQRInTerminal: !pairingCode,
		generateHighQualityLinkPreview: true,
		logger: pino({ level: 'silent' }),
		auth: {
			creds: state.creds,

			keys: makeCacheableSignalKeyStore(state.keys, pino({ level: 'silent' }))
		},

		keepAliveIntervalMs: 15000,

		retryRequestDelayMs: 2000,
	})

	if (pairingCode && !NXL.authState.creds.registered) {
		console.log(chalk.yellow('\n𖦹 NOMOR : (contoh: 628xxx)'))
		let phoneNumber = await question('')
		phoneNumber = phoneNumber.replace(/[^0-9]/g, '').trim()
		let code = await NXL.requestPairingCode(phoneNumber)
		code = code?.match(/.{1,4}/g)?.join(' - ') || code
		console.log(chalk.magenta.italic('Kode Pairing Kamu :'), chalk.white.bold(code))
	}

NXL.ev.on('creds.update', saveCreds)
  NXL.sendText = (jid, text, quoted = '', options) => NXL.sendMessage(jid, {
    text: text,
    ...options
  }, {
    quoted,
    ...options
  })
  NXL.sendTextWithMentions = async (jid, text, quoted, options = {}) => NXL.sendMessage(jid, {
    text: text,
    mentions: [...text.matchAll(/@(\d{0,16})/g)].map(v => v[1] + '@s.whatsapp.net'),
    ...options
  }, {
    quoted
  })
  
  
NXL.ev.on('connection.update', async (update) => {
		markActivity()
		const { connection, lastDisconnect, receivedPendingNotifications } = update

		if (connection === 'close') {
			_isConnecting = false
			_connectingSince = null
			markActivity()
			const err = lastDisconnect?.error
			const reason = new Boom(err)?.output?.statusCode

			// [PATCH A] Hentikan seluruh side-effect connection-scoped seketika
			// (flags broadcast, autopromo interval, game timeout, botReady timer,
			//  tandai koneksi tidak aktif) agar tidak bekerja memakai socket mati.
			stopConnectionScopedWork()

			if (reason === DisconnectReason.loggedOut) {
				console.log(chalk.red('[LOGOUT] Sesi habis. Hapus folder session/ dan scan ulang.'))
				exec('rm -rf ./session/*')
				process.exit(1)
			} else if (reason === DisconnectReason.Multidevicemismatch) {
				console.log(chalk.red('[MISMATCH] Multi-device mismatch. Hapus session dan scan ulang.'))
				exec('rm -rf ./session/*')
				process.exit(0)
			} else {

				const label = {
					[DisconnectReason.connectionLost]    : 'Connection Lost',
					[DisconnectReason.connectionClosed]  : 'Connection Closed',
					[DisconnectReason.restartRequired]   : 'Restart Required',
					[DisconnectReason.timedOut]          : 'Timed Out',
					[DisconnectReason.badSession]        : 'Bad Session',
					[DisconnectReason.connectionReplaced]: 'Connection Replaced',
				}[reason] || `Unknown (${reason})`
				scheduleReconnect(label)
			}
		}

		if (connection === 'open') {

			_reconnectDelay = 3000
			_isConnecting = false
			_connectingSince = null
			markActivity()

			// [PATCH A] Koneksi resmi aktif — getLiveConn() mulai mengembalikan socket ini.
			global._connAlive = true

			global.stopjpm = false
			global.stopswgc = false
			global.stoppush = false

			global.botReady = false

			global.allGroupsCacheTime = 0

			global._cachedBotNumber    = null
			global._cachedCreatorJids  = null
			global._cachedPremiumJids  = null
			global._qrisCache          = null
			console.log(chalk.green.bold(`[CONNECTED] NXL Bot Online ✓`))

			if (global._botReadyTimer) clearTimeout(global._botReadyTimer)
			global._botReadyTimer = setTimeout(() => {
				if (!global.botReady) {
					global.botReady = true
					console.log(chalk.cyan('[INFO] Bot siap menerima perintah JPM. (fallback 20s)'))

					prefetchAllGroups(NXL)
				}
			}, 20000)
		}

		if (receivedPendingNotifications === true) {
			console.log(chalk.cyan('[INFO] Menerima pending messages, harap tunggu...'))

			if (global._botReadyTimer) clearTimeout(global._botReadyTimer)
			global._botReadyTimer = setTimeout(() => {
				global.botReady = true
				console.log(chalk.cyan('[INFO] Bot siap menerima perintah JPM.'))

				prefetchAllGroups(NXL)
			}, 3000)
		}
	});

store.bind(NXL.ev)

global.store = store
global._nxlConn = NXL

global.sleep = (ms) => new Promise(r => setTimeout(r, ms))

global.getGroupsCached = () => getGroupsCached(NXL)

global.prefetchAllGroups = () => prefetchAllGroups(NXL)

try {
	const savedMode = JSON.parse(fs.readFileSync('./database/botmode.json', 'utf-8'))
	NXL.public = typeof savedMode.public === 'boolean' ? savedMode.public : global.NXL !== 'self'
	console.log(chalk.cyan(`[MODE] Bot dimulai dalam mode: ${NXL.public ? 'PUBLIC' : 'SELF'} (dari database/botmode.json)`))
} catch {

	NXL.public = global.NXL !== 'self'
	console.log(chalk.cyan(`[MODE] Bot dimulai dalam mode: ${NXL.public ? 'PUBLIC' : 'SELF'} (dari settings.js)`))
}

await Solving(NXL, store)

NXL.ev.on('messages.upsert', async (message) => {
  markActivity()
  // [FIX H1] Antilink processing dipindahkan sepenuhnya ke case.js
  // untuk menghindari double-processing (duplicate delete/kick/warning)
  await MessagesUpsert(NXL, message, store);
});

NXL.ev.on('contacts.update', (update) => {
		for (let contact of update) {
			let id = NXL.decodeJid(contact.id)
			if (store && store.contacts) {
				const existing = store.contacts[id] || {}
				store.contacts[id] = { ...existing, id, name: contact.notify }

				if (contact.lid) store.contacts[id].lid = contact.lid
			}
		}
});

NXL.ev.on('group-participants.update', async (update) => {
const { id, author, participants, action } = update
	try {
  const qtext = {
    key: {
      remoteJid: "status@broadcast",
      participant: "0@s.whatsapp.net"
    },
    message: {
      "extendedTextMessage": {
        "text": "[ 𝗚𝗿𝗼𝘂𝗽 𝗡𝗼𝘁𝗶𝗳𝗶𝗰𝗮𝘁𝗶𝗼𝗻 ]"
      }
    }
  }

  const welcomeList = global.cache?.welcome || []
  if (welcomeList.includes(id)) {
    const metadata = await NXL.groupMetadata(id)
    let teks
    for(let n of participants) {
      let profile;
      try {
        profile = await NXL.profilePictureUrl(n, 'image');
      } catch {
        profile = 'https://telegra.ph/file/95670d63378f7f4210f03.png';
      }
      let imguser = await prepareWAMessageMedia({
        image: {
          url: profile
        }
      }, {
        upload: NXL.waUploadToServer
      })
      if(action == 'add') {
        teks = author === n ? `@${n.split('@')[0]} join via *link group*` : `@${author.split("@")[0]} telah *menambahkan* @${n.split('@')[0]} kedalam grup`
        let asu = await NXL.sendMessage(id, {
          text: `${teks}`,
          mentions: [author, n]
        }, {
          quoted: qtext
        })
await NXL.relayMessage(id, {
  "productMessage": {
    "product": {
      "productImage": imguser.imageMessage,
      "productId": "343056591714248",
      "title": "Welcome To Group",
      "description": `Selamat datang @${n.split('@')[0]}`,
      "productImageCount": 1
    },
    "businessOwnerJid": "6283197813991@s.whatsapp.net",
    "contextInfo": {
      mentionedJid: [n]
    }
  }
}, {})
      } else if(action == 'remove') {
        teks = author == n ? `@${n.split('@')[0]} telah *keluar* dari grup` : author !== n ? `@${author.split("@")[0]} telah *mengeluarkan* @${n.split('@')[0]} dari grup` : ""
        let asu = await NXL.sendMessage(id, {
          text: `${teks}`,
          mentions: [author, n]
        }, {
          quoted: qtext
        })
        await NXL.relayMessage(id, {
  "productMessage": {
    "product": {
      "productImage": imguser.imageMessage,
      "productId": "343056591714248",
      "title": "Leaving To Group",
      "description": `Selamat tinggal @${n.split("@")[0]}`,
      "productImageCount": 1
    },
    "businessOwnerJid": "6283197813991@s.whatsapp.net",
    "contextInfo": {
      mentionedJid: [n]
    }
  }
}, {})
      } else if(action == 'promote') {
        teks = author == n ? `@${n.split('@')[0]} telah *menjadi admin* grup ` : author !== n ? `@${author.split("@")[0]} telah *menjadikan* @${n.split('@')[0]} sebagai *admin* grup` : ""
        let asu = await NXL.sendMessage(id, {
          text: `${teks}`,
          mentions: [author, n]
        }, {
          quoted: qtext
        })
        await NXL.relayMessage(id, {
  "productMessage": {
    "product": {
      "productImage": imguser.imageMessage,
      "productId": "343056591714248",
      "title": "Promote Member",
      "description": `Promote member @${n.split("@")[0]}`,
      "productImageCount": 1
    },
    "businessOwnerJid": "6283197813991@s.whatsapp.net",
    "contextInfo": {
      mentionedJid: [n]
    }
  }
}, {})
      } else if(action == 'demote') {
        teks = author == n ? `@${n.split('@')[0]} telah *berhenti* menjadi *admin*` : author !== n ? `@${author.split("@")[0]} telah *menghentikan* @${n.split('@')[0]} sebagai *admin* grup` : ""
        let asu = await NXL.sendMessage(id, {
          text: `${teks}`,
          mentions: [author, n]
        }, {
          quoted: qtext
        })
        await NXL.relayMessage(id, {
  "productMessage": {
    "product": {
      "productImage": imguser.imageMessage,
      "productId": "343056591714248",
      "title": "Demote Member",
      "description": `Demote member @${n.split("@")[0]}`,
      "productImageCount": 1
    },
    "businessOwnerJid": "6283197813991@s.whatsapp.net",
    "contextInfo": {
      mentionedJid: [n]
    }
  }
}, {})
      }
    }
  }
} catch (e) {}
});

NXL.sendImageAsSticker = async (jid, path, quoted, options = {}) => {
const { imageToWebp, writeExifImg } = _requireSticker()
let buff = Buffer.isBuffer(path) ? path : /^data:.*?\/.*?;base64,/i.test(path) ? Buffer.from(path.split`,`[1], 'base64') : /^https?:\/\//.test(path) ? await (await getBuffer(path)) : fs.existsSync(path) ? fs.readFileSync(path) : Buffer.alloc(0)
let buffer
if (options && (options.packname || options.author)) {
buffer = await writeExifImg(buff, options)
} else {
buffer = await imageToWebp(buff)}
await NXL.sendMessage(jid, { sticker: { url: buffer }, ...options }, { quoted })
return buffer}

NXL.sendImageAsStickerAV = async (jid, path, quoted, options = {}) => {
const { imageToWebp2, writeExifImgAV } = _requireSticker()
let buff = Buffer.isBuffer(path) ? path : /^data:.*?\/.*?;base64,/i.test(path) ? Buffer.from(path.split`,`[1], 'base64') : /^https?:\/\//.test(path) ? await (await getBuffer(path)) : fs.existsSync(path) ? fs.readFileSync(path) : Buffer.alloc(0)
let buffer
if (options && (options.packname || options.author)) {
buffer = await writeExifImgAV(buff, options)
} else {
buffer = await imageToWebp2(buff)}
await NXL.sendMessage(jid, { sticker: { url: buffer }, ...options }, { quoted })
return buffer}

NXL.sendImageAsStickerAvatar = async (jid, path, quoted, options = {}) => {
const { imageToWebp3, writeExifImg } = _requireSticker()
let buff = Buffer.isBuffer(path) ? path : /^data:.*?\/.*?;base64,/i.test(path) ? Buffer.from(path.split`,`[1], 'base64') : /^https?:\/\//.test(path) ? await (await getBuffer(path)) : fs.existsSync(path) ? fs.readFileSync(path) : Buffer.alloc(0)
let buffer
if (options && (options.packname || options.author)) {
buffer = await writeExifImg(buff, options)
} else {
buffer = await imageToWebp3(buff)}
await NXL.sendMessage(jid, { sticker: { url: buffer }, ...options }, { quoted })
return buffer}

NXL.sendVideoAsSticker = async (jid, path, quoted, options = {}) => {
const { videoToWebp, writeExifVid } = _requireSticker()
let buff = Buffer.isBuffer(path) ? path : /^data:.*?\/.*?;base64,/i.test(path) ? Buffer.from(path.split`,`[1], 'base64') : /^https?:\/\//.test(path) ? await (await getBuffer(path)) : fs.existsSync(path) ? fs.readFileSync(path) : Buffer.alloc(0)
let buffer
if (options && (options.packname || options.author)) {
buffer = await writeExifVid(buff, options)
} else {
buffer = await videoToWebp(buff)}
await NXL.sendMessage(jid, { sticker: { url: buffer }, ...options }, { quoted })
return buffer}

if (!global.anticallWarnings) global.anticallWarnings = {}

NXL.ev.on('call', async (calls) => {
  try {
    if (!global.anticallgcList) {
      try {
        global.anticallgcList = JSON.parse(fs.readFileSync('./database/anticallgc.json', 'utf8'))
      } catch {
        global.anticallgcList = []
      }
    }
    if (!global.anticallWarnings) global.anticallWarnings = {}

    const anticallgcList = global.anticallgcList

    const botJid = NXL.decodeJid ? NXL.decodeJid(NXL.user.id) : (NXL.user.id.split(':')[0] + '@s.whatsapp.net')
    const ownerNums = (global.cache?.owner || []).map(v => v.replace(/[^0-9]/g, '') + '@s.whatsapp.net')

    for (const call of calls) {
      const callerId = NXL.decodeJid ? NXL.decodeJid(call.from) : call.from

      if (!call.isGroup) continue
      if (call.status && !['offer', 'ringing'].includes(call.status)) continue

      try { await NXL.rejectCall(call.id, callerId) } catch {}

      if (typeof areJidsSameUser === 'function' ? areJidsSameUser(callerId, botJid) : callerId === botJid) continue
      if (ownerNums.includes(callerId)) continue

      let groupId = null
      let groupMeta = null

      for (const gid of anticallgcList) {
        try {
          const meta = await NXL.groupMetadata(gid).catch(() => null)

          const inGroup = meta?.participants?.some(p =>
            (typeof areJidsSameUser === 'function' ? areJidsSameUser(p.id, callerId) : p.id === callerId)
          )
          if (inGroup) {
            groupId = gid
            groupMeta = meta
            break
          }
        } catch {}
      }

      if (!groupId || !groupMeta) continue

      try { groupMeta = await NXL.groupMetadata(groupId).catch(() => groupMeta) } catch {}

      const participants = groupMeta?.participants || []

      const groupAdmins = participants.filter(p => p.admin).map(p => p.id) || []
      const isAdmin = groupAdmins.some(adminId =>
        typeof areJidsSameUser === 'function' ? areJidsSameUser(adminId, callerId) : adminId === callerId
      )

      if (isAdmin) continue

      const isBotAdmin = participants.some(p =>
        ((p.id && (typeof areJidsSameUser === 'function' ? areJidsSameUser(p.id, botJid) : p.id === botJid)) ||
        (p.lid && p.lid === p.id)) && p.admin
      )

      const callerNum = callerId.split('@')[0]
      const timeNow = new Date().toLocaleTimeString('id-ID', { timeZone: 'Asia/Jakarta' })
      const warnKey = `${groupId}__${callerId}`

      if (!global.anticallWarnings[warnKey]) global.anticallWarnings[warnKey] = 0
      global.anticallWarnings[warnKey]++

      const warnCount = global.anticallWarnings[warnKey]

      if (warnCount === 1) {

        await NXL.sendMessage(groupId, {
          text:
            `📵 *ANTI CALL GRUP AKTIF*\n\n` +
            `⚠️ @${callerNum} melakukan panggilan di grup ini!\n\n` +
            `🚫 *Panggilan di grup dilarang.*\n` +
            `⏰ Waktu: ${timeNow}\n\n` +
            `⚠️ *Ini peringatan ke-1.* Jika melakukan panggilan lagi, kamu akan dikick!\n\n` +
            `_Harap patuhi peraturan grup!_`,
          mentions: [callerId]
        })
      } else if (warnCount >= 2) {

        await NXL.sendMessage(groupId, {
          text:
            `🚨 *KICK OTOMATIS*\n\n` +
            `@${callerNum} telah melakukan panggilan untuk ke-${warnCount} kalinya!\n\n` +
            `🚫 *Dikeluarkan dari grup.*\n` +
            `⏰ Waktu: ${timeNow}`,
          mentions: [callerId]
        })

        if (isBotAdmin) {
          try {
            await NXL.groupParticipantsUpdate(groupId, [callerId], 'remove')
            delete global.anticallWarnings[warnKey]
          } catch (err) {
            console.error('Gagal mengeksekusi kick:', err)
          }
        } else {
          await NXL.sendMessage(groupId, {
            text: `⚠️ Bot tidak bisa kick @${callerNum} karena bot bukan admin grup.`,
            mentions: [callerId]
          })
        }
      }
    }
  } catch (e) {
    console.error('[ANTICALLGC ERROR]', e?.message || e)
  }
})
NXL.ev.on('groups.update', async (update) => {
		try {

		if (global.notifGrup === false) return

		const data = update[0]
		const qtext = {
    key: {
      remoteJid: "status@broadcast",
      participant: "0@s.whatsapp.net"
    },
    message: {
      "extendedTextMessage": {
        "text": "[ 𝗚𝗿𝗼𝘂𝗽 𝗡𝗼𝘁𝗶𝗳𝗶𝗰𝗮𝘁𝗶𝗼𝗻 ]"
      }
    }
  }
		if (data?.inviteCode) {
		let botNumber = NXL.user.id.split(":")[0]
		let participant = data.author
		if (participant.split("@")[0] === botNumber) return
  await NXL.sendMessage(data.id, {text: `@${participant.split("@")[0]} telah *mereset* link grup`, mentions: [participant]}, {quoted: qtext})
		}

		if (data?.desc) {
		let botNumber = NXL.user.id.split(":")[0]
		let participant = data.author
		if (participant.split("@")[0] === botNumber) return
		await NXL.sendMessage(data.id, {text: `@${participant.split("@")[0]} telah *memperbarui* deskripsi grup`, mentions: [participant]}, {quoted: qtext})
		}

		if (data?.subject) {
		let botNumber = NXL.user.id.split(":")[0]
		let participant = data.author
		if (participant.split("@")[0] === botNumber) return
		await NXL.sendMessage(data.id, {text: `@${participant.split("@")[0]} telah *mengganti* nama grup`, mentions: [participant]}, {quoted: qtext})
		}

		} catch (e) {
		}
});

     NXL.downloadAndSaveMediaMessage = async (message, filename, attachExtension = true) => {
        const quoted = message.msg ? message.msg : message;
        const mime = (message.msg || message).mimetype || "";
        const messageType = message.mtype ? message.mtype.replace(/Message/gi, "") : mime.split("/")[0];
        const Randoms = Date.now();
        const fil = Randoms;

        const stream = await downloadContentFromMessage(quoted, messageType);
        let buffer = Buffer.from([]);
        for await (const chunk of stream) {
            buffer = Buffer.concat([buffer, chunk]);
        }

        const type = await FileType.fromBuffer(buffer);

        if (!fs.existsSync('./Tmp')) fs.mkdirSync('./Tmp', { recursive: true })
        const trueFileName = attachExtension ? `./Tmp/${fil}.${type.ext}` : filename;
        fs.writeFileSync(trueFileName, buffer);

        return trueFileName;
    };

return NXL

}

startingBot()

setInterval(() => {
	// [PATCH C] FIFO eviction (bukan clear-all) & threshold konsisten dengan case.js (500/400).
	// clear-all bisa menghapus ID pesan aktif -> celah double-execute (T2-08/F1-01).
	if (global._processedMsgIds && global._processedMsgIds.size > 500) {
		const _rm = global._processedMsgIds.size - 400
		let _i = 0
		for (const _old of global._processedMsgIds) {
			if (_i++ >= _rm) break
			global._processedMsgIds.delete(_old)
		}
	}
	if (global.groupCacheTime) {
		const now = Date.now()
		const TTL = (global.GROUP_CACHE_TTL || 300000) * 2
		for (const jid of Object.keys(global.groupCacheTime)) {
			if (now - global.groupCacheTime[jid] > TTL) {
				delete global.groupCache?.[jid]
				delete global.groupCacheTime[jid]
			}
		}
	}
	if (typeof global.gc === 'function') global.gc()
}, 10 * 60 * 1000)

setInterval(() => {
	if (global.botReady && global._nxlConn) {

		if (global.statusjpm) {
			global.pendingGroupsRefresh = true
			return
		}
		const age = Date.now() - (global.allGroupsCacheTime || 0)
		if (age > 6 * 60 * 60 * 1000) {
			prefetchAllGroups(global._nxlConn).catch(() => {})
		}
	}
}, 6 * 60 * 60 * 1000)

process.on('uncaughtException', (err) => {
	console.error(chalk.red('[UNCAUGHT]'), err?.message || err)
})
process.on('unhandledRejection', (reason) => {
	console.error(chalk.red('[UNHANDLED]'), reason?.message || reason)
})

