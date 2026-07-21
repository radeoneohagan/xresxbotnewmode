/* ============================================================
 * BASE ORI FANNYFA
 * ============================================================
 * BY      : FannyFa
 * VERSION : 1.0
 * BOT     : NXL
 * KODE SC : 11246867678989 ✓
 *
 * CHAT    : 6283171084295
 * SUPPORT : 625624179853
 *
 * THANKS TO:
 *  - FannyFa  [ Developer ]
 *
 * NOTE:
 *  - Yang ori yang ada kode SC-nya ⚙️
 *  - Kami tidak bertanggung jawab atas semua kesalahan
 * ============================================================ */

const fs = require('fs')

// ── PESAN SISTEM ─────────────────────────────────────────────
global.mess = {
  owner   : 'Fitur ini hanya untuk ownerbot.',
  group   : 'Fitur ini hanya dapat digunakan ketika bot berada di dalam grup.',
  private : 'Fitur ini hanya dapat digunakan ketika bot berada di private chat.',
  admin   : 'Fitur ini hanya dapat digunakan admin grup.',
  botadmin: 'Fitur ini hanya dapat digunakan ketika bot menjadi admin grup.',
  wait    : 'Sedang memproses, harap tunggu...',
}


// ── SETTING BOT ───────────────────────────────────────────────
global.prefa      = ['', '!', '.', ',', '🐤', '🗿']
global.ownername  = 'XRESX DIGITAL STORE'
global.namabot    = '*PT SONTOLOYO*'
global.owner      = ['6287728163189']
global.wm         = 'PT SONTOLOYO'
global.versibot   = '2.0'
global.prefix     = '.'
global.NXL        = `` // default mode — akan di-override oleh botmode.json di index.js
global.botMode = `Case`
// ── LINK & URL ────────────────────────────────────────────────
global.url          = ''
global.chjid        = ''
global.urlc1        = ''
global.urlc2        = ''
global.linktambahan = 'vpnlancar.pakde-premium.xyz'
global.linkweb      = 'vpnlancar.pakde-premium.xyz'
global.linkchannel  = 'https://whatsapp.com/channel/0029VadYQe7G3R3g3WecRM0h'
global.idsal = '120363275694082356@newsletter'
global.linkGrup     = 'https://chat.whatsapp.com/CrYDg5Az8Ol1XidwvAFRoO'
global.linkpreview  = 'https://raw.githubusercontent.com/belluptaka/dat3/main/uploads/bb3ff5-1771233100633.jpg'


// ── BASE URL API (PIHAK KE-3) ─────────────────────────────────
// Hanya base domain, path/query tetap ditulis manual di case.js
global.apiGroq         = 'https://api.groq.com'
global.apiAskai        = 'https://askai.free'
global.apiImgbb        = 'https://api.imgbb.com'
global.apiFreeimage     = 'https://freeimage.host'
global.apiTmpfiles      = 'https://tmpfiles.org'
global.apiUguu          = 'https://uguu.se'
global.apiSiputzx       = 'https://api.siputzx.my.id'
global.apiSiputzxCors   = 'https://cors.siputzx.my.id'
global.apiFaa           = 'https://api-faa.my.id'
global.apiPollinations  = 'https://image.pollinations.ai'
global.apiLexcode       = 'https://api.lexcode.biz.id'
global.apiSavetubeVip   = 'https://media.savetube.vip'
global.apiSavetubeMe    = 'https://yt.savetube.me'
global.apiTikwm         = 'https://tikwm.com'
global.apiTraceMoe      = 'https://api.trace.moe'
global.apiFastdl        = 'https://api-wh.fastdl.app'
global.webFastdl        = 'https://fastdl.app'
global.apiSfile         = 'https://sfile.co'
global.apiTenor         = 'https://tenor.googleapis.com'
global.apiNekosBest     = 'https://nekos.best'
global.apiNekosapi      = 'https://api.nekosapi.com'
global.apiPurrbot       = 'https://purrbot.site'
global.apiPurrbotV2     = 'https://api.purrbot.site'
global.apiMyquran       = 'https://api.myquran.com'
global.apiLibur         = 'https://libur.deno.dev'
global.apiBmkg          = 'https://data.bmkg.go.id'
global.apiOpenweather   = 'https://api.openweathermap.org'
global.apiSavetwitter   = 'https://savetwitter.net'
global.apiRoblox        = 'https://users.roblox.com'
global.apiNpmRegistry   = 'https://registry.npmjs.org'
global.apiScreenshot    = 'https://api.screenshotmachine.com'
global.apiThumIo        = 'https://image.thum.io'
global.apiSynoxCloud    = 'https://api.synoxcloud.xyz'
global.apiMemeApi       = 'https://meme-api.com'
global.apiHlgaming      = 'https://proapis.hlgamingofficial.com'
global.apiTelegraph     = 'https://telegra.ph'
global.cdnPixabay       = 'https://cdn.pixabay.com'
global.cdnYtimg         = 'https://i.ytimg.com'
global.waMe             = 'https://wa.me'
global.waChat           = 'https://chat.whatsapp.com'
global.waWeb            = 'https://whatsapp.com'
global.ytWeb            = 'https://www.youtube.com'
global.ytShort          = 'https://youtu.be'
global.igWeb            = 'https://www.instagram.com'
global.igShort          = 'https://instagram.com'
global.ttWeb            = 'https://www.tiktok.com'
global.threadsWeb       = 'https://threads.net'
global.xWeb             = 'https://x.com'
global.driveWeb         = 'https://drive.google.com'
global.githubWeb        = 'https://github.com'
global.googleWeb        = 'https://www.google.com'
global.storeNXLhost     = 'https://store.NXLhost.web.id'


// ── PAYMENT ───────────────────────────────────────────────────
global.qris       ="./lib/image/payment/qris.jpg"
global.dana      = ''
global.ovo       = ''
global.gopay     = ''
global.rek       = ''
global.storename = ''
global.thumbnail = ''


// ── STICKER ───────────────────────────────────────────────────
global.packname = 'Sticker By'
global.author   = 'PT SONTOLOYO'


// ── AI ────────────────────────────────────────────────────────
global.geminiapi = process.env.GEMINI_API_KEY || 'YOUR_GEMINI_API_KEY'
global.gris      = '`'

global.hfToken = process.env.HF_TOKEN || "YOUR_HF_TOKEN"

global.groqKey = process.env.GROQ_API_KEY || 'YOUR_GROQ_API_KEY'

global.promptCoding = `Kamu adalah NXL, asisten coding. Kalau diminta membuat code, balas HANYA dengan code saja tanpa penjelasan, tanpa markdown, tanpa backtick, tanpa komentar tambahan. Langsung tulis codenya saja.`

global.promptUmum = `Saya adalah NXL yang dirancang untuk membantu mahasiswa dalam pembahasan coding serta pelajaran umum seperti Matematika, Bahasa Indonesia, Bahasa Inggris, Fisika, Kimia, Rekayasa Perangkat Lunak, dan Basis Data dengan penjelasan yang mudah dipahami dan relevan`

// ── CACHE TTL & LIMIT ────────────────────────────────────────
global.GROUP_CACHE_TTL = 5 * 60 * 1000  // 5 menit — cache groupMetadata
global.limitawal       = 10              // limit command awal untuk user baru


// ── DELAY & TIMER ─────────────────────────────────────────────
global.JedaSwgc        = 5000
global.JedaPushkontak  = 5000
global.JedaJpm         = 4000


// ── ANTILINK FALLBACK ─────────────────────────────────────────
// [FIX #3] case.js pakai .includes/.push/.splice → harus Array, bukan Object {}
// Nilai ini hanya dipakai kalau file database/antilink.json belum ada
global.antilinkDefault  = []
global.antilink2Default = []

// ── SWGC ──────────────────────────────────────────────────────
global.autoSwgcContent = null
global.stopswgc        = false


// ── CPANEL / API ──────────────────────────────────────────────
global.domain  = 'https://fanny.dk-vpn.xyz'
global.apikey  = 'ptla_XlbPBxUEo9EH0iGOLGmku89PvvW1CueJNe4idphcZej'
global.capikey = 'ptlc_lo6p8PYtMTGdlOOIV3zFa3v2YDCORatakerpFLXgb9C'
global.eggs    = '15'
global.locc    = '1'


// ── LIST PANEL (Harga) ────────────────────────────────────────
global.listpanel = {
  1   : '1000',
  2   : '2000',
  3   : '3000',
  4   : '4000',
  5   : '5000',
  6   : '6000',
  7   : '7000',
  8   : '8000',
  9   : '9000',
  unli: '10000',
}


// ── (hot-reload dihapus untuk stabilitas production) ──
