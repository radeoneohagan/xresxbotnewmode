/**
 * lib/upswStatus.js
 * -----------------------------------------------------------------------------
 * Single source of truth untuk upload STATUS WhatsApp (status@broadcast).
 *
 * Logika di sini dipindahkan VERBATIM dari implementasi .upsw yang sudah
 * diperbaiki (case 'upsw'/'upstatus'/'upstatuswa'/'uploadsw' di case.js) agar:
 *   - Tidak ada duplikasi kode audience (statusJidList).
 *   - .done1–.done10 dapat me-reuse implementasi .upsw yang sama persis.
 *
 * [UPSW AUDIENCE] Sumber audience = kontak WhatsApp tersinkronisasi
 * (global.store.contacts) yang mencerminkan buku kontak akun bot — PALING
 * mendekati perilaku aplikasi WA resmi. Difilter HANYA ke JID
 * @s.whatsapp.net (buang @lid, @g.us, dan device-suffix ':') untuk
 * menghindari bug Baileys/Wileys "No sessions".
 */

/**
 * Bangun statusJidList dari kontak WA tersinkronisasi (PN-only) + nomor bot.
 * @param {object} NXL - socket utama (dipakai untuk user id / decodeJid)
 * @returns {string[]} daftar JID unik untuk statusJidList
 */
function buildStatusJidList(NXL) {
  let statusJidList = []
  try {
    const _contacts = (global.store && global.store.contacts) ? global.store.contacts : {}
    statusJidList = Object.keys(_contacts).filter(jid =>
      typeof jid === 'string' &&
      jid.endsWith('@s.whatsapp.net') &&
      !jid.includes(':')
    )
  } catch { statusJidList = [] }
  // Selalu sertakan nomor bot sendiri agar bot dapat melihat statusnya.
  try {
    const _botSelf = NXL && NXL.user && NXL.user.id ? NXL.decodeJid(NXL.user.id) : null
    if (_botSelf && _botSelf.endsWith('@s.whatsapp.net') && !statusJidList.includes(_botSelf)) {
      statusJidList.push(_botSelf)
    }
  } catch {}
  return [...new Set(statusJidList)]
}

/**
 * Upload satu postingan status memakai logika audience .upsw yang sudah diperbaiki.
 *
 * @param {object} NXL       - socket utama (untuk user id / decodeJid).
 * @param {object} content   - konten pesan Baileys ({image|video|audio|text}).
 * @param {object} extraOpts - opsi tambahan yang di-merge ke opsi status.
 * @returns {Promise<any>} hasil conn.sendMessage
 */
async function uploadStatusWA(NXL, content, extraOpts = {}) {
  // [UPSW] Pakai socket hidup (Patch A) agar aman setelah reconnect; tidak
  // memakai runBroadcast (Patch B) karena ini SATU status post, bukan fan-out.
  const conn = (typeof global.getLiveConn === 'function' && global.getLiveConn()) || NXL

  const statusJidList = buildStatusJidList(NXL)

  const opts = {
    backgroundColor: '#000000',
    font: 1,
    ...extraOpts
  }
  // Hanya set statusJidList bila ada kontak valid. Jika kosong (mis. kontak belum
  // tersinkronisasi tepat setelah boot), biarkan Baileys memakai default-nya agar
  // upload tetap berhasil tanpa "No sessions".
  if (statusJidList.length > 0) opts.statusJidList = statusJidList

  return conn.sendMessage('status@broadcast', content, opts)
}

module.exports = { buildStatusJidList, uploadStatusWA }
