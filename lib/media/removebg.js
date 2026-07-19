const fs = require('fs')
const path = require('path')
const os = require('os')

/**
 * Remove background dari buffer gambar, 100% lokal (tidak ada request ke luar).
 * Menggunakan @imgly/background-removal-node (model ONNX, jalan di CPU).
 *
 * Install dulu:
 *   npm install @imgly/background-removal-node
 *
 * Catatan:
 * - Pertama kali dipanggil, library ini akan download model ONNX (~beberapa MB)
 *   dan menyimpannya di cache lokal (node_modules/.cache atau sesuai config).
 *   Setelah itu, model dipakai dari cache — tidak perlu koneksi lagi.
 * - Proses berjalan di CPU, jadi tergantung spek server. Untuk gambar besar
 *   bisa memakan beberapa detik.
 */
async function removeBg(buffer) {
  let tmpInputPath
  try {
    const { removeBackground } = require('@imgly/background-removal-node')

    // Library ini menerima Buffer/Blob/File/URL. Paling stabil pakai file path
    // via Blob dari buffer langsung (hindari I/O disk kalau versi library support).
    let blob
    try {
      // Node 18+ punya global Blob
      blob = new Blob([buffer])
    } catch {
      // fallback: tulis ke tmp file kalau Blob tidak tersedia
      tmpInputPath = path.join(os.tmpdir(), `rbg_in_${Date.now()}.jpg`)
      fs.writeFileSync(tmpInputPath, buffer)
      blob = tmpInputPath
    }

    const resultBlob = await removeBackground(blob)

    // resultBlob adalah Blob (punya .arrayBuffer())
    const arrayBuffer = await resultBlob.arrayBuffer()
    const outBuffer = Buffer.from(arrayBuffer)

    if (!outBuffer || outBuffer.length < 100) {
      throw new Error('Hasil remove background kosong/tidak valid.')
    }

    return outBuffer
  } catch (err) {
    throw new Error(`Gagal menghapus background: ${err.message}`)
  } finally {
    if (tmpInputPath) {
      try { fs.unlinkSync(tmpInputPath) } catch {}
    }
  }
}

module.exports = { removeBg }
