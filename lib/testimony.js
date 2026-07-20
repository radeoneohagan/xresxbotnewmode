const { createCanvas } = require('@napi-rs/canvas')
const fs = require('fs')

function formatRupiah(num) {
  return 'Rp ' + num.toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.')
}

function maskPhoneNumber(numberOrJid) {
  if (!numberOrJid) return 'XXXXXXXXXXX'
  const digits = String(numberOrJid).replace(/@.*$/, '').replace(/[^0-9]/g, '')
  if (digits.length < 7) {
    return digits ? `${digits.slice(0, 2)}*****` : 'XXXXXXXXXXX'
  }
  const head = digits.slice(0, 5)
  const tail = digits.slice(-3)
  const maskedLen = Math.max(digits.length - head.length - tail.length, 3)
  return `${head}${'*'.repeat(maskedLen)}${tail}`
}

function stripBrandMarkdown(str) {
  if (!str) return str
  return String(str).replace(/\*+/g, '').trim()
}

function generateTestimonyImage(data) {
  const {
    server, durasi, perangkat, harga, tanggal, trx,
    namaPaket, aplikasiVpn, sshV2ray, customerNumber,
    ptName, brandName, ownerName
  } = data

  const customerDisplay = maskPhoneNumber(customerNumber)
  const ptDisplay = stripBrandMarkdown(ptName) || 'PT SONTOLOYO'
  // Brand pada gambar diambil dari global.ownername (via param brandName). Fallback
  // juga menunjuk ke global.ownername agar tidak ada brand hardcode di generator.
  const brandDisplay = stripBrandMarkdown(brandName) || stripBrandMarkdown(global.ownername) || 'Store'
  // Owner pada gambar diambil dari global.owner (via param ownerName). Fallback juga
  // menunjuk ke global.owner[0] agar nomor owner tidak hardcode di generator.
  const ownerDisplay = stripBrandMarkdown(ownerName) || ((global.owner && global.owner[0]) ? String(global.owner[0]) : '-')

  const width = 1000
  const height = 460
  const canvas = createCanvas(width, height)
  const ctx = canvas.getContext('2d')

  // === BACKGROUND (premium dark) ===
  // Gradient nyaris hitam dengan kedalaman diagonal halus → tampilan lebih
  // gelap, smooth, dan premium. Layout/ukuran/posisi elemen TIDAK berubah.
  const bgGrad = ctx.createLinearGradient(0, 0, width, height)
  bgGrad.addColorStop(0, '#05070f')
  bgGrad.addColorStop(0.45, '#0a0d1a')
  bgGrad.addColorStop(1, '#0d1120')
  ctx.fillStyle = bgGrad
  ctx.fillRect(0, 0, width, height)

  // Soft glow diffus (radial gradient) — jauh lebih halus dari lingkaran solid,
  // memberi kesan cahaya lembut premium. Memakai alpha pada warna (bukan
  // ctx.globalAlpha) agar state canvas tetap bersih untuk elemen berikutnya.
  const glowCyan = ctx.createRadialGradient(width - 80, 50, 0, width - 80, 50, 260)
  glowCyan.addColorStop(0, 'rgba(6, 182, 212, 0.14)')
  glowCyan.addColorStop(1, 'rgba(6, 182, 212, 0)')
  ctx.fillStyle = glowCyan
  ctx.fillRect(0, 0, width, height)

  const glowViolet = ctx.createRadialGradient(60, height - 40, 0, 60, height - 40, 240)
  glowViolet.addColorStop(0, 'rgba(139, 92, 246, 0.13)')
  glowViolet.addColorStop(1, 'rgba(139, 92, 246, 0)')
  ctx.fillStyle = glowViolet
  ctx.fillRect(0, 0, width, height)

  // Vignette lembut untuk kedalaman & fokus ke tengah kartu (aesthetic).
  const vignette = ctx.createRadialGradient(width / 2, height / 2, height * 0.3, width / 2, height / 2, width * 0.75)
  vignette.addColorStop(0, 'rgba(0, 0, 0, 0)')
  vignette.addColorStop(1, 'rgba(0, 0, 0, 0.38)')
  ctx.fillStyle = vignette
  ctx.fillRect(0, 0, width, height)

  // top accent line (framing kartu — warna dipertahankan, juga dipakai garis bawah)
  const accentGrad = ctx.createLinearGradient(0, 0, width, 0)
  accentGrad.addColorStop(0, '#06b6d4')
  accentGrad.addColorStop(0.5, '#8b5cf6')
  accentGrad.addColorStop(1, '#06b6d4')
  ctx.fillStyle = accentGrad
  ctx.fillRect(0, 0, width, 4)

  // === HEADER ROW: badge + brand (left)  |  total harga (right) ===
  const padX = 44
  let cursorY = 38

  // success badge (pill)
  const badgeText = 'TRANSAKSI BERHASIL'
  ctx.font = 'bold 14px sans-serif'
  const badgeTextW = ctx.measureText(badgeText).width
  const badgeW = badgeTextW + 76
  const badgeH = 34
  const badgeX = padX
  const badgeY = cursorY - 22

  const badgeGrad = ctx.createLinearGradient(badgeX, badgeY, badgeX + badgeW, badgeY)
  badgeGrad.addColorStop(0, '#059669')
  badgeGrad.addColorStop(1, '#10b981')
  ctx.fillStyle = badgeGrad
  roundRect(ctx, badgeX, badgeY, badgeW, badgeH, 17)
  ctx.fill()

  // draw a small checkmark manually (avoids relying on a unicode glyph the font may lack)
  ctx.strokeStyle = '#ffffff'
  ctx.lineWidth = 2.4
  ctx.lineCap = 'round'
  ctx.lineJoin = 'round'
  ctx.beginPath()
  ctx.moveTo(badgeX + 16, badgeY + 17)
  ctx.lineTo(badgeX + 20, badgeY + 21)
  ctx.lineTo(badgeX + 28, badgeY + 12)
  ctx.stroke()

  ctx.fillStyle = '#ffffff'
  ctx.font = 'bold 14px sans-serif'
  ctx.textAlign = 'left'
  ctx.fillText(badgeText, badgeX + 36, badgeY + 22)

  // total harga, top-right
  ctx.textAlign = 'right'
  ctx.fillStyle = '#94a3b8'
  ctx.font = '13px sans-serif'
  ctx.fillText('TOTAL PEMBAYARAN', width - padX, cursorY - 6)
  ctx.fillStyle = '#34d399'
  ctx.font = 'bold 38px sans-serif'
  ctx.fillText(formatRupiah(harga), width - padX, cursorY + 36)

  // brand + product title (left, below badge)
  cursorY = badgeY + badgeH + 34
  ctx.textAlign = 'left'
  ctx.fillStyle = '#a78bfa'
  ctx.font = 'bold 13px sans-serif'
  ctx.fillText(brandDisplay.toUpperCase(), padX, cursorY)

  cursorY += 30
  ctx.fillStyle = '#ffffff'
  ctx.font = 'bold 27px sans-serif'
  const titleLine = `${namaPaket || '-'} • Server ${server === 'SG' ? 'Singapore' : 'Indonesia'}`
  ctx.fillText(titleLine, padX, cursorY)

  // === DASHED DIVIDER ===
  const dividerY = cursorY + 26
  ctx.strokeStyle = 'rgba(148, 163, 184, 0.30)'
  ctx.lineWidth = 1
  ctx.setLineDash([6, 6])
  ctx.beginPath()
  ctx.moveTo(padX, dividerY)
  ctx.lineTo(width - padX, dividerY)
  ctx.stroke()
  ctx.setLineDash([])

  // === DATA GRID (2 columns x 4 rows, compact) ===
  const gridTop = dividerY + 38
  const rowH = 56
  const colGap = 40
  const colW = (width - padX * 2 - colGap) / 2
  const colLeftX = padX
  const colRightX = padX + colW + colGap

  const leftRows = [
    { label: 'CUSTOMER', value: customerDisplay },
    { label: 'APLIKASI', value: aplikasiVpn || '-' },
    { label: 'SERVER', value: server === 'SG' ? 'Singapore' : 'Indonesia' },
    { label: 'PERANGKAT', value: `${perangkat} IP` },
  ]
  const rightRows = [
    { label: 'PAKET', value: namaPaket || '-' },
    { label: 'SSH/V2RAY', value: sshV2ray || '-' },
    { label: 'DURASI', value: `${durasi} Hari` },
    { label: 'TANGGAL', value: tanggal || '-' },
  ]

  const drawRow = (row, x, y) => {
    ctx.textAlign = 'left'
    ctx.fillStyle = '#7c8aa8'
    ctx.font = '12px sans-serif'
    ctx.fillText(row.label, x, y)
    ctx.fillStyle = '#f1f5f9'
    ctx.font = 'bold 17px sans-serif'
    ctx.fillText(String(row.value), x, y + 24)
  }

  leftRows.forEach((row, i) => drawRow(row, colLeftX, gridTop + i * rowH))
  rightRows.forEach((row, i) => drawRow(row, colRightX, gridTop + i * rowH))

  // vertical divider between columns
  ctx.strokeStyle = 'rgba(139, 92, 246, 0.18)'
  ctx.lineWidth = 1
  ctx.beginPath()
  ctx.moveTo(padX + colW + colGap / 2, gridTop - 10)
  ctx.lineTo(padX + colW + colGap / 2, gridTop + leftRows.length * rowH - 24)
  ctx.stroke()

  // === FOOTER BAND ===
  const footerY = gridTop + leftRows.length * rowH + 6
  ctx.strokeStyle = 'rgba(148, 163, 184, 0.18)'
  ctx.lineWidth = 1
  ctx.beginPath()
  ctx.moveTo(padX, footerY)
  ctx.lineTo(width - padX, footerY)
  ctx.stroke()

  const footerTextY = footerY + 30
  ctx.textAlign = 'left'
  ctx.fillStyle = '#64748b'
  ctx.font = '11px sans-serif'
  ctx.fillText('NO. TRANSAKSI', padX, footerTextY - 16)
  ctx.fillStyle = '#cbd5e1'
  ctx.font = 'bold 14px sans-serif'
  ctx.fillText(trx || '-', padX, footerTextY + 4)

  ctx.fillStyle = '#64748b'
  ctx.font = '11px sans-serif'
  ctx.fillText(`${ptDisplay}  •  Owner: ${ownerDisplay}`, padX + 260, footerTextY - 16)

  ctx.textAlign = 'right'
  ctx.fillStyle = '#a78bfa'
  ctx.font = 'bold 16px sans-serif'
  ctx.fillText('TERIMA KASIH', width - padX, footerTextY)

  // bottom accent line
  ctx.fillStyle = accentGrad
  ctx.fillRect(0, height - 4, width, 4)

  return canvas.toBuffer('image/png')
}

function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath()
  ctx.moveTo(x + r, y)
  ctx.lineTo(x + w - r, y)
  ctx.quadraticCurveTo(x + w, y, x + w, y + r)
  ctx.lineTo(x + w, y + h - r)
  ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h)
  ctx.lineTo(x + r, y + h)
  ctx.quadraticCurveTo(x, y + h, x, y + h - r)
  ctx.lineTo(x, y + r)
  ctx.quadraticCurveTo(x, y, x + r, y)
  ctx.closePath()
}

module.exports = { generateTestimonyImage, formatRupiah, maskPhoneNumber }
