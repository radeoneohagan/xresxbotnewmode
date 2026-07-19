const axios = require('axios')

const SUNO_API_BASE = process.env.SUNO_API_BASE || 'https://api.sunoapi.org'
const SUNO_API_KEY = process.env.SUNO_API_KEY || ''

async function generateMusic(prompt, { instrumental = false, style = '', title = '' } = {}) {
  const res = await axios.post(
    `${SUNO_API_BASE}/api/v1/generate`,
    {
      prompt,
      style,
      title,
      instrumental,
      customMode: !!style || !!title,
      model: 'V4'
    },
    {
      headers: {
        Authorization: `Bearer ${SUNO_API_KEY}`,
        'Content-Type': 'application/json'
      },
      timeout: 30000
    }
  )

  const taskId = res.data?.data?.taskId || res.data?.taskId
  if (!taskId) throw new Error('Gagal mendapatkan taskId dari Suno API')

  return await pollResult(taskId)
}

async function pollResult(taskId, maxTries = 60, interval = 5000) {
  for (let i = 0; i < maxTries; i++) {
    await new Promise(r => setTimeout(r, interval))

    let res
    try {
      res = await axios.get(`${SUNO_API_BASE}/api/v1/generate/record-info`, {
        params: { taskId },
        headers: { Authorization: `Bearer ${SUNO_API_KEY}` },
        timeout: 15000
      })
    } catch (e) {
      // [AUDIT-FIX] Retry pada network error (bukan crash); skip iterasi ini
      if (i >= maxTries - 1) throw new Error('Gagal polling status musik: ' + (e?.message || e))
      continue
    }

    const data = res.data?.data
    const status = data?.status

    if (status === 'SUCCESS' || status === 'complete') {
      const clip = data?.response?.sunoData?.[0] || data?.clips?.[0]
      if (!clip?.audioUrl && !clip?.audio_url) throw new Error('Audio URL tidak ditemukan')
      return {
        audioUrl: clip.audioUrl || clip.audio_url,
        title: clip.title || 'Untitled'
      }
    }

    if (status === 'FAILED' || status === 'error') {
      throw new Error('Generate musik gagal di pihak Suno')
    }
  }

  throw new Error('Timeout menunggu hasil generate musik (5 menit)')
}

module.exports = { generateMusic }