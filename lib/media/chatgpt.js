const axios = require('axios')
const fs = require('fs')
const path = require('path')

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || global.OPENAI_API_KEY || ''
const OPENAI_URL = 'https://api.openai.com/v1/chat/completions'
const MODEL = 'gpt-4o-mini'

const STATE_DIR = path.join(__dirname, '..', 'database', 'chatgpt')
if (!fs.existsSync(STATE_DIR)) fs.mkdirSync(STATE_DIR, { recursive: true })

function statePath(userId) {
  const safeId = userId.replace(/[^0-9a-zA-Z]/g, '_')
  return path.join(STATE_DIR, `${safeId}.json`)
}

function loadState(userId) {
  try {
    return JSON.parse(fs.readFileSync(statePath(userId), 'utf8'))
  } catch {
    return { history: [] }
  }
}

function saveState(userId, state) {
  try {
    fs.writeFileSync(statePath(userId), JSON.stringify(state, null, 2))
  } catch (e) {
    console.log('Gagal simpan state chatgpt:', e.message)
  }
}

function resetHistory(userId) {
  try { fs.unlinkSync(statePath(userId)) } catch {}
}

async function chatgpt(userId, prompt, opts = {}) {
  if (!OPENAI_API_KEY) {
    throw new Error('OPENAI_API_KEY belum di-set. Isi di environment variable server kamu.')
  }

  const {
    onDelta = null,
    systemPrompt = 'Kamu adalah asisten WhatsApp yang ramah, santai, dan membantu. Jawab dengan bahasa Indonesia kecuali diminta lain.'
  } = opts

  const state = loadState(userId)
  const history = state.history || []

  const messages = [
    { role: 'system', content: systemPrompt },
    ...history,
    { role: 'user', content: prompt }
  ]

  let res
  try {
    res = await axios.post(
      OPENAI_URL,
      {
        model: MODEL,
        messages,
        stream: true
      },
      {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          'Content-Type': 'application/json'
        },
        responseType: 'stream',
        timeout: 120000
      }
    )
  } catch (err) {
    const apiError = err.response?.data?.error?.message || err.message
    throw new Error(`Request ke OpenAI gagal: ${apiError}`)
  }

  return new Promise((resolve, reject) => {
    let fullText = ''
    let buf = ''

    res.data.on('data', chunk => {
      buf += chunk.toString()
      const lines = buf.split('\n')
      buf = lines.pop()

      for (const line of lines) {
        const trimmed = line.trim()
        if (!trimmed.startsWith('data: ')) continue
        const payload = trimmed.slice(6)
        if (payload === '[DONE]') continue

        try {
          const evt = JSON.parse(payload)
          const delta = evt.choices?.[0]?.delta?.content
          if (delta) {
            fullText += delta
            if (onDelta) onDelta(fullText)
          }
        } catch {}
      }
    })

    res.data.on('end', () => {
      if (!fullText.trim()) {
        return reject(new Error('OpenAI mengembalikan respons kosong. Cek kuota/billing akun kamu.'))
      }

      const newHistory = [
        ...history,
        { role: 'user', content: prompt },
        { role: 'assistant', content: fullText }
      ].slice(-20)

      saveState(userId, { history: newHistory })
      resolve(fullText.trim())
    })

    res.data.on('error', reject)
  })
}

module.exports = { chatgpt, resetHistory }