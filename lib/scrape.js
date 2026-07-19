const axios = require("axios")
const chalk = require('chalk')
const cheerio = require("cheerio")
const BodyForm = require('form-data')
const FormData = require("form-data")
const fs = require('fs')
const path = require('path')
const fetch = require('node-fetch')
async function sfiledl(url) {
    try {
        const headers = {
            'referer': url,
            'user-Agent': 'Mozilla/5.0 (Linux; Android 14; NX769J Build/UKQ1.230917.001; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/130.0.6723.107 Mobile Safari/537.36'
        }

        const getPage = await axios.get(url, { headers, timeout: 30000 })
        const $ = cheerio.load(getPage.data)
        const safelink = $("#download").attr("href")

        headers.cookie = getPage.headers['set-cookie'].map(c => c.split(';')[0]).join('; ')
        headers.referer = safelink

        const resPage = await axios.get(safelink, { headers, timeout: 30000 })
        const ca = cheerio.load(resPage.data)
        const co = ca('.w3-text-blue b').text().match(/^(.+?)(?:\.([^.\s()]+))?(?:\s*\(([^)]*)\))?$/)

        return {
            title: co[1].trim(),
            size: co[3],
            author: $('.list a').first().text().trim(),
            uploaded: $('.icon-upload').parent().text().split(':')[1].trim(),
            mimetype: $('.list:nth-child(2)').eq(0).text().slice(3).trim(),
            extension: co[2],
            downloaded: $('.icon-cloud-download').parent().text().split(':')[1].trim(),
            download: ca("#download").attr("href") + '&k=' + ca("#download").attr("onclick").match(/&k='\+(.*?)';/)?.[1].replace("'", '')
        }
    } catch (err) {
        throw Error(err.message)
    }
}

async function ytdl(url) {
    const response = await fetch('https://shinoa.us.kg/api/download/ytdl', {
        method: 'POST',
        headers: {
            'accept': '*/*',
            'api_key': 'free',
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            text: url
        }),
        timeout: 60000
    });

    if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data;
}

async function uploadToPomf(path) {
  try {
    const fileStream = fs.createReadStream(path);
    const formData = new BodyForm();
    formData.append('files[]', fileStream);

    const response = await axios.post('https://pomf.lain.la/upload.php', formData, {
      headers: {
        ...formData.getHeaders(),
      },
      timeout: 120000,
    });

    return response.data.files[0].url
  } catch (error) {
    console.log("Error at pomf uploader in lib/uploader.js:", error)
    return "error"
  }
}

async function uploadToCdn(Path) {
    return new Promise(async (resolve, reject) => {
        if (!fs.existsSync(Path)) return reject(new Error("File tidak ditemukan."));
        try {
            const form = new BodyForm();
            form.append("file", fs.createReadStream(Path));
            const response = await axios({
                url: "https://cdn.meitang.xyz/upload",
                method: "POST",
                headers: {
                    ...form.getHeaders()
                },
                data: form,
                timeout: 120000
            });
            return resolve(response.data.file.url)
        } catch (err) {
            return reject(new Error(`Gagal upload: ${err.message}`));
        }
    });
}


module.exports = { uploadToPomf,uploadToCdn,ytdl,sfiledl }