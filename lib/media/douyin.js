const axios = require("axios");

class DouyinDownloader {
  constructor() {
    this.baseUrl = "https://snapdouyin.app/wp-json/mx-downloader/video-data/";
    this.headers = {
      accept: "application/json, text/javascript, */*; q=0.01",
      "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
      origin: "https://snapdouyin.app",
      referer: "https://snapdouyin.app/",
      "user-agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
      "x-requested-with": "XMLHttpRequest"
    };
  }

  _validateUrl(url) {
    if (!url || typeof url !== "string") {
      return { valid: false, error: "URL is required." };
    }
    const douyinRegex = /^(https?:\/\/)?(v\.|www\.|vt\.)?(douyin\.com|iesdouyin\.com)/i;
    if (!douyinRegex.test(url)) {
      return { valid: false, error: "Invalid Douyin URL. Use douyin.com or iesdouyin.com links." };
    }
    return { valid: true };
  }

  async download(url) {
    const { valid, error } = this._validateUrl(url);
    if (!valid) return { status: false, error };

    try {
      const { data } = await axios.post(
        this.baseUrl,
        new URLSearchParams({ url, token: "" }).toString(),
        { headers: this.headers, timeout: 30000 }
      );

      if (!data || data.error) {
        return {
          status: false,
          error: data?.error || "Failed to fetch video data."
        };
      }

      return {
        status: true,
        data: {
          title: data.title,
          thumbnail: data.thumbnail,
          duration: data.duration,
          source: data.source,
          medias: (data.medias || []).map(m => ({
            url: m.url,
            quality: m.quality,
            extension: m.extension,
            size: m.size,
            formattedSize: m.formattedSize,
            videoAvailable: m.videoAvailable,
            audioAvailable: m.audioAvailable
          }))
        }
      };
    } catch (err) {
      const errMsg = err.response?.data?.error || err.message || "Download failed.";
      return {
        status: false,
        error: errMsg
      };
    }
  }

  async getDownloadUrl(url, quality = "hd") {
    const result = await this.download(url);
    if (!result.status) return result;

    const media = result.data.medias.find(m => m.quality === quality) ||
                  result.data.medias.find(m => m.quality.includes("hd")) ||
                  result.data.medias[0];

    if (!media) {
      return { status: false, error: "No download URL found." };
    }

    return {
      status: true,
      data: {
        url: media.url,
        quality: media.quality,
        extension: media.extension,
        size: media.formattedSize
      }
    };
  }
}

module.exports = DouyinDownloader;