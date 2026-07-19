/*
Base : https://play.google.com/store/apps/details?id=com.droodstudio.aiart
Author : Geno
WhatsApp Channel: https://whatsapp.com/channel/0029Vb6hVYK8V0tkiz4bKs0N
Featured
Support Image Generation
Support Video Generation (Wan 2.2, Hunyuan 1.5, LTX 2.3)
Generate Video Ga Support 10 detik Cuma Support 5 detik doang 
*/

const axios = require('axios');
const crypto = require('crypto');

const IMAGE_MODELS = [
    { id: 'dream_shape_lighting', name: 'General', cost: 1, work_type: 'text2img' },
    { id: 'juggernaut_lighting', name: 'Realistic', cost: 1, work_type: 'text2img' },
    { id: 'redcraft_illustrious', name: 'Realistic 2', cost: 1, work_type: 'text2img' },
    { id: 'ilustreal_illustrious', name: 'Realistic 3', cost: 2, work_type: 'text2img' },
    { id: 'babes_illustrious', name: 'Realistic 4', cost: 2, work_type: 'text2img' },
    { id: 'raemu_lighting', name: 'Anime', cost: 1, work_type: 'text2img' },
    { id: 'wai_Illustrious', name: 'Anime 2', cost: 2, work_type: 'text2img' },
    { id: 'illustrij_Illustrious', name: 'Anime 2.5D', cost: 2, work_type: 'text2img' },
    { id: 'prefect_illustrious', name: 'Anime 3', cost: 1, work_type: 'text2img' },
    { id: 'goddess_illustrious', name: 'Realistic 6', cost: 2, work_type: 'text2img' },
    { id: 'perfectdeliberate_illustrious', name: 'Anime 2.5D 2', cost: 2, work_type: 'text2img' },
    { id: 'guofeng_sdxl', name: 'GuoFeng', cost: 1, work_type: 'text2img' },
    { id: 'disney_cartoon_sdxl', name: 'Disney Cartoon', cost: 1, work_type: 'text2img' },
    { id: 'samaritan_sdxl', name: 'Samaritan', cost: 1, work_type: 'text2img' },
    { id: 'prefectious_illustrious', name: 'Anime 4', cost: 1, work_type: 'text2img' },
    { id: 'realvis_lighting', name: 'Realistic 5', cost: 1, work_type: 'text2img' },
    { id: 'flux2_klein_fast', name: 'Flux 2 Klein Fast', cost: 2.5, work_type: 'flux2_text2img' },
    { id: 'flux2_klein', name: 'Flux 2 Klein', cost: 2.5, work_type: 'flux2_text2img' },
    { id: 'redzimage_zimg', name: 'ZImage', cost: 2, work_type: 'zimg_text2img' }
];

const MODEL_IDS = IMAGE_MODELS.map(m => m.id);

const VIDEO_WORK_TYPES = [
    'text2video_wan',
    'image2video_wan',
    'text2video_hunyuan',
    'image2video_hunyuan',
    'text2video_ltx2',
    'image2video_ltx2'
];

const VIDEO_ENGINES = [
    {
        id: 'wan2_2',
        name: 'Wan 2.2',
        work_types: {
            text2video: 'text2video_wan',
            image2video: 'image2video_wan'
        }
    },
    {
        id: 'hunyuan1_5',
        name: 'Hunyuan 1.5',
        work_types: {
            text2video: 'text2video_hunyuan',
            image2video: 'image2video_hunyuan'
        }
    },
    {
        id: 'ltx2',
        name: 'LTX 2',
        work_types: {
            text2video: 'text2video_ltx2',
            image2video: 'image2video_ltx2'
        }
    }
];

const IMAGE_RATIOS = ['1:1', '9:16', '16:9', '3:4', '4:3', '2:3', '3:2'];
const VIDEO_DURATIONS = [5, 10, 20];
const VIDEO_RESOLUTIONS = ['480p', '720p', '1080p'];

function generateUUID() {
    return crypto.randomUUID();
}

function generateAndroidId() {
    return crypto.randomBytes(8).toString('hex');
}

class AIArtGenClient {
    constructor(config = {}) {
        this.appVersionCode = config.appVersionCode || '831';
        this.appVersionName = config.appVersionName || '8.3.1';
        this.platform = 'android';
        
        this.deviceId = config.deviceId || generateUUID();
        this.adId = config.adId || generateUUID();
        this.androidId = config.androidId || generateAndroidId();
        
        this.configHost = 'https://config.production.aiartgen.net';
        this.accountHost = 'https://account.production.aiartgen.net';
        this.syncTaskHost = 'https://sync-task.production.aiartgen.net';
        this.asyncTaskHost = 'https://async-task.production.aiartgen.net';
        
        this.bearerToken = config.bearerToken || null;
    }

    refreshGuestAccount() {
        if (!this.bearerToken) {
            this.deviceId = generateUUID();
            this.adId = generateUUID();
            this.androidId = generateAndroidId();
        }
    }

    _buildQueryParams(extraParams = {}) {
        const params = new URLSearchParams({
            app_version_code: this.appVersionCode,
            app_version_name: this.appVersionName,
            device_id: this.deviceId,
            platform: this.platform,
            ad_id: this.adId,
            android_id: this.androidId,
            ...extraParams
        });
        return params.toString();
    }

    async _request(url, method = 'GET', body = null) {
        const headers = {
            'User-Agent': 'Neo/1.0',
            'Accept-Encoding': 'gzip',
        };

        if (this.bearerToken) {
            headers['Authorization'] = `Bearer ${this.bearerToken}`;
        }

        const options = {
            method,
            url,
            headers,
            timeout: 120000
        };

        if (body) {
            options.data = body;
        }

        const response = await axios(options);
        return response.data;
    }

    async getConfig(clientDiamonds = 0) {
        const qps = this._buildQueryParams({ client_diamonds: clientDiamonds.toString() });
        const url = `${this.configHost}/api/v1/config?${qps}`;
        const data = await this._request(url, 'GET');
        this._lastConfig = data;
        return data;
    }

    async updateFcmToken(fcmToken) {
        const qps = this._buildQueryParams();
        const url = `${this.accountHost}/api/v1/account/update_fcm_token?${qps}`;
        return await this._request(url, 'POST', { fcm_token: fcmToken });
    }

    async addSyncTask(prompt, options = {}) {
        const qps = this._buildQueryParams();
        const url = `${this.syncTaskHost}/api/v1/sync_task/add?${qps}`;
        
        const modelId = options.modelId || 'flux2_klein_fast';
        let defaultWorkType = 'text2img';
        if (modelId.startsWith('flux2')) {
            defaultWorkType = 'flux2_text2img';
        } else if (modelId.startsWith('redzimage') || modelId.includes('zimg')) {
            defaultWorkType = 'zimg_text2img';
        }

        const payload = {
            device_id: this.deviceId,
            prompt: prompt,
            prompt_translated: prompt,
            negative_prompt: options.negativePrompt || '',
            model_id: modelId,
            work_type: options.workType || defaultWorkType,
            width: options.width || 756,
            height: options.height || 1344,
            seed: options.seed || Math.floor(Math.random() * 1000000000000),
            priority: 0,
            has_face: options.hasFace || false,
            batch_size: 1,
            steps: options.steps || 20,
            cfg_scale: options.cfgScale || 7.0,
            is4k: options.is4k || false,
            client_diamonds: options.clientDiamonds || 50,
            ratio: options.ratio || '9:16',
            style: options.style || 'base'
        };

        return await this._request(url, 'POST', payload);
    }

    async getTaskStatus(taskId) {
        const qps = this._buildQueryParams();
        const url = `${this.syncTaskHost}/api/v1/sync_task/status/${taskId}?${qps}`;
        return await this._request(url, 'GET');
    }

    async getTaskResult(taskId) {
        const qps = this._buildQueryParams();
        const url = `${this.syncTaskHost}/api/v1/sync_task/result/${taskId}?${qps}`;
        return await this._request(url, 'GET');
    }

    async addVideoTask(prompt, options = {}) {
        const qps = this._buildQueryParams();
        const url = `${this.asyncTaskHost}/api/v1/async_task/add?${qps}`;

        const workType = options.workType || 'text2video_wan_720P';
        
        const payload = {
            device_id: this.deviceId,
            prompt: prompt,
            prompt_translated: prompt,
            negative_prompt: options.negativePrompt || '',
            model_id: 'static',
            work_type: workType,
            width: 1024,
            height: 1024,
            seed: options.seed || Math.floor(Math.random() * 1000000000000),
            priority: 0,
            has_face: options.hasFace || false,
            batch_size: 1,
            steps: options.steps || 20,
            cfg_scale: options.cfgScale || 7.0,
            is4k: options.is4k || false,
            client_diamonds: options.clientDiamonds || 50,
            ratio: options.ratio || '9:16',
            style: options.style || '',
            video_width: options.videoWidth || 720,
            video_height: options.videoHeight || 1280,
            video_duration: options.videoDuration || 5,
            image_url: options.imageUrl || ''
        };

        return await this._request(url, 'POST', payload);
    }

    async getBatchTaskStatus(taskIds) {
        const qps = this._buildQueryParams();
        const url = `${this.asyncTaskHost}/api/v1/async_task/batch-status?${qps}`;
        return await this._request(url, 'POST', {
            device_id: this.deviceId,
            task_ids: taskIds
        });
    }

    async generateVideo(prompt, options = {}, progressCallback = null) {
        const workType = options.workType || 'text2video_wan_720P';
        const duration = options.videoDuration || 5;

        const _getCost = (cfg) => {
            const costInfo = cfg?.async_task_cost_info?.costs?.[workType];
            if (!costInfo) return 0;
            const base = costInfo.base_cost || 0;
            const mult = costInfo.duration_multiplier?.[String(duration)] || 1;
            return base * mult;
        };

        this.refreshGuestAccount();
        let config = await this.getConfig(0);
        let diamonds = config?.account_info?.diamonds ?? 0;
        const cost = _getCost(config);

        if (diamonds < cost) {
            this.refreshGuestAccount();
            config = await this.getConfig(0);
            diamonds = config?.account_info?.diamonds ?? 0;
        }

        let taskResponse;
        try {
            taskResponse = await this.addVideoTask(prompt, { ...options, clientDiamonds: diamonds });
        } catch (err) {
            if (err.response && err.response.status === 402) {
                this.refreshGuestAccount();
                config = await this.getConfig(0);
                diamonds = config?.account_info?.diamonds ?? 0;
                taskResponse = await this.addVideoTask(prompt, { ...options, clientDiamonds: diamonds });
            } else {
                throw err;
            }
        }
        
        if (!taskResponse.success || !taskResponse.task_id) {
            throw new Error(`Failed to submit video task: ${JSON.stringify(taskResponse)}`);
        }
        
        const taskId = taskResponse.task_id;

        let isDone = false;
        let pollAttempts = 0;
        const maxAttempts = 60;
        const pollIntervalMs = 5000;

        while (!isDone && pollAttempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
            pollAttempts++;
            
            const batchStatusResponse = await this.getBatchTaskStatus([taskId]);
            const tasksList = Array.isArray(batchStatusResponse) ? batchStatusResponse : (batchStatusResponse.tasks || []);
            const taskStatus = tasksList.find(t => t.task_id === taskId);
            
            if (!taskStatus) {
                continue;
            }

            const status = taskStatus.status;
            const progress = taskStatus.progress ?? 0;
            
            if (progressCallback) {
                progressCallback(status, progress);
            }

            if (status === 2 || status === 'completed' || status === 'success') {
                isDone = true;
                return taskStatus;
            } else if (status === 3 || status === 'failed') {
                throw new Error(`Video generation task failed: ${taskStatus.error_message || 'Unknown error'}`);
            }
        }

        if (!isDone) {
            throw new Error('Video generation timed out.');
        }
    }

    async generateImage(prompt, options = {}, progressCallback = null) {
        this.refreshGuestAccount();
        let config = await this.getConfig(0);
        let diamonds = config?.account_info?.diamonds ?? 50;

        let taskResponse;
        try {
            taskResponse = await this.addSyncTask(prompt, { ...options, clientDiamonds: diamonds });
        } catch (err) {
            if (err.response && err.response.status === 402) {
                this.refreshGuestAccount();
                config = await this.getConfig(0);
                diamonds = config?.account_info?.diamonds ?? 50;
                taskResponse = await this.addSyncTask(prompt, { ...options, clientDiamonds: diamonds });
            } else {
                throw err;
            }
        }

        if (!taskResponse.success || !taskResponse.task_id) {
            throw new Error(`Failed to submit task: ${JSON.stringify(taskResponse)}`);
        }
        
        const taskId = taskResponse.task_id;

        let isDone = false;
        let attempts = 0;
        const maxAttempts = 60;
        const pollIntervalMs = 5000;

        while (!isDone && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, pollIntervalMs));
            attempts++;
            
            const statusData = await this.getTaskStatus(taskId);
            const status = statusData.status;
            const progress = statusData.progress ?? 0;
            
            if (progressCallback) {
                progressCallback(status, progress);
            }

            if (status === 'completed') {
                isDone = true;
            } else if (status === 'failed') {
                throw new Error(`Generation task failed: ${statusData.error_message}`);
            }
        }

        if (!isDone) {
            throw new Error('Generation timed out.');
        }

        return await this.getTaskResult(taskId);
    }
}

AIArtGenClient.MODEL_IDS = MODEL_IDS;
AIArtGenClient.IMAGE_MODELS = IMAGE_MODELS;
AIArtGenClient.VIDEO_WORK_TYPES = VIDEO_WORK_TYPES;
AIArtGenClient.VIDEO_ENGINES = VIDEO_ENGINES;
AIArtGenClient.IMAGE_RATIOS = IMAGE_RATIOS;
AIArtGenClient.VIDEO_DURATIONS = VIDEO_DURATIONS;
AIArtGenClient.VIDEO_RESOLUTIONS = VIDEO_RESOLUTIONS;

if (require.main === module) {
    (async () => {
        const client = new AIArtGenClient();

        console.log("-> Pesan 1 Uji Coba Generate Gambar ...");
        try {
            const imgRes = await client.generateImage('a cute panda wizard, detailed, 4k', {
                modelId: 'flux2_klein',
                ratio: '1:1'
            }, (status, progress) => {
                process.stdout.write(`\rGambar: Progress ${progress}% [Status: ${status}]`);
            });
            console.log("\n[SUCCESS] Gambar selesai!");
            console.log(JSON.stringify(imgRes, null, 2));
        } catch (err) {
            console.error("\n[ERROR] Gagal generate gambar:", err.message);
        }

        console.log("\n-> Pesan 2 Uji Coba Generate Video ...");
        try {
            const vidRes = await client.generateVideo('a beautiful waterfall flowing smoothly, realistic', {
                workType: 'text2video_wan',
                videoDuration: 5
            }, (status, progress) => {
                process.stdout.write(`\rVideo: Progress ${progress}% [Status: ${status}]`);
            });
            console.log("\n[SUCCESS] Video selesai!");
            console.log(JSON.stringify(vidRes, null, 2));
        } catch (err) {
            console.error("\n[ERROR] Gagal generate video:", err.message);
        }
    })();
} else {
    module.exports = AIArtGenClient;
}