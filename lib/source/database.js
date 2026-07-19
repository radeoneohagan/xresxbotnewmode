require('../../settings');
const fs = require('fs');
const path = require('path');
const chalk = require('chalk');
const mongoose = require('mongoose');
let DataBase;

if (/mongo/.test("database.json")) {
	DataBase = class mongoDB {
		constructor(url, options = { useNewUrlParser: true, useUnifiedTopology: true }) {
			this.url = url
			this.data = {}
			this._model = {}
			this.options = options
		}
		
		read = async () => {
			mongoose.connect(this.url, { ...this.options })
			this.connection = mongoose.connection
			try {
				const schema = new mongoose.Schema({
					data: {
						type: Object,
						required: true,
						default: {},
					}
				})
				this._model = mongoose.model('data', schema)
			} catch {
				this._model = mongoose.model('data')
			}
			this.data = await this._model.findOne({})
			if (!this.data) {
				new this._model({ data: {} }).save()
				this.data = await this._model.findOne({})
			} else return this?.data?.data || this?.data
		}
		
		write = async (data) => {
			if (this.data && !this.data.data) return (new this._model({ data })).save()
			this._model.findById(this.data._id, (err, docs) => {
				if (!err) {
					if (!docs.data) docs.data = {}
					docs.data = data
					return docs.save()
				}
			})
		}
	}
} else if (/json/.test("database.json")) {
	DataBase = class dataBase {
		data = {}
		file = path.join(process.cwd(), 'database', "database.json");
		backupFile = path.join(process.cwd(), 'database', "database.json.bak");
		
		read = async () => {
			let data;
			if (fs.existsSync(this.file)) {
				try {
					const raw = fs.readFileSync(this.file, 'utf-8')
					data = JSON.parse(raw)
					// [FIX C2] Validasi: jika data punya minimal structure yang valid, gunakan
					// Jika file corrupt jadi {} tapi backup ada, restore dari backup
					if (data && typeof data === 'object' && Object.keys(data).length === 0) {
						if (fs.existsSync(this.backupFile)) {
							try {
								const backupRaw = fs.readFileSync(this.backupFile, 'utf-8')
								const backupData = JSON.parse(backupRaw)
								if (backupData && Object.keys(backupData).length > 0) {
									console.log(chalk.yellow('[DATABASE] File utama kosong, restore dari backup.'))
									data = backupData
								}
							} catch { /* backup juga corrupt, pakai data kosong */ }
						}
					}
				} catch (e) {
					// [FIX C1] JSON parse gagal (file corrupt) — coba restore dari backup
					console.log(chalk.red('[DATABASE] File utama corrupt, mencoba backup...'))
					if (fs.existsSync(this.backupFile)) {
						try {
							const backupRaw = fs.readFileSync(this.backupFile, 'utf-8')
							data = JSON.parse(backupRaw)
							console.log(chalk.green('[DATABASE] Berhasil restore dari backup.'))
						} catch {
							console.log(chalk.red('[DATABASE] Backup juga corrupt. Mulai dengan data kosong.'))
							data = this.data
						}
					} else {
						data = this.data
					}
				}
			} else {
				fs.writeFileSync(this.file, JSON.stringify(this.data, null, 2))
				data = this.data
			}
			return data
		}
		
		write = async (data) => {
			this.data = !!data ? data : global.db
			let dirname = path.dirname(this.file)
			if (!fs.existsSync(dirname)) fs.mkdirSync(dirname, { recursive: true })
			// [FIX C1] Atomic write: tulis ke .tmp dulu, lalu rename
			const tmpFile = this.file + '.tmp'
			try {
				const jsonStr = JSON.stringify(this.data, null, 2)
				fs.writeFileSync(tmpFile, jsonStr)
				// Buat backup dari file yang valid sebelumnya
				if (fs.existsSync(this.file)) {
					try { fs.copyFileSync(this.file, this.backupFile) } catch {}
				}
				fs.renameSync(tmpFile, this.file)
			} catch (e) {
				console.log(chalk.red('[DATABASE] Gagal menulis database:'), e.message)
				// Hapus tmp file jika gagal
				try { if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile) } catch {}
			}
			return this.file
		}
	}
}

module.exports = DataBase