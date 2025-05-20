import { rules } from 'pols-validator'
import { spawn } from 'cross-spawn'
import * as crypto from 'crypto'
import * as shellQuote from 'shell-quote'
import { PLogger, PLoggerLogParams } from 'pols-logger'
import { PDate } from 'pols-date'
import os from 'os'

export enum PStdType {
	OUT = 'OUT',
	ERROR = 'ERROR'
}

export enum PTypeOfExecution {
	AUTOMATIC = 'AUTOMATIC',
	MANUAL = 'MANUAL',
}

export enum PTaskExecutorStatuses {
	RUNNING = 'RUNNING',
	STOPPED = 'STOPPED'
}

export type PTaskDeclaration = {
	id?: string
	schedule: PSchedule | PSchedule[]
	command: string
	workPath?: string
}

export type PTaskParams = {
	tasks?: PTaskDeclaration[]
	logger?: PLogger
}

export class PTaskSystem {
	id: string
	schedule: PSchedule | PSchedule[]
	command: string | (() => Promise<void>)
	workPath?: string
	status: PTaskStatuses
	runningStart?: PDate
	runningEnd?: PDate
	duration?: number
	process?: ReturnType<typeof spawn>

	toJSON() {
		return {
			...this,
			process: undefined
		}
	}
}

export type PSchedule = {
	validity?: {
		startDate?: string | PDate | Date
		endDate?: string | PDate | Date
	}
	weekDays?: number[]
	days?: number[]
	months?: number[]
} & ({
	minutes?: number[]
	hours?: number[]
} | {
	every: number
	startTime?: string
	endTime?: string
})

export enum PTaskStatuses {
	RUNNING = 'RUNNING',
	REPOSE = 'REPOSE',
}

const finishTask = (task: PTaskSystem) => {
	task.status = PTaskStatuses.REPOSE
	task.runningEnd = new PDate
}

const killMethods: Record<string, () => void> = {}

const run = (pTaskExecutor: PTaskExecutor, task: PTaskSystem, typeOfExecution: PTypeOfExecution) => {
	task.status = PTaskStatuses.RUNNING
	task.runningStart = new PDate
	task.runningEnd = null
	task.process = null

	pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} iniciada` })

	try {
		pTaskExecutor.onBeforeExecute?.({ task, type: typeOfExecution })

		if (typeof task.command == 'string') {
			const args = shellQuote.parse(task.command).filter(v => typeof v == 'string')
			const process = spawn(args[0], args.slice(1), {
				cwd: task.workPath,
				stdio: 'pipe',
			})

			task.process = process

			process.on('close', (code) => {
				if (task.status == PTaskStatuses.RUNNING) {
					finishTask(task)
					pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada (Exitcode ${code})` })
				}

				const killMethod = killMethods[task.id]
				try {
					pTaskExecutor.onAfterExecute?.({ task, type: typeOfExecution, code, error: code != 0, killed: !!killMethod })
				} catch (error) {
					pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} dio error en el evento "onAfterExecute"`, body: error })
				}
				killMethod?.()
				delete killMethods[task.id]
			})

			process.on('error', (error) => {
				pTaskExecutor.onStd?.({ task, type: PStdType.ERROR, data: error.message + '\n' + error.stack })
				finishTask(task)
				pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada con error`, body: error })
			})

			process.stdout.on('data', (data) => {
				pTaskExecutor.onStd?.({ task, type: PStdType.OUT, data: data.toString().trim() })
			})

			process.stderr.on('data', (data) => {
				pTaskExecutor.onStd?.({ task, type: PStdType.ERROR, data: data.toString().trim() })
			})
		} else {
			task.command.bind(pTaskExecutor)().then(() => {
				pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada` })
			}).catch((error: Error) => {
				pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada con error`, body: error })
			})
		}
	} catch (error) {
		task.status = PTaskStatuses.REPOSE
		task.runningEnd = new PDate
		pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada con error`, body: error })
	}
}

const onInterval = (pTaskExecutor: PTaskExecutor, execute: boolean) => {
	const now = new PDate

	const interval = 1000 * (60 - now.second) - now.millisecond + 500

	/* Define el temporizador para que se ejecute en el segundo cero del siguiente minuto */
	pTaskExecutor.idTimer = setTimeout(
		() => onInterval(pTaskExecutor, true),
		interval
	)

	if (!execute) return

	pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: 'Revisando tareas a ejecutar' })
	for (const task of Object.values(pTaskExecutor.tasks)) {
		if (task.status == PTaskStatuses.RUNNING || !task.schedule) continue
		const schedules = task.schedule instanceof Array ? task.schedule : [task.schedule]
		if (!schedules.length) continue

		let success = false
		for (let [i, schedule] of schedules.entries()) {
			if (!schedule) continue

			const validationResult = rules({ label: 'schedule', required: true }).isObject({
				validity: rules().isObject({
					starDate: rules().isDate(),
					endDate: rules().isDate(),
				}),
				weekDays: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().isGte(0).isLte(6)),
				days: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().isGte(1).isLte(31)),
				months: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().isGte(1).isLte(12)),
				minutes: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().isGte(0).isLte(59)),
				hours: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().isGte(0).isLte(23)),
				every: rules().floor().isGt(0),
				startTime: rules().isTime(),
				endTime: rules().isTime(),
			}).validate<PSchedule>(schedule)
			if (validationResult.error == true) {
				pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `La propiedad 'schedule' del elemento '${i}' (ID ${task.id}) no tiene un formato de programación correcta: ${validationResult.messages}`, body: validationResult.messages })
				continue
			}
			schedule = validationResult.result

			/* Se verifica la vigencia de la programación */
			const startDateValue = schedule.validity?.startDate
			if (startDateValue) {
				const startDate = new PDate(startDateValue)
				if (startDate.isInvalidDate || startDate.timestamp > now.timestamp) continue
			}
			const endDateValue = schedule.validity?.endDate
			if (endDateValue) {
				const endDate = new PDate(endDateValue)
				if (endDate.isInvalidDate || endDate.timestamp < now.timestamp) continue
			}

			/* Se valida si se encuentra dentro del mes y días indicados */
			if (schedule.months?.length && !schedule.months.includes(now.month)) continue
			if (schedule.days?.length && !schedule.days.includes(now.day)) continue
			if (schedule.weekDays?.length && !schedule.weekDays.includes(now.weekDay)) continue

			if ('every' in schedule && schedule.every) {
				/* Ejecución del programa a cada cantidad de minutos */

				/* Se definen las horas de inicio y fin y se valida si se encuentra en el rango */
				const startTime = new PDate().setClockTime(schedule.startTime ?? '00:00:00')
				if (startTime.isInvalidDate || now.timestamp < startTime.timestamp) continue

				const endTime = startTime.clone().clearClockTime()
				if (schedule.endTime) {
					endTime.setClockTime(schedule.endTime)
				} else {
					endTime.addDay(1)
				}
				if (endTime.isInvalidDate || now.timestamp > endTime.timestamp) continue

				if (startTime.timestamp > endTime.timestamp) {
					pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `La propiedad 'startTime' del elemento '${i}' no puede ser posterior a 'endTime'` })
					continue
				}

				/* Verifica si, de acuerdo a la cantidad indicada en every, la tarea se debe ejecutar */
				if (now.minutesDifference(startTime) % schedule.every != 0) continue
			} else if (('minutes' in schedule && schedule.minutes?.length) || ('hours' in schedule && schedule.hours?.length)) {
				if (schedule.hours?.length && !schedule.hours.includes(now.hour)) continue
				if (schedule.minutes?.length && !schedule.minutes.includes(now.minute)) continue
			} else {
				continue
			}
			success = true
		}

		if (success) run(pTaskExecutor, task, PTypeOfExecution.AUTOMATIC)
	}
}

export class PTaskExecutor {
	idTimer: ReturnType<typeof setInterval>
	tasks: Record<string, PTaskSystem> = {}
	logger?: PLogger

	get status() {
		if (this.idTimer != null) {
			return PTaskExecutorStatuses.RUNNING
		} else {
			return PTaskExecutorStatuses.STOPPED
		}
	}

	onBeforeExecute?({ task, type }: { task: PTaskSystem, type: PTypeOfExecution }): void
	onAfterExecute?({ task, type, code, error, killed }: { task: PTaskSystem, type: PTypeOfExecution, code: number, error: boolean, killed: boolean }): void
	onStd?({ task, type, data }: { task: PTaskSystem, type: PStdType, data: string }): void

	constructor(params?: PTaskParams) {
		if (params?.tasks?.length) this.set(...params.tasks)
		this.logger = params?.logger
	}

	set(...taskDeclarations: PTaskDeclaration[]) {
		const ids: string[] = []
		for (const taskDeclaration of taskDeclarations) {
			const id = taskDeclaration.id ?? crypto.randomUUID()

			const isNew = !this.tasks[id]

			const task = this.tasks[id] ?? new PTaskSystem
			if (isNew) {
				task.id = id
				task.status = PTaskStatuses.REPOSE
			}
			task.command = taskDeclaration.command
			task.workPath = taskDeclaration.workPath
			task.schedule = taskDeclaration.schedule

			this.tasks[id] = task

			if (isNew && task.status == PTaskStatuses.REPOSE) {
				task.runningStart = null
				task.runningEnd = null
				task.duration = null
				task.process = null
			}
			ids.push(id)
		}
		return ids
	}

	start() {
		this.log.system({ label: 'TASK-EXECUTOR', description: 'Sistema iniciado' })
		onInterval(this, false)
	}

	stop() {
		clearInterval(this.idTimer)
		this.idTimer = null
	}

	removeTask(id: string) {
		if (!this.tasks[id]) return false
		delete this.tasks[id]
		return true
	}

	runTask(id: string) {
		const task = this.tasks[id]
		if (!task) throw new Error(`No existe tarea con id "${id}"`)
		if (task.status == PTaskStatuses.RUNNING) return false
		run(this, task, PTypeOfExecution.MANUAL)
		return true
	}

	stopTask(id: string): Promise<boolean> {
		return new Promise((resolve) => {
			const task = this.tasks[id]
			if (!task) throw new Error(`No existe tarea con id "${id}"`)
			if (task.status == PTaskStatuses.RUNNING && task.process?.pid) {
				killMethods[id] = () => {
					resolve(true)
				}
				switch (os.platform()) {
					case 'win32':
						spawn('taskkill', ['/F', '/T', '/PID', task.process.pid.toString()])
						break
					default:
						spawn('pkill', ['-TERM', '-P', task.process.pid.toString()])
						break
				}
			} else {
				resolve(false)
			}
		})
	}

	get log() {
		return {
			info: (params: PLoggerLogParams) => this.logger?.info(params),
			warning: (params: PLoggerLogParams) => this.logger?.warning(params),
			error: (params: PLoggerLogParams) => this.logger?.error(params),
			debug: (params: PLoggerLogParams) => this.logger?.debug(params),
			system: (params: PLoggerLogParams) => this.logger?.system(params),
		}
	}
}