import { PDate } from "pols-utils"
import { PLogger, PLoggerLogParams } from "pols-utils/dist/plogger"
import { rules } from 'pols-validator'
import { spawn } from 'child_process'
import * as crypto from 'crypto'

export type PTask = {
	id?: string
	enabled?: boolean
	schedule: PSchedule | PSchedule[]
	command: string
	arguments?: string[]
	workPath?: string
}

export type PTaskReport = {
	state: PTaskState
	runningStart?: PDate
	runningEnd?: PDate
	duration?: number
}

export type PTaskSystem = PTask & PTaskReport

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

export enum PTaskState {
	running = 'running',
	repose = 'repose',
}

const onInterval = (pTaskExecutor: PTaskExecutor, execute: boolean) => {
	const now = new PDate
	const seconds = now.second
	const milliSeconds = now.millisecond

	/* Define el temporizador para que se ejecute en el segundo cero del siguiente minuto */
	pTaskExecutor.idTimer = setTimeout(
		() => onInterval(pTaskExecutor, true),
		1000 * (60 - seconds) - milliSeconds
	)

	if (execute) {
		pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: 'Revisando tareas a ejecutar' })
		for (const task of Object.values(pTaskExecutor.tasks)) {
			if (task.enabled != null && !task.enabled) continue

			if (!task.schedule) continue
			const schedules = task.schedule instanceof Array ? task.schedule : [task.schedule]
			if (!schedules.length) continue

			let success = false
			for (let [i, schedule] of schedules.entries()) {
				if (!schedule) continue

				const validationResult = rules({ label: 'schedule', required: true }).isObject({
					validity: rules().isObject({
						starDate: rules().isTime(),
						endDate: rules().isTime(),
					}),
					weekDays: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().gt(0)),
					days: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().gt(0)),
					months: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().gt(0)),
					minutes: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().gt(0)),
					hours: rules().isArray(i => rules({ label: `Elemento ${i}` }).floor().gt(0)),
					every: rules().floor().gt(0),
					startTime: rules().isTime(),
					endTime: rules().isTime(),
				}).validate<PSchedule>(schedule)
				if (validationResult.error == true) {
					pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `La propiedad 'schedule' del elemento '${i}' no tiene un formato de programación correcta`, body: validationResult.messages })
					continue
				}
				schedule = validationResult.result

				/* Se verifica la vigencia de la programación */
				const startDateValue = schedule.validity?.startDate
				if (startDateValue) {
					const startDate = new PDate(startDateValue)
					if (startDate.isInvalidDate || startDate.time > now.time) continue
				}
				const endDateValue = schedule.validity?.endDate
				if (endDateValue) {
					const endDate = new PDate(endDateValue)
					if (endDate.isInvalidDate || endDate.time < now.time) continue
				}

				/* Se valida si se encuentra dentro del mes y días indicados */
				if (schedule.months?.length && !schedule.months.includes(now.month)) continue
				if (schedule.days?.length && !schedule.days.includes(now.day)) continue
				if (schedule.weekDays?.length && !schedule.weekDays.includes(now.weekDay)) continue

				if ('every' in schedule) {
					/* Ejecución del programa a cada cantidad de minutos */

					/* Se definen las horas de inicio y fin y se valida si se encuentra en el rango */
					const startTime = new PDate().setTime(schedule.startTime ?? '00:00:00')
					if (startTime.isInvalidDate || now.time < startTime.time) continue

					const endTime = startTime.clone().clearTime()
					if (schedule.endTime) {
						endTime.setTime(schedule.endTime)
					} else {
						endTime.addDay(1)
					}
					if (endTime.isInvalidDate || now.time > endTime.time) continue

					if (startTime.time > endTime.time) {
						pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `La propiedad 'startTime' del elemento '${i}' no puede ser posterior a 'endTime'` })
						continue
					}

					/* Verifica si, de acuerdo a la cantidad indicada en every, la tarea se debe ejecutar */
					if (now.minutesDifference(startTime) % schedule.every != 0) continue
				} else if ('minutes' in schedule || 'hours' in schedule) {
					if (schedule.hours?.length && !schedule.hours.includes(now.hour)) continue
					if (schedule.minutes?.length && !schedule.minutes.includes(now.minute)) continue
				} else {
					continue
				}
				success = true
			}

			if (success) {
				task.state = PTaskState.running
				task.runningStart = new PDate

				pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} iniciada` })

				// const process = spawn('cmd.exe', ['/c', 'npx'], {
				// 	cwd: task.workPath,
				// 	// stdio: 'inherit', // Hereda la entrada y salida estándar
				// })

				try {
					const process = spawn(task.command, task.arguments ?? [], {
						cwd: task.workPath,
						stdio: 'pipe',
					})

					process.on('close', (code) => {
						if (task.state == PTaskState.running) {
							task.state = PTaskState.repose
							task.runningEnd = new PDate
							pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada (Exitcode ${code})` })
						}
					})

					process.on('error', (error) => {
						task.state = PTaskState.repose
						task.runningEnd = new PDate
						pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada con error`, body: error })
					})

					process.stdout.on('data', (data) => {
						pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `STDOUT: ${data.toString().trim()}` })
					})

					process.stderr.on('data', (data) => {
						pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `STDERR: ${data.toString().trim()}` })
					})
				} catch (error) {
					pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada con error`, body: error })
				}
			}
		}
	}
}

export class PTaskExecutor {
	idTimer: ReturnType<typeof setInterval>
	_tasks: Record<string, PTask> = {}
	_tasksReport: Record<string, PTaskReport> = {}
	declare tasks: Record<string, PTaskSystem>
	logger?: PLogger

	constructor(params?: {
		tasks: PTask[]
		logger?: PLogger
	}) {
		this.tasks = new Proxy<Record<string, PTaskSystem>>({}, {
			set: (target, property: string, value: PTask) => {
				this._tasks[property] = value
				return true
			},
			get: (target, property: string) => {
				return {
					...this._tasks[property],
					...this._tasksReport[property],
				}
			},
			ownKeys: () => {
				// Devolvemos las claves disponibles
				return Reflect.ownKeys(this._tasks)
			},
			getOwnPropertyDescriptor: (target, property: string) => {
				// Permitir que se enumeren las propiedades en el proxy
				return {
					configurable: true,
					enumerable: true,
					value: {
						...this._tasks[property],
						...this._tasksReport[property],
					},
				}
			}
		})
		if (params?.tasks?.length) this.add(...params.tasks)
		this.logger = params?.logger
	}

	add(...tasks: PTask[]) {
		for (const task of tasks) {
			const id = task.id ?? crypto.randomUUID()
			this._tasks[id] = {
				...task,
				id,
			}
			this._tasksReport[id] = { state: PTaskState.repose }
		}
	}

	start() {
		this.log.system({ label: 'TASK-EXECUTOR', description: 'Sistema iniciado' })
		onInterval(this, false)
	}

	stop() {
		clearInterval(this.idTimer)
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