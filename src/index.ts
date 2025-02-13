import { rules } from 'pols-validator'
import { spawn } from 'cross-spawn'
import * as crypto from 'crypto'
import * as shellQuote from 'shell-quote'
import { PLogger, PLoggerLogParams } from 'pols-logger'
import { PDate } from 'pols-date'

export enum PTypeOfExecution {
	AUTOMATIC = 'AUTOMATIC',
	MANUAL = 'MANUAL',
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

export type PTaskSystem = PTaskDeclaration & {
	id: string,
	status: PTaskStatuses
	runningStart?: PDate
	runningEnd?: PDate
	duration?: number
	errorMessage?: string
	process?: ReturnType<typeof spawn>
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
	running = 'running',
	repose = 'repose',
}

const finishTask = (task: PTaskSystem, error?: Error) => {
	task.status = PTaskStatuses.repose
	task.runningEnd = new PDate
	task.errorMessage = error?.message
}

const run = (pTaskExecutor: PTaskExecutor, task: PTaskSystem, typeOfExecution: PTypeOfExecution) => {
	task.status = PTaskStatuses.running
	task.runningStart = new PDate
	task.runningEnd = null
	task.errorMessage = null
	task.process = null

	pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} iniciada` })

	try {
		pTaskExecutor.onBeforeExecute?.({ task, type: typeOfExecution })

		const args = shellQuote.parse(task.command).filter(v => typeof v == 'string')
		const process = spawn(args[0], args.slice(1), {
			cwd: task.workPath,
			stdio: 'pipe',
		})
		task.process = process

		process.on('close', (code) => {
			if (task.status == PTaskStatuses.running) {
				finishTask(task)
				pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada (Exitcode ${code})` })
			}
			try {
				pTaskExecutor.onAfterExecute?.({ task, type: typeOfExecution })
			} catch (error) {
				task.errorMessage = error
				pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} dio error en el evento "onAfterExecute"`, body: error })
			}
		})

		process.on('error', (error) => {
			finishTask(task, error)
			pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada con error`, body: error })
		})

		process.stdout.on('data', (data) => {
			pTaskExecutor.log.info({ label: 'TASK-EXECUTOR', description: `STDOUT: ${data.toString().trim()}` })
		})

		process.stderr.on('data', (data) => {
			pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `STDERR: ${data.toString().trim()}` })
		})
	} catch (error) {
		task.status = PTaskStatuses.repose
		task.runningEnd = new PDate
		pTaskExecutor.log.error({ label: 'TASK-EXECUTOR', description: `Tarea ${task.id} finalizada con error`, body: error })
	}
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
			if (task.status == PTaskStatuses.running || !task.schedule) continue
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

				if ('every' in schedule && schedule.every) {
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
}

export class PTaskExecutor {
	idTimer: ReturnType<typeof setInterval>
	tasks: Record<string, PTaskSystem> = {}
	logger?: PLogger

	declare onBeforeExecute?: ({ task, type }: { task: PTaskSystem, type: PTypeOfExecution }) => void
	declare onAfterExecute?: ({ task, type, error }: { task: PTaskSystem, type: PTypeOfExecution, error?: Error }) => void

	constructor(params?: PTaskParams) {
		if (params?.tasks?.length) this.set(...params.tasks)
		this.logger = params?.logger
	}

	set(...taskDeclarations: PTaskDeclaration[]) {
		const ids: string[] = []
		for (const taskDeclaration of taskDeclarations) {
			const id = taskDeclaration.id ?? crypto.randomUUID()

			const isNew = !this.tasks[id]

			const task = this.tasks[id] ?? {
				...taskDeclaration,
				id,
				status: PTaskStatuses.repose
			} as PTaskSystem
			task.command = taskDeclaration.command
			task.workPath = taskDeclaration.workPath
			task.schedule = taskDeclaration.schedule

			this.tasks[id] = task

			if (isNew && task.status == PTaskStatuses.repose) {
				task.runningStart = null
				task.runningEnd = null
				task.duration = null
				task.errorMessage = null
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
	}

	removeTask(id: string) {
		if (!this.tasks[id]) return false
		delete this.tasks[id]
		return true
	}

	runTask(id: string) {
		const task = this.tasks[id]
		if (!task) throw new Error(`No existe tarea con id "${id}"`)
		run(this, task, PTypeOfExecution.MANUAL)
	}

	stopTask(id: string): boolean {
		const task = this.tasks[id]
		if (!task) throw new Error(`No existe tarea con id "${id}"`)
		if (task.status == PTaskStatuses.running) {
			task.process?.kill()
			return true
		} else {
			return false
		}
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