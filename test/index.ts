import { PLogger } from 'pols-utils'
import { PTaskExecutor } from '../src/index'
import path from 'path'

const taskExecutor = new PTaskExecutor({
	tasks: [{
		schedule: {
			every: 1
		},
		command: 'powershell.exe -Command npx ts-node-dev -r tsconfig-paths/register --project tsconfig.json "test/task1.ts"',
		workPath: path.join(__dirname, '..'),
	}, {
		schedule: {
			every: 5,
			startTime: '10:30:00',
		},
		command: 'powershell.exe -Command npx ts-node-dev -r tsconfig-paths/register --project tsconfig.json "test/task2.ts"',
		workPath: path.join(__dirname, '..'),
	}],
	logger: new PLogger
})
taskExecutor.start()