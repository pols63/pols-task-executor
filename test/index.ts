import { PLogger } from 'pols-logger'
import { PTaskExecutor } from '../src/index'
import path from 'path'

const taskExecutor = new PTaskExecutor({
	tasks: [{
	// 	schedule: {
	// 		every: 1
	// 	},
	// 	command: 'powershell.exe -Command npx ts-node-dev -r tsconfig-paths/register --project tsconfig.json "test/task1.ts"',
	// 	workPath: path.join(__dirname, '..'),
	// }, {
		schedule: {
			hours: [17],
			minutes: [5,6]
		},
		command: 'powershell.exe -Command npx ts-node-dev -r tsconfig-paths/register --project tsconfig.json "test/task2.ts"',
		workPath: path.join(__dirname, '..'),
	}],
	logger: new PLogger
})
taskExecutor.start()