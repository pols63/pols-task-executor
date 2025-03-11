import { PLogger } from 'pols-logger'
import { PTaskExecutor } from '../src/index'
import path from 'path'

const taskExecutor = new PTaskExecutor({
	tasks: [{
		schedule: {
			every: 2
		},
		command: 'powershell.exe -Command npx ts-node-dev -r tsconfig-paths/register --project tsconfig.json "test/task1.ts"',
		workPath: path.join(__dirname, '..'),
	},
		// {
		// id: '1',
		// schedule: {
		// 	hours: [17],
		// 	minutes: [5, 6]
		// },
		// command: 'powershell.exe -Command npx ts-node-dev -r tsconfig-paths/register --project tsconfig.json "test/task2.ts"',
		// workPath: path.join(__dirname, '..'),
		// }
	],
	logger: new PLogger
})
// taskExecutor.onStd = (params) => console.log(params)
taskExecutor.start()

// taskExecutor.runTask('1')

// setTimeout(() => {
// 	taskExecutor.stopTask('1').then(() => {
// 		taskExecutor.runTask('1')
// 	}).catch(() => { })
// }, 10000)