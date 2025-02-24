import fs from 'fs'

process.on('SIGINT', () => {
	fs.appendFileSync('salida.txt', 'A Cerrar\n')
	console.log('a cerrar')
	process.exit()
})

const run = async () => {
	let i = 0
	return new Promise((resolve) => {
		setInterval(() => {
			console.log(i++)
			fs.appendFileSync('salida.txt', i.toString() + '\n')
			if (i > 40) {
				resolve(null)
			}
		}, 1000)
	})
}

fs.appendFileSync('salida.txt', 'Inicio\n')
console.log('inicio')
run().then(() => {
	console.log('fin')
	process.exit()
}).catch(error => {
	console.log('error')
	console.error(error)
	process.exit(1)
})