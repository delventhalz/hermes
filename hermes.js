const zmq = require('zmq')
const protobuf = require('protobufjs')
const argv = require('minimist')(process.argv.slice(2))

const randInt = max => Math.floor(Math.random() * max)
const ID = ('00' + randInt(46655).toString(36)).toUpperCase().slice(-3)
const ADDRESS = `${argv.protocol || 'tcp'}://${argv.host || '127.0.0.1'}`
const TOPIC = argv.topic || 'hermes'

const Message = protobuf.loadSync('message.proto').lookup('message.Message')

const wrapMessage = msg => Message.encode({message: msg}).finish()
const unwrapMessage = buffer => Message.decode(buffer).message
const getDefaultMessage = ({wait}) => `${ID}: hello after ${wait} ms`

// sends messages at random intervals indefinitely
const sendMessages = (socket, msgFn = getDefaultMessage) => {
  ;(function waitAndSend () {
    const wait = randInt(10000)

    setTimeout(() => {
      socket.send(wrapMessage(msgFn({wait})))
      console.log(`\n${ID} ${socket.type}ed a message`)

      waitAndSend()
    }, wait)
  })()
}

console.log(`Hermes ${ID} created on ${ADDRESS}`)

// push-pull
if (argv.push) {
  let socket = zmq.socket('push')

  socket.bindSync(`${ADDRESS}:${argv.push}`)
  console.log(`${ID} is pushing to ${argv.push}`)

  sendMessages(socket)
}

if (argv.pull) {
  let socket = zmq.socket('pull')

  socket.connect(`${ADDRESS}:${argv.pull}`)
  console.log(`${ID} is pulling from ${argv.pull}`)

  socket.on('message', buffer => {
    console.log(`\n${ID} pulled a message:\n${unwrapMessage(buffer)}`)
  })
}

// publish-subscribe
if (argv.pub) {
  let socket = zmq.socket('pub')

  socket.bindSync(`${ADDRESS}:${argv.pub}`)
  console.log(`${ID} is publishing to ${argv.pub} under "${TOPIC}"`)

  sendMessages(socket, (data) => [TOPIC, getDefaultMessage(data)])
}

if (argv.sub) {
  let socket = zmq.socket('sub')

  socket.connect(`${ADDRESS}:${argv.sub}`)
  socket.subscribe(TOPIC)
  console.log(`${ID} is subscribed to ${argv.sub} under "${TOPIC}"`)

  socket.on('message', (topic, buffer) => {
    console.log(
      `\n${ID} received a message relating to "${TOPIC}":
      \n${unwrapMessage(buffer)}`
    )
  })
}

// request-reply
if (argv.req) {
  let socket = zmq.socket('req')

  socket.connect(`${ADDRESS}:${argv.req}`)
  console.log(`${ID} is sending requests to ${argv.req}`)

  sendMessages(socket)
  socket.on('message', buffer => {
    console.log(`\n${ID} got a reply:\n${unwrapMessage(buffer)}`)
  })
}

if (argv.rep) {
  let socket = zmq.socket('rep')

  socket.bindSync(`${ADDRESS}:${argv.rep}`)
  console.log(`${ID} is ready to reply to ${argv.rep}`)

  socket.on('message', buffer => {
    const sender = unwrapMessage(buffer).slice(0, 3)
    socket.send(wrapMessage(`${ID}: Hello ${sender}!`))
    console.log(`\nreplied to ${sender}`)
  })
}

// dealer-router
if (argv.dealer) {
  let socket = zmq.socket('dealer')

  socket.bindSync(`${ADDRESS}:${argv.dealer}`)
  console.log(`${ID} is dealing to ${argv.dealer}`)

  sendMessages(socket)

  socket.on('message', buffer => {
    console.log(`\n${ID} got routed:\n${unwrapMessage(buffer)}`)
  })
}

if (argv.router) {
  let socket = zmq.socket('router')

  socket.connect(`${ADDRESS}:${argv.router}`)
  console.log(`${ID} is ready to route ${argv.router}`)

  socket.on('message', (envelope, buffer) => {
    const sender = unwrapMessage(buffer).slice(0, 3)
    socket.send([envelope, wrapMessage(`${ID}: Routing ${sender} with ${envelope}`)])
    console.log(`\nrouted ${sender}`)
  })
}
