import { Kafka } from "kafkajs"

/**
 * Kafka connection
 */
const kafka = new Kafka({
    clientId: "certificate",
    brokers: ["localhost:9094"],
})

const topic = "ISSUE_CERTIFICATE"
const consumer = kafka.consumer({ groupId: "certificate-group" })

const producer = kafka.producer()

async function run() {
    await producer.connect()
    await consumer.connect()
    await consumer.subscribe({ topic })
    await consumer.run({
        eachMessage: async({ topic, partition, message }) => {
            const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
            console.log(`- ${prefix} ${message.key}#${message.value}`)

            const payload = JSON.parse(message.value)

            // setTimeout(() => {
            producer.send({
                    topic: "CERTIFICATION_RESPONSE",
                    messages: [{
                        value: `Certificate of ${payload.user.name}, course ${payload.course}, generated!`,
                    }, ],
                })
                // }, 3000)
        },
    })
}

run().catch(console.error)