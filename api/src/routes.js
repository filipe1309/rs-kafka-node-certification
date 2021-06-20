import express from "express"
import { CompressionTypes } from "kafkajs"

const routes = express.Router()

routes.post("/certifications", async(req, res) => {
    const message = {
        user: { id: 1, name: "John Doe" },
        course: "Kafka with Node.js",
        grade: 10,
    }

    // Call micro service
    await req.producer.send({
        topic: "ISSUE_CERTIFICATE",
        compression: CompressionTypes.GZIP,
        messages: [
            { value: JSON.stringify(message) },
            {
                value: JSON.stringify({
                    ...message,
                    user: {...message.user, name: "Bob Dylan" },
                }),
            },
        ],
    })

    return res.json({ ok: true })
})

export default routes