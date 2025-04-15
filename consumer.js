const {kafka} = require("./client")

// Get the group name from the arguments
const group = process.argv[2] || "user-1"

async function init(){
    const consumer = kafka.consumer({
        groupId: group
    })

    await consumer.connect()

    await consumer.subscribe({
        topic: "rider-updates",
        fromBeginning: true
    })

    await consumer.run({
        eachMessage: async ({topic, partition, message}) => {
            console.log(`${group}: [${topic}]: PART: ${partition}, Received message: ${message.value.toString()}`)
        }
    })
}

init()
