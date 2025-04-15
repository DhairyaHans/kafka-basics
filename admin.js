const {kafka} = require("./client")

async function init(){
    const admin = kafka.admin()
    console.log("Admin Connecting...")
    await admin.connect()
    console.log("Admin Connected")
    
    console.log("Creating Topic...")
    // Create a topic with 2 partitions
    await admin.createTopics({
        topics:[{
            topic: "rider-updates",
            numPartitions: 2
        }]
    })
    console.log("Topic Created [rider-updates]")

    console.log("Disconnecting...")
    await admin.disconnect()
}

init()
