const {kafka} = require("./client")
const readline = require("readline")

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
})

// Basic Init function to send a message to the topic "rider-updates"
async function init(){
    const producer = kafka.producer()

    console.log("Producer Connecting...")
    await producer.connect()
    console.log("Producer Connected Successfully")
    
    await producer.send({
        topic: "rider-updates",
        messages: [
            {
                key: "location-update", 
                value:JSON.stringify({
                    name:"Mony Stank",
                    location:"SOUTH"
                }),
                paritition: 0
            }
        ]
    })

    console.log("Message Sent Successfully")
    console.log("Disconnecting...")
    await producer.disconnect()
    console.log("Producer Disconnected Successfully")
}

// init()

// Takes input from User regarding the message to be sent
async function smartInit(){
    const producer = kafka.producer()

    console.log("Producer Connecting...")
    await producer.connect()
    console.log("Producer Connected Successfully")
    
    rl.setPrompt(">")
    rl.prompt()

    // line format -> <name> <location>
    // e.g. Mony SOUTH
    rl.on("line", async (line) => {
        const [riderName, location] = line.split(" ")   
        await producer.send({
            topic: "rider-updates",
            messages: [
                {
                    key: "location-update", 
                    value:JSON.stringify({
                        name: riderName,
                        location: location
                    }),
                    partition: location.toLowerCase() === "north" ? 0 : 1
                }
            ]
        })
        console.log("Message Sent Successfully")
    }).on("close", async () => {
        console.log("Disconnecting...")
        await producer.disconnect()
        console.log("Producer Disconnected Successfully")
    })
   
}

smartInit()