const fs = require("fs");
const mqtt = require("mqtt");



// CONSTS FOR GENERATR CONFIGURATION
const BLINK_INTERVAL = 2200
const MAX_TIME_OFFSET = 1400
const TIME_TO_RUN = 10000
const NUMBER_OF_TAGS = 500
// not used right now, might be used in the future, determines if data is generated from a template or from a file
const TAG_SOURCE = process.env.TAG_SOURCE


// CONSTS FOR MQTT CONNECTION
const HOST = process.env.BROKER_HOST
const PORT = process.env.BROKER_PORT
const CONNECT_URL = `mqtts://${HOST}:${PORT}`
const CLIENT_ID = process.env.DEVICE_NAME
const USERNAME = process.env.USERNAME
const PASSWORD = process.env.PASSWORD
const CA_CERTS = fs.readFileSync(process.env.AWS_IOT_CA_PATH)
const CERTFILE = fs.readFileSync(process.env.AWS_IOT_CERT_PATH)
const KEYFILE = fs.readFileSync(process.env.AWS_IOT_KEY_PATH)

// TOPICS FOR MQTT PUBLISH
const TAG_TOPIC = process.env.TOPIC_TAG; 
const AGGR_TOPIC = process.env.TOPIC_AGGR 

// template

// CONFIGURE MQTT CLIENT
const client = mqtt.connect(CONNECT_URL, {
    CLIENT_ID,
    clear: true,
    connectTimeout: 2000,
    USERNAME,
    PASSWORD,
    recconectPeriod: 1000,
    ca: [CA_CERTS],
    cert: CERTFILE,
    key: KEYFILE
});

const TAG_TEMPLATE = fs.readFileSync("./tag_data_template.json", "utf-8")

// --------------------------------------------------------------------------
// FUNCTIONS
// --------------------------------------------------------------------------
async function main() {
    
    const tags = loadTags();
    const tags_with_offset = addTimeOffset(tags);
    sendTags(tags_with_offset);
}
    
// return an array of NUMBER_OF_TAGS tags with unique IDs, randomised blinkIndexes and coords (based on a template file)
function loadTags() {
    let ret_tags = []
    let template = JSON.parse(TAG_TEMPLATE);
    let new_tag;
    let id_count = 1000;
    
    while(ret_tags.length < NUMBER_OF_TAGS) {
        // deep copy of object
        new_tag = JSON.parse(JSON.stringify(template));
        // set unique id
        new_tag.tagId = id_count;
        id_count++;
        // set randomised blinkIndex
        new_tag.data.tagData.blinkIndex = Math.floor((Math.random() * 7000000) + 1000000)
        // set randomised coords
        new_tag.data.coordinates.x = Math.floor(Math.random() * 1000);
        new_tag.data.coordinates.y = Math.floor(Math.random() * 1000);
        new_tag.data.coordinates.z = Math.floor(Math.random() * 1000);
        ret_tags.push(new_tag);
    }
    return ret_tags;
    
}
// returns array [[single_tag_data1, time_offset1], [single_tag_data2, time_offset2], ...]
// this is done to add individual randomised time offset for sending tags
function addTimeOffset(tag_data) {
    let ret_tags = tag_data.map(tag => [tag, Math.floor(Math.random() * MAX_TIME_OFFSET)]);
    return ret_tags;
}

// Every 2,2 seconds starts sending tags with a randomised delay for each individual tag. The delay isn't longer than MAX_TIME_OFFSET
function sendTags(tag_data) {
    
    const end_time = Date.now() + TIME_TO_RUN;
    
    const self = setInterval(async () => {

        const interval_start = Date.now();
        // makes an array of promises, each promise sends a tag
        let promises = tag_data.map(tag => {
            const tag_object = tag[0];
            const tag_time_offset = tag[1];
            return new Promise((resolve, reject) => {
                setTimeout(() => {
                    
                    tag_object.timestamp = Date.now();
                    console.log(`Sent tagID: ${tag_object.tagId}, blinkIndex: ${tag_object.data.tagData.blinkIndex}, coords: (${tag_object.data.coordinates.x}, ${tag_object.data.coordinates.y}, ${tag_object.data.coordinates.z}) after ${tag_time_offset}ms`);

                    // send tag, increase blink index and change coordinates after sending
                    client.publish(TAG_TOPIC, JSON.stringify([tag_object]), {qos: 0}, error => {
                        if(error) {
                            console.log(error);
                        }
                        tag_object.data.tagData.blinkIndex++;
                        tag_object.data.coordinates.x += Math.floor(Math.random() * 3 - 1);
                        tag_object.data.coordinates.y += Math.floor(Math.random() * 3 - 1);
                        tag_object.data.coordinates.z += Math.floor(Math.random() * 3 - 1);
                        resolve();
                    });
                    
                }, tag_time_offset)
            })
        });
        
        await Promise.all(promises);
        console.log("All tags sent after " + Date.now() - interval_start + " ms");

        if(end_time < Date.now()) {
            clearInterval(self);
        }
    }, BLINK_INTERVAL);
}

// function sendMQTT(tag) {
//     client.publish(TAG_TOPIC, JSON.stringify([tag]), {qos: 0}, error => {
//         if(error) {
//             console.log(error);
//         }
//     });
// }


client.on("connect", () => {
        main();
});
