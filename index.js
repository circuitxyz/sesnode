const mqtt = require('mqtt');
const Influx = require('influx');
let topic = '#'; // The topic to which the script subscribes, this is a multi-level wildcard, so messages on any topic are received
let connStates = {};
let powerStates = {};
let io = require('socket.io').listen(9000);

const influx = new Influx.InfluxDB({
    host: 'localhost',
    database: 'test_db', // change this value to your database name
    schema: [
        {
            measurement: 'energy_usage',
            fields: {
                power : Influx.FieldType.FLOAT,
                current: Influx.FieldType.FLOAT,
                voltage: Influx.FieldType.FLOAT,
                reactivePower: Influx.FieldType.FLOAT,
                apparentPower: Influx.FieldType.FLOAT,
                powerFactor: Influx.FieldType.FLOAT
            }, // tells InfluxDB about the structure of the data that will be inserted
            tags: [
                'device'
            ]
        }
    ]
});

let client = mqtt.connect('mqtt://localhost');
let sub = client.subscribe(topic);

let i = setInterval(function () {
    console.log('Client connected: ' + client.connected);
    if (client.connected){
        clearInterval(i);
    }
}, 5000); // This interval checks whether the server can connect to the MQTT broker when you run the script


client.on("message", function (topic,payload) {
    console.log('recieved data from ' + topic);
    try {
        console.log('payload: ' + payload);
        let device = topic.split('/')[1]; // The device that has sent a message gets parsed from the topic
        let subject =  topic.split('/')[2]; // The message type gets parsed from the topic
        if(subject === 'SENSOR') {
            payload = JSON.parse(payload); // The message gets parsed as JSON
            let point = {
                measurement: 'energy_usage',
                tags: {device: device},
                fields: {
                    power: payload.ENERGY.Power,
                    current: payload.ENERGY.Current,
                    voltage: payload.ENERGY.Voltage,
                    reactivePower: payload.ENERGY.ReactivePower,
                    apparentPower: payload.ENERGY.ApparentPower,
                    powerFactor: payload.ENERGY.Factor
                },
                timestamp: new Date()
            };
            influx.writePoints([point]).then(() => {
                console.log('Point written to Influx!'); // A new data point is created from the message data
            }).catch(err => {
                console.log(err);
            });
        } else if (subject === 'LWT'){
            connStates[device] = (payload === 'Online'); // The online state is updated
            io.sockets.emit('connStateChange', {device:device, status: connStates[device]});
            console.log(connStates);
        } else if (subject === 'STATE'){
            payload = JSON.parse(payload); // The message gets parsed as JSON
            powerStates[device] = (payload.POWER === "ON"); // The power state gets registered
            io.sockets.emit('powerStateChange', {device:device, status: powerStates[device]});
            console.log(powerStates);
        }
    } catch (e) {
        console.error("An error occurred!") // This message is shown when an unrecoverable error occurs
    }
});

/* --- SIGNALING, DNT --- */
io.sockets.on('connection', function (socket) {
   socket.send({connected:connStates, power:powerStates});
});

io.sockets.on('/switch', function (message) {
    client.publish('/cmnd/'+message.device+'/power toggle')
});



/*
recieved data from tele/sonoff/SENSOR
payload: {"Time":"1970-01-01T00:05:00","ENERGY":{"TotalStartTime":"1970-01-01T00:00:00","Total":0.000,"Yesterday":0.000,"Today":0.000,"Period":0,"Power":0,"ApparentPower":0,"ReactivePower":0,"Factor":0.00,"Voltage":0,"Current":0.000}}
 */
