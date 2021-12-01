module.exports = function(RED) {
    const mqtt = require("mqtt");
    const fs   = require("fs");


    function ConnectionNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.on('close', function(done) {
            DynMQTT.closeClients();
            done();
        });
        node.on('input', function(msg, send, done) {
            let client = DynMQTT.createClient(msg,function(new_status){ 
                node.status(new_status.summary);
                node.send({ 
                    "client_id": new_status.client_id,
                    "payload": new_status.payload });
            })
            done();
        });
    }

    function DisconnectNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.on('input', function(msg, send, done) {
            let client = DynMQTT.getClient(msg.client_id)
            if (client) {
                client.close();
                done();
            } else {
                done("Client not found!");
            }
        });
    }

    function PublishNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.on('input', function(msg, send, done) {
            let client = DynMQTT.getClient(msg.client_id);
            if (client && (client.status == "connected")) {
                client.publish(msg.topic,msg.payload);
                done();
            } else {
                node.warn("Client not known or not connected - sending msg back for re-looping");
                send(msg);
                done();
            }
        });
    }


    function SubscribeNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.on('input', function(msg, send, done) {
            let client = DynMQTT.getClient(msg.client_id);
            if (client) {
                client.subscribe(msg.topic,(message) => { 
                    send({
                        "payload": JSON.parse(message.toString()),
                        "client_id": client.client_id,
                        "topic": msg.topic
                    })
                })
                done();
            } else
                done("Client " + msg.client_id + " for Subsription not known!");
        });
    }

    function StatusNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.on('input', function(msg, send, done) {
                send({ "payload": DynMQTT.listClients()})
                done();
        });
    }


    function UnsubscribeNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.on('input', function(msg, send, done) {
            let client = DynMQTT.getClient(msg.client_id);
            if (client) {
                client.unsubscribe(msg.topic);
                done();
            } else
                done("Client " + msg.client_id + " for Un-Subsription not known!");
        });
    }

    //--Backend class to maintain the MQTT clients
    class DynMQTT{
        static clients = {};
        static client_stats = {};

        static matchTopics(received,subscribtions) {
            for (subscribed in subscribtions) {
                subscribed = subscribed.replace(/\+/g,'[^\/]+')
                subscribed = subscribed.replace(/\#/g,'.*')
                subscribed = subscribed.replace(/\$/g,'\\$')
                subscribed = subscribed.replace(/\//g,'\\/')
                subscribed = '^' + subscribed + '$'
            
                return received.match(subscribed) != null
            }
        }
        
        static closeClients() {
            for (const [id, client] of Object.entries(DynMQTT.clients))
                client.close();
            DynMQTT.clients = {};
        }

        static createClient(config, status_callback) {
            const client_id = config.payload.client_id;
            if (DynMQTT.clients.hasOwnProperty(client_id)) {
                console.log("Client already known: " + client_id);
                //ToDo: check health status of client
            } else {
                console.log("Creating new client: " + client_id);
                DynMQTT.clients[client_id] = new DynMQTT(client_id,config.payload,status_callback);
            }
            return DynMQTT.clients[client_id]
        }

        static listClients() {
            let ret = {};
            
            for (const [id, client] of Object.entries(DynMQTT.clients)) {
                ret[id] = { "status": client.status, "subscriptions": [] }

                for (let topic of Object.keys(client.subscriptions))
                    ret[id]["subscriptions"].push(topic);
            }
            return ret;
        }

        static getClient(client_id) {
            if (DynMQTT.clients.hasOwnProperty(client_id)) {
                //console.log("Client " + client_id + " is known");
                //ToDo: check health status of client
                return DynMQTT.clients[client_id]
            } else {
                //console.error("Client " + client_id + " is not known");
                return false;
            }
        }

        constructor(client_id,config,status_callback) {
            this.client_id      = client_id;
            this.host           = config.host;
            this.key_path       = config.key  || false;
            this.cert_path      = config.cert || false;
            this.ca_path        = config.ca   || false;
            this.reconnect_t    = config.reconnect || 0;
            this.subscriptions  = {};
            this.status         = false;
            this.status_callback= status_callback;
           
            this.change_status('initiated');

            if (this.cert_path) {
                const options = {
                    clientId: this.client_id,
                    rejectUnauthorized: false,
                    key: fs.readFileSync(this.key_path),
                    cert: fs.readFileSync(this.cert_path),
                    ca: [ fs.readFileSync(this.ca_path) ],
                    reconnectPeriod: this.reconnect_t
                }
                this.connection     = mqtt.connect('mqtts://' + this.host, options);
            } else {
                const options = {
                    clientId: this.client_id,
                    reconnectPeriod: this.reconnect_t
                }
                this.connection     = mqtt.connect('mqtt://' + this.host, options);
            }

            /*Client events*/
            this.connection.on('connect', () => {
                this.change_status("connected");
            });

            this.connection.on('error', (err) => {
                this.change_status("error");
                console.error("error on MQTT connection: " + err);
                this.status_callback({fill:"red",shape:"dot",text:err});
            });

            this.connection.on('reconnect', () => {
                this.change_status("reconnecting");
            });

            this.connection.on('disconnect', () => {
                this.change_status("disconnect");
            });

            this.connection.on('offline', () => {
                this.change_status("offline");
            });

            this.connection.on('close', () => {
                this.change_status("closed");
                for (let topic of Object.keys(this.subscriptions))
                    this.unsubscribe(topic);
            });

            this.connection.on('message', (topic, message) => {
                if (DynMQTT.matchTopics(topic,Object.keys(this.subscriptions))) { 
                    this.subscriptions[topic](message);
                } else {
                    console.error("Rec msg w/o subscription on " + topic);
                }
            });

        }

        change_status(status) {
            let ret = {
                "client_id"  : this.client_id,
                "summary"    : "",
                "payload"    : {
                    "status_new" : status,
                    "status_old" : this.status
                }
            };

            console.log(this.client_id + " -> " + status);
            // --- Take care of the previous status (only if it is set)
            if (this.status) {
                if (DynMQTT.client_stats.hasOwnProperty(this.status))
                    DynMQTT.client_stats[this.status]--;
                else 
                    DynMQTT.client_stats[this.status] = 0;
            }

            // --- Take care of the new status
            this.status = status;

            if (this.status != 'closed') { // -- we don't count closed (they will never decrement if reconnect is disabled)
                if (DynMQTT.client_stats.hasOwnProperty(this.status))
                    DynMQTT.client_stats[this.status]++;
                else
                    DynMQTT.client_stats[this.status] = 1;
            }                    

            let txt = "";
            for (const [name, count] of Object.entries(DynMQTT.client_stats)) 
                if (parseInt(count) > 0)
                    txt = txt + name + ":" + parseInt(count) + " ";
                else
                    delete DynMQTT.client_stats[name]; // -- auto-sanitize 
            ret.summary = {fill:"green",shape:"dot",text:txt};
            this.status_callback(ret);
        }

        subscribe(topic,callback) {
            if (this.subscriptions.hasOwnProperty(topic)) {
                //console.log("Already subscribed");
            } else {
                //console.log("Creating new subscription");
                this.subscriptions[topic] = callback;
                this.connection.subscribe(topic, function (err, result) {
                    if (!err){
                        console.log("Subscribed to:" + result[0].topic);
                    }
                    else console.error(err);
                });
            }
        }

        unsubscribe(topic) {
            if (this.subscriptions.hasOwnProperty(topic)) {
                console.log("Removing subscription to:" + topic);
                this.connection.unsubscribe(topic,(err) => {
                    if (err)
                        console.error(err);
                });
                delete this.subscriptions[topic];
            } else {
                //console.log("Not subscribed");
            }
        }

        publish(topic,payload) {
            let send_payload = payload; // covers for payloads of type Buffer (Binary)

            if(typeof payload == "object") 
                send_payload=JSON.stringify(payload);
            else if(typeof payload == "number") 
                send_payload=payload.toString(10);

            this.connection.publish(topic,send_payload);
        }

        close(){
            this.connection.end();
            this.subscriptions = {};
            delete DynMQTT.clients[this.client_id];
        }

    }

    RED.nodes.registerType("mqtt-flex-connect",ConnectionNode);
    RED.nodes.registerType("mqtt-flex-disconnect",DisconnectNode);
    RED.nodes.registerType("mqtt-flex-publish",PublishNode);
    RED.nodes.registerType("mqtt-flex-subscribe",SubscribeNode);
    RED.nodes.registerType("mqtt-flex-unsubscribe",UnsubscribeNode);
    RED.nodes.registerType("mqtt-flex-status",StatusNode);
}