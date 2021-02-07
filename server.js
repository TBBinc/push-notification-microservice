const { Expo } = require('expo-server-sdk');
const solace = require('solclientjs');
const cassandra = require('cassandra-driver');

if (process.env.NODE_ENV !== 'production') {
  require('dotenv').config();
}
// Create a new Expo SDK client
// optionally providing an access token if you have enabled push security
let expo = new Expo({ accessToken: process.env.EXPO_ACCESS_TOKEN });

// Create the messages that you want to send to clients


// let chunks = expo.chunkPushNotifications(messages);
// let tickets = [];
// (async () => {
//   // Send the chunks to the Expo push notification service. There are
//   // different strategies you could use. A simple one is to send one chunk at a
//   // time, which nicely spreads the load out over time:
//   for (let chunk of chunks) {
//     try {
//       let ticketChunk = await expo.sendPushNotificationsAsync(chunk);
//       console.log(ticketChunk);
//       tickets.push(...ticketChunk);
//       // NOTE: If a ticket contains an error code in ticket.details.error, you
//       // must handle it appropriately. The error codes are listed in the Expo
//       // documentation:
//       // https://docs.expo.io/push-notifications/sending-notifications/#individual-errors
//     } catch (error) {
//       console.error(error);
//     }
//   }
// })();

var TopicSubscriber = function (solaceModule, topicName) {
    'use strict';
    var solace = solaceModule;
    var subscriber = {};
    subscriber.session = null;
    subscriber.topicName = topicName;
    subscriber.subscribed = false;

    // Logger
    subscriber.log = function (line) {
        var now = new Date();
        var time = [('0' + now.getHours()).slice(-2), ('0' + now.getMinutes()).slice(-2),
            ('0' + now.getSeconds()).slice(-2)];
        var timestamp = '[' + time.join(':') + '] ';
        console.log(timestamp + line);
    };

    subscriber.log('\n*** Subscriber to topic "' + subscriber.topicName + '" is ready to connect ***');

    // main function
    subscriber.run = () => {
        subscriber.connect();
    };

    // Establishes connection to Solace message router
    subscriber.connect = ()=> {
        if (subscriber.session !== null) {
            subscriber.log('Already connected and ready to subscribe.');
            return;
        }

        var hosturl = process.env.URL;
        subscriber.log('Connecting to Solace message router using url: ' + hosturl);
        var username = process.env.USERNAME;
        subscriber.log('Client username: ' + username);
        var vpn = process.env.VPNNAME;
        subscriber.log('Solace message router VPN name: ' + vpn);
        var pass = process.env.PASSWORD;

        const client = new cassandra.Client({
            cloud: { secureConnectBundle: process.env.PATH_TO_ZIP },
            credentials: { username: process.env.ASTRA_DB_USERNAME , password: process.env.ASTRA_DB_PASSWORD }
        });
        // create session
        try {
            subscriber.session = solace.SolclientFactory.createSession({
                // solace.SessionProperties
                url:      hosturl,
                vpnName:  vpn,
                userName: username,
                password: pass,
            });
        } catch (error) {
            subscriber.log(error.toString());
        }
        // define session event listeners
        subscriber.session.on(solace.SessionEventCode.UP_NOTICE, function (sessionEvent) {
            subscriber.log('=== Successfully connected and ready to subscribe. ===');
            subscriber.subscribe();
        });
        subscriber.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, function (sessionEvent) {
            subscriber.log('Connection failed to the message router: ' + sessionEvent.infoStr +
                ' - check correct parameter values and connectivity!');
        });
        subscriber.session.on(solace.SessionEventCode.DISCONNECTED, function (sessionEvent) {
            subscriber.log('Disconnected.');
            subscriber.subscribed = false;
            if (subscriber.session !== null) {
                subscriber.session.dispose();
                subscriber.session = null;
            }
        });
        subscriber.session.on(solace.SessionEventCode.SUBSCRIPTION_ERROR, function (sessionEvent) {
            subscriber.log('Cannot subscribe to topic: ' + sessionEvent.correlationKey);
        });
        subscriber.session.on(solace.SessionEventCode.SUBSCRIPTION_OK, function (sessionEvent) {
            if (subscriber.subscribed) {
                subscriber.subscribed = false;
                subscriber.log('Successfully unsubscribed from topic: ' + sessionEvent.correlationKey);
            } else {
                subscriber.subscribed = true;
                subscriber.log('Successfully subscribed to topic: ' + sessionEvent.correlationKey);
                subscriber.log('=== Ready to receive messages. ===');
            }
        });
        // define message event listener
        subscriber.session.on(solace.SessionEventCode.MESSAGE, function (message) {

            let messages = [];
            let somePushTokens = []

            function calcReq (row, data) {
                console.log((data.latlocation + 0.05))
                console.log(row.latlocation < (data.latlocation + 0.05))
                return row.latlocation < (data.latlocation + 0.05) && row.latlocation > (data.latlocation - 0.05) && row.longlocation < (data.latlocation + 0.05) && row.longlocation > (data.latlocation - 0.05) && row.age > data.age;
            }

            client.eachRow(
                'SELECT * FROM users.location_info',
                [],
                (n, row) => {
                  // The callback will be invoked per each row as soon as they are received
                    somePushTokens.push(row.pushtoken)
                },
                err => { 
                  // This function will be invoked when all rows where consumed or an error was encountered  
                  console.log(somePushTokens)
                
                    
                  for (let pushToken of somePushTokens) {
                    // Each push token looks like ExponentPushToken[xxxxxxxxxxxxxxxxxxxxxx]
                  
                    // Check that all your push tokens appear to be valid Expo push tokens
                    if (!Expo.isExpoPushToken(pushToken)) {
                      console.error(`Push token ${pushToken} is not a valid Expo push token`);
                      continue;
                    }
                    // Construct a message (see https://docs.expo.io/push-notifications/sending-notifications/)
                    try {
                      messages.push({
                          to: pushToken,
                          sound: 'default',
                          body: 'Vaccine is avaliable at the following location until 21:00!',
                          data: { longdata: 'data', latdata: 'data'},
                        })
                    } catch (error) {
                      console.error(error);
                    }
                  }

                let chunks = expo.chunkPushNotifications(messages);
                let tickets = [];
                (async () => {
                  // Send the chunks to the Expo push notification service. There are
                  // different strategies you could use. A simple one is to send one chunk at a
                  // time, which nicely spreads the load out over time:
                  for (let chunk of chunks) {
                    try {
                      let ticketChunk = await expo.sendPushNotificationsAsync(chunk);
                      console.log(ticketChunk);
                      tickets.push(...ticketChunk);
                      // NOTE: If a ticket contains an error code in ticket.details.error, you
                      // must handle it appropriately. The error codes are listed in the Expo
                      // documentation:
                      // https://docs.expo.io/push-notifications/sending-notifications/#individual-errors
                    } catch (error) {
                      console.error(error);
                    }
                  }
                })();

                }
              );
            
        });
        // connect the session
        try {
            subscriber.session.connect();
        } catch (error) {
            subscriber.log(error.toString());
        }
    };

    // Subscribes to topic on Solace message router
    subscriber.subscribe = function () {
        if (subscriber.session !== null) {
            if (subscriber.subscribed) {
                subscriber.log('Already subscribed to "' + subscriber.topicName
                    + '" and ready to receive messages.');
            } else {
                subscriber.log('Subscribing to topic: ' + subscriber.topicName);
                try {
                    subscriber.session.subscribe(
                        solace.SolclientFactory.createTopicDestination(subscriber.topicName),
                        true, // generate confirmation when subscription is added successfully
                        subscriber.topicName, // use topic name as correlation key
                        10000 // 10 seconds timeout for this operation
                    );
                } catch (error) {
                    subscriber.log(error.toString());
                }
            }
        } else {
            subscriber.log('Cannot subscribe because not connected to Solace message router.');
        }
    };

    subscriber.exit = function () {
        subscriber.unsubscribe();
        subscriber.disconnect();
        setTimeout(function () {
            process.exit();
        }, 1000); // wait for 1 second to finish
    };

    // Unsubscribes from topic on Solace message router
    subscriber.unsubscribe = function () {
        if (subscriber.session !== null) {
            if (subscriber.subscribed) {
                subscriber.log('Unsubscribing from topic: ' + subscriber.topicName);
                try {
                    subscriber.session.unsubscribe(
                        solace.SolclientFactory.createTopicDestination(subscriber.topicName),
                        true, // generate confirmation when subscription is removed successfully
                        subscriber.topicName, // use topic name as correlation key
                        10000 // 10 seconds timeout for this operation
                    );
                } catch (error) {
                    subscriber.log(error.toString());
                }
            } else {
                subscriber.log('Cannot unsubscribe because not subscribed to the topic "'
                    + subscriber.topicName + '"');
            }
        } else {
            subscriber.log('Cannot unsubscribe because not connected to Solace message router.');
        }
    };

    // Gracefully disconnects from Solace message router
    subscriber.disconnect = function () {
        subscriber.log('Disconnecting from Solace message router...');
        if (subscriber.session !== null) {
            try {
                subscriber.session.disconnect();
            } catch (error) {
                subscriber.log(error.toString());
            }
        } else {
            subscriber.log('Not connected to Solace message router.');
        }
    };

    return subscriber;
};

// Initialize factory with the most recent API defaults
var factoryProps = new solace.SolclientFactoryProperties();
factoryProps.profile = solace.SolclientFactoryProfiles.version10;
solace.SolclientFactory.init(factoryProps);

// create the subscriber, specifying the name of the subscription topic
var subscriber = new TopicSubscriber(solace, 'TBBinc/signal');



// subscribe to messages on Solace message router
subscriber.run();

// wait to be told to exit
subscriber.log('Press Ctrl-C to exit');
process.stdin.resume();

process.on('SIGINT', function () {
    'use strict';
    subscriber.exit();
});