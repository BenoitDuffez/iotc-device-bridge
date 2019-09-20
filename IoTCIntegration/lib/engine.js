/*!
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License.
 */

const crypto = require('crypto');
const request = require('request-promise-native');
const Device = require('azure-iot-device');
const DeviceTransport = require('azure-iot-device-http');
const util = require('util');

const StatusError = require('../error').StatusError;

const registrationHost = 'global.azure-devices-provisioning.net';
const registrationSasTtl = 3600; // 1 hour
const registrationApiVersion = `2018-11-01`;
const registrationStatusQueryAttempts = 10;
const registrationStatusQueryTimeout = 2000;
const minDeviceRegistrationTimeout = 60*1000; // 1 minute

const deviceCache = {};

/**
 * Forwards external telemetry messages for IoT Central devices.
 * @param {{ idScope: string, primaryKeyUrl: string, log: Function, getSecret: (context: Object, secretUrl: string) => string }} context 
 * @param {{ deviceId: string }} device 
 * @param {{ [field: string]: number }} measurements 
 */
module.exports = async function (context, loraMessage) {
    if (!loraMessage.endDevice) {
        throw new StatusError('endDevice object missing');
    }
    if (!loraMessage.endDevice.devEui || !/^[A-Za-z0-9]{16}$/.test(loraMessage.endDevice.devEui)) {
        throw new StatusError('Invalid format: devEui must be a 16 digit hex string.', 400);
    }

    if (!loraMessage.payload) {
        throw new StatusError('Invalid format: invalid payload.', 400);
    }

    const date = new Date(loraMessage.recvTime);

    const client = Device.Client.fromConnectionString(await getDeviceConnectionString(context, loraMessage.endDevice), DeviceTransport.Http);

    try {
        const payload = Decoder(hexToBytes(loraMessage.payload))
        context.log("payload decoded: ", payload);

        var loc = null;
        if (payload.hasOwnProperty('gps_quality')
            && (payload.latitude < 90 && payload.latitude > -90)
            && (payload.longitude < 180 && payload.longitude > -180)
            && (payload.latitude != 0 && payload.longitude != 0)) {
            loc = {
                'lat': payload.latitude,
                'lon': payload.longitude
            };
            context.log("Location: ", loc);
        }

        const iotMessage = {
            payload: loraMessage.payload,
            fCntUp: loraMessage.fCntUp,
            fCntDown: loraMessage.fCntDown,
            ulFrequency: loraMessage.ulFrequency,
            confirmed: loraMessage.confirmed,
            encrypted: loraMessage.encrypted,
            adr: loraMessage.adr,
            fPort: loraMessage.fPort,
            location: loc
        };
        const message = new Device.Message(JSON.stringify(iotMessage));
        message.properties.add('iothub-creation-time-utc', date.toString());

        if (payload.temperature && payload.temperature > -30 && payload.temperature < 60) {
            message.properties.add('temperature', payload.temperature);
        }
        if (payload.battery_level) {
            message.properties.add('battery_level', payload.battery_level);
        }
        if (loc != null) {
            message.properties.add('location', loc);
        }

        context.log("Telemetry: ", message.properties);

        await util.promisify(client.open.bind(client))();
        context.log('[HTTP] Sending telemetry for device', loraMessage.endDevice.devEui);
        await util.promisify(client.sendEvent.bind(client))(message);
        await util.promisify(client.close.bind(client))();
    } catch (e) {
        // If the device was deleted, we remove its cached connection string
        if (e.name === 'DeviceNotFoundError' && deviceCache[loraMessage.endDevice.devEui]) {
            delete deviceCache[loraMessage.endDevice.devEui].connectionString;
        }

        throw new Error(`Unable to send telemetry for device ${loraMessage.endDevice.devEui}: ${e.message}`);
    }
};

/**
 * @returns true if a measurement is a location.
 */
function isLocation(measurement) {
    if (!measurement || typeof measurement !== 'object' || typeof measurement.lat !== 'number' || typeof measurement.lon !== 'number') {
        return false;
    }

    if ('alt' in measurement && typeof measurement.alt !== 'number') {
        return false;
    }

    return true;
}

async function getDeviceConnectionString(context, device) {
    const devEui = device.devEui;

    if (deviceCache[devEui] && deviceCache[devEui].connectionString) {
        return deviceCache[devEui].connectionString;
    }

    const connStr = `HostName=${await getDeviceHub(context, device)};DeviceId=${devEui};SharedAccessKey=${await getDeviceKey(context, devEui)}`;
    deviceCache[devEui].connectionString = connStr;
    return connStr;
}

/**
 * Registers this device with DPS, returning the IoT Hub assigned to it.
 */
async function getDeviceHub(context, device) {
    const devEui = device.devEui;
    const now = Date.now();

    // A 1 minute backoff is enforced for registration attempts, to prevent unauthorized devices
    // from trying to re-register too often.
    if (deviceCache[devEui] && deviceCache[devEui].lasRegisterAttempt && (now - deviceCache[devEui].lasRegisterAttempt) < minDeviceRegistrationTimeout) {
        const backoff = Math.floor((minDeviceRegistrationTimeout - (now - deviceCache[devEui].lasRegisterAttempt)) / 1000);
        throw new StatusError(`Unable to register device ${devEui}. Minimum registration timeout not yet exceeded. Please try again in ${backoff} seconds`, 403);
    }

    deviceCache[devEui] = {
        ...deviceCache[devEui],
        lasRegisterAttempt: Date.now()
    }

    const sasToken = await getRegistrationSasToken(context, devEui);

    const registrationOptions = {
        url: `https://${registrationHost}/${context.idScope}/registrations/${devEui}/register?api-version=${registrationApiVersion}`,
        method: 'PUT',
        json: true,
        headers: { Authorization: sasToken },
        body: { registrationId: devEui }
    };

    try {
        context.log('[HTTP] Initiating device registration');
        const response = await request(registrationOptions);

        if (response.status !== 'assigning' || !response.operationId) {
            throw new Error('Unknown server response');
        }

        const statusOptions = {
            url: `https://${registrationHost}/${context.idScope}/registrations/${devEui}/operations/${response.operationId}?api-version=${registrationApiVersion}`,
            method: 'GET',
            json: true,
            headers: { Authorization: sasToken }
        };

        // The first registration call starts the process, we then query the registration status
        // every 2 seconds, up to 10 times.
        for (let i = 0; i < registrationStatusQueryAttempts; ++i) {
            await new Promise(resolve => setTimeout(resolve, registrationStatusQueryTimeout));

            context.log('[HTTP] Querying device registration status');
            const statusResponse = await request(statusOptions);

            if (statusResponse.status === 'assigning') {
                continue;
            } else if (statusResponse.status === 'assigned' && statusResponse.registrationState && statusResponse.registrationState.assignedHub) {
                return statusResponse.registrationState.assignedHub;
            } else if (statusResponse.status === 'failed' && statusResponse.registrationState && statusResponse.registrationState.errorCode === 400209) {
                throw new StatusError('The device may be unassociated or blocked', 403);
            } else {
                throw new Error('Unknown server response');
            }
        }

        throw new Error('Registration was not successful after maximum number of attempts');
    } catch (e) {
        throw new StatusError(`Unable to register device ${devEui}: ${e.message}`, e.statusCode);
    }
}

async function getRegistrationSasToken(context, devEui) {
    const uri = encodeURIComponent(`${context.idScope}/registrations/${devEui}`);
    const ttl = Math.round(Date.now() / 1000) + registrationSasTtl;
    const signature = crypto.createHmac('sha256', new Buffer(await getDeviceKey(context, devEui), 'base64'))
        .update(`${uri}\n${ttl}`)
        .digest('base64');
    return`SharedAccessSignature sr=${uri}&sig=${encodeURIComponent(signature)}&skn=registration&se=${ttl}`;
}

/**
 * Computes a derived device key using the primary key.
 */
async function getDeviceKey(context, devEui) {
    if (deviceCache[devEui] && deviceCache[devEui].deviceKey) {
        return deviceCache[devEui].deviceKey;
    }

    const key = crypto.createHmac('SHA256', Buffer.from(await context.getSecret(context, context.primaryKeyUrl), 'base64'))
        .update(devEui)
        .digest()
        .toString('base64');

    deviceCache[devEui].deviceKey = key;
    return key;
}


// Adeunis decoder, thanks to TTN: https://www.thethingsnetwork.org/labs/story/payload-decoder-for-adeunis-field-test-device-ttn-mapper-integration#
function Decoder(bytes) {
    // Functions
    function parseCoordinate(raw_value, coordinate) {
        // This function parses a coordinate payload part into 
        // dmm and ddd 
        var raw_itude = raw_value;
        var temp = "";

        // Degree section
        var itude_string = ((raw_itude >> 28) & 0xF).toString();
        raw_itude <<= 4;

        itude_string += ((raw_itude >> 28) & 0xF).toString();
        raw_itude <<= 4;

        coordinate.degrees += itude_string;
        itude_string += "°";

        // Minute section
        temp = ((raw_itude >> 28) & 0xF).toString();
        raw_itude <<= 4;

        temp += ((raw_itude >> 28) & 0xF).toString();
        raw_itude <<= 4;
        itude_string += temp;
        itude_string += ".";
        coordinate.minutes += temp;

        // Decimal section
        temp = ((raw_itude >> 28) & 0xF).toString();
        raw_itude <<= 4;

        temp += ((raw_itude >> 28) & 0xF).toString();
        raw_itude <<= 4;

        itude_string += temp;
        coordinate.minutes += ".";
        coordinate.minutes += temp;

        return itude_string;
    }

    function parseLatitude(raw_latitude, coordinate) {
        var latitude = parseCoordinate(raw_latitude, coordinate);
        latitude += ((raw_latitude & 0xF0) >> 4).toString();
        coordinate.minutes += ((raw_latitude & 0xF0) >> 4).toString();

        return latitude;
    }

    function parseLongitude(raw_longitude, coordinate) {
        var longitude = (((raw_longitude >> 28) & 0xF)).toString();
        coordinate.degrees = longitude;
        longitude += parseCoordinate(raw_longitude << 4, coordinate);

        return longitude;
    }

    function addField(field_no, payload) {
        switch (field_no) {
            // Presence of temperature information
            case 0:
                payload.temperature = bytes[bytes_pos_] & 0x7F;
                // Temperature is negative
                if ((bytes[bytes_pos_] & 0x80) > 0) {
                    payload.temperature -= 128;
                }
                bytes_pos_++;
                break;
            // Transmission triggered by the accelerometer
            case 1:
                payload.trigger = "accelerometer";
                break;
            // Transmission triggered by pressing pushbutton 1
            case 2:
                payload.trigger = "pushbutton";
                break;
            // Presence of GPS information
            case 3:
                // GPS Latitude
                // An object is needed to handle and parse coordinates into ddd notation
                var coordinate = {};
                coordinate.degrees = "";
                coordinate.minutes = "";

                var raw_value = 0;
                raw_value |= bytes[bytes_pos_++] << 24;
                raw_value |= bytes[bytes_pos_++] << 16;
                raw_value |= bytes[bytes_pos_++] << 8;
                raw_value |= bytes[bytes_pos_++];

                payload.lati_hemisphere = (raw_value & 1) == 1 ? "South" : "North";
                payload.latitude_dmm = payload.lati_hemisphere.charAt(0) + " ";
                payload.latitude_dmm += parseLatitude(raw_value, coordinate);
                payload.latitude = (parseFloat(coordinate.degrees) + parseFloat(coordinate.minutes) / 60) * ((raw_value & 1) == 1 ? -1.0 : 1.0);

                // GPS Longitude
                coordinate.degrees = "";
                coordinate.minutes = "";
                raw_value = 0;
                raw_value |= bytes[bytes_pos_++] << 24;
                raw_value |= bytes[bytes_pos_++] << 16;
                raw_value |= bytes[bytes_pos_++] << 8;
                raw_value |= bytes[bytes_pos_++];

                payload.long_hemisphere = (raw_value & 1) == 1 ? "West" : "East";
                payload.longitude_dmm = payload.long_hemisphere.charAt(0) + " ";
                payload.longitude_dmm += parseLongitude(raw_value, coordinate);
                payload.longitude = (parseFloat(coordinate.degrees) + parseFloat(coordinate.minutes) / 60) * ((raw_value & 1) == 1 ? -1.0 : 1.0);

                // GPS Quality
                raw_value = bytes[bytes_pos_++];

                switch ((raw_value & 0xF0) >> 4) {
                    case 1:
                        payload.gps_quality = "Good";
                        break;
                    case 2:
                        payload.gps_quality = "Average";
                        break;
                    case 3:
                        payload.gps_quality = "Poor";
                        break;
                    default:
                        payload.gps_quality = (raw_value >> 4) & 0xF;
                        break;
                }
                payload.hdop = (raw_value >> 4) & 0xF;

                // Number of satellites
                payload.sats = raw_value & 0xF;

                break;
            // Presence of Uplink frame counter
            case 4:
                payload.ul_counter = bytes[bytes_pos_++];
                break;
            // Presence of Downlink frame counter
            case 5:
                payload.dl_counter = bytes[bytes_pos_++];
                break;
            // Presence of battery level information
            case 6:
                payload.battery_level = bytes[bytes_pos_++] << 8;
                payload.battery_level |= bytes[bytes_pos_++];
                break;
            // Presence of RSSI and SNR information
            case 7:
                // RSSI
                payload.rssi_dl = bytes[bytes_pos_++];
                payload.rssi_dl *= -1;

                // SNR
                payload.snr_dl = bytes[bytes_pos_] & 0x7F;
                if ((bytes[bytes_pos_] & 0x80) > 0) {
                    payload.snr_dl -= 128;
                }
                bytes_pos_++;
                break;
            default:
                // Do nothing
                break;
        }
    }

    // Declaration & initialization
    var status_ = bytes[0];
    var bytes_len_ = bytes.length;
    var bytes_pos_ = 1;
    var i = 0;
    var payload = {};

    // Get raw payload
    var temp_hex_str = ""
    payload.payload = "";
    for (var j = 0; j < bytes_len_; j++) {
        temp_hex_str = bytes[j].toString(16).toUpperCase();
        if (temp_hex_str.length == 1) {
            temp_hex_str = "0" + temp_hex_str;
        }
        payload.payload += temp_hex_str;
    }

    // Get payload values
    do {
        // Check status, whether a field is set
        if ((status_ & 0x80) > 0) {
            addField(i, payload);
        }
        i++;
    }

    while (((status_ <<= 1) & 0xFF) > 0);
    return payload;
}

// Convert a hex string to a byte array
function hexToBytes(hex) {
    for (var bytes = [], c = 0; c < hex.length; c += 2)
    bytes.push(parseInt(hex.substr(c, 2), 16));
    return bytes;
}

