const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const { DynamoDBDocumentClient, QueryCommand } = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = "TaipLocationData-dev";
const VEHICLE_IDS = ["0135", "0143", "0754", "0756", "0757"];

function clean_number(n) {
    if (n.startsWith("+")) n = n.replace("+", "");
    n = n.replace(/(\d{4})$/, ".$1");
    return Number(n);
}

async function getvehicles(rt) {
    const results = [];
    const url = "https://bustracker.gocarta.org/bustime/api/v3/getvehicles";
    const searchParams = new URLSearchParams();
    searchParams.append("format", "json");
    searchParams.append("key", process.env.CLEVER_BUS_TIME_API_KEY)
    searchParams.append("rt", rt);
    const response = await fetch(url + "?" + searchParams.toString());
    const data = await response.json();
    console.log("data:", data);
    return data['bustime-response']['vehicle'];
}

export const handler = async (event, context) => {
    // console.log("process.env:", process.env);
    // grab access token from headers;
    // console.log("headers:", event.headers);
    const { headers } = event;

    const credentials = {
        accessKeyId: process.env.MOCS_AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.MOCS_AWS_SECRET_ACCESS_KEY,
    };

    console.log(credentials);

    const ddbClient = new DynamoDBClient({
        region: "us-east-2",
        credentials
    });

    const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

    const seen_vehicle_ids = new Set();

    // scan dynamodb for each vehicle ID
    const geojson = {
        type: "FeatureCollection",
        features: []
    };

    const vehicles = await getvehicles(14);
    console.log(vehicles);

    vehicles.forEach(vehicle => {
        geojson.features.push({
            type: "Feature",
            properties: {
                vehicleId: vehicle.vid,
                latitude: vehicle.lat,
                longitude: vehicle.lon,
                timestamp: vehicle.tmstmp
            },
            geometry: {
                type: "Point",
                coordinates: [vehicle.lon, vehicle.lat]
            }
        });
        seen_vehicle_ids.add(Number(vehicle.vid));
    });

    for (let i = 0; i < VEHICLE_IDS.length; i++) {
        const vehicleId = VEHICLE_IDS[i];

        if (seen_vehicle_ids.has(Number(vehicleId))) {
            continue;
        }

        const queryParams = {
            TableName: TABLE_NAME,
            KeyConditionExpression: "vehicleId = :vId",
            ExpressionAttributeValues: { ":vId": vehicleId },
            // Set ScanIndexForward to false to sort by 'timestamp' descending (newest first)
            ScanIndexForward: false,             
            Limit: 1 
        };

        const data = await ddbDocClient.send(new QueryCommand(queryParams));

        const item = data.Items[0];
        if (item) {
            const latitude = clean_number(item.latitude);
            const longitude = clean_number(item.longitude);

            geojson.features.push({
                type: "Feature",
                properties: {
                    vehicleId,
                    latitude,
                    longitude,
                    timestamp: item.timestamp
                },
                geometry: {
                    type: "Point",
                    coordinates: [
                        longitude,
                        latitude
                    ]
                }
            });
        }
    }

    return {
        statusCode: 200,
        body: JSON.stringify(geojson),
        headers: {
            "Content-Type": "application/json"
        }
    };
};
  