# Evently REST API

This project provides the REST interface for evently's [ledger-db](https://github.com/evently-cloud/pg-ledger).

## Prerequisites
- Node.js 20+
- A running PostgreSQL instance with [ledger-db](https://github.com/evently-cloud/pg-ledger) installed. 
- An environment file at `./env/evently.env`

## Configure
Create the environment file at `./env/evently.env` and fill in these values:

```bash
DB_DATABASE=evently_dev
DB_USER=evently
DB_PASSWORD=<evently account password>
DB_HOST=<database address>
```

## Launch
The REST API has two launch options. One is with `npm` and the other is with node directly.

#### NPM

The advantage of the NPM route is that the project does not need to be built. From the commandline, run this command:

```bash
npm run launch
```

### Node Directly

In this approach, the project needs to be compiled before running. This is useful for some deployment environments that do not want to use npm to run applications.

First build the application:

```bash
npm build
```

Then, launch the REST API with this command:

```bash
node --env-file ./env/evently.env dist/src/index.js
```

## Conceptual Model

Evently stores events in ledgers. These events must be registered before use to prevent incorrect events from being appended.

Once registered, events are appended to the ledger, and then retrieved using Selectors. Selectors have different query mechanisms to find exactly the events required. They also act as atomic append conditionals to safely control the state changes an event signifies.

## Usage

The REST API, once launched, is located at `http://127.0.0.1:4802`. The API utilizes the Hypermedia style, which means the responses from a request include links to relevant data and actions. One can use cURL or other tools to "look around" and learn about the API with direct calls, rather than fully relying on documentation.

### Access Tokens

To access the API, one must use an Authorization Bearer token.

### Create a Ledger

Events are stored and retrieved from a ledger. To create a new ledger, use an access token that holds the 'admin' role. In development mode, the simplest form of token looks like this:

`Authorization: Bearer eyJyb2xlcyI6WyJhZG1pbiJdfQ`

This token will grant access to the ledgers API:

```bash
curl http://127.0.0.1:4802/ledgers -H "Authorization: Bearer eyJyb2xlcyI6WyJhZG1pbiJdfQ"
```

This URL provides a list of existing ledgers, if any, as well as a link to the `create-ledger` form. Submit a POST request to this endpoint to generate a new ledger.

```bash
> curl http://localhost:4802/ledgers/create-ledger \
    -H "Authorization: Bearer eyJyb2xlcyI6WyJhZG1pbiJdfQ" \
    -H "Content-Type: application/json" \
    -d '{
          "name": "First ledger",
          "description": "My first Evently Ledger."
        }'

created Ledger 'First ledger' with id '3b9c0ad0'
```

Note down this ledger ID, as it will be used to generate client access tokens. If you lose the ID, simply fetch the `/ledgers` endpoint to find your ledger again.

### Client Access Tokens

Clients access ledgers with an client access token. This token contains the ledger ID and allowed roles for the client. To generate a token, identify the ledger ID to be accessed and convert the JSON object to base 64:

```bash
echo -n '{"roles":["registrar","admin","client"],"ledger":"YOUR_LEDGER_ID"}' | base64
```

The output from this command is the access token. Use it in HTTP requests to the API. Here is an example:

```bash
curl "http://127.0.0.1:4802" -H "Authorization: Bearer YOUR_ACCESS_TOKEN"
```
