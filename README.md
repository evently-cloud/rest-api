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
