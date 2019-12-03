# fxa-amplitude-send

This library contains code that processes some of the Firefox Accounts metrics.

## Build and deploy

The pubsub code is dockerized. Docker builds must currently be created on local machines before deploying to production ([#111](https://github.com/mozilla/fxa-amplitude-send/issues/111) tracks building on Circle-CI).

Note: `npm install` fails on Mac OS Sierra and later (10.12+), because the `node-parquet` dependency fails to build (see [#112](https://github.com/mozilla/fxa-amplitude-send/issues/112)).

## Lint and test

* `npm run lint` runs eslint on some of the JS code.
* no automated unit tests
* there are some test scripts (docs TBD, see 'other code' section below)

## Code docs

### Amplitude event processing from GCP Pub/Sub queue

The `pubsub/` directory contains two different queue worker implementations that process metrics events off a Pub/Sub queue and send to Amplitude.

#### Data flow / system overview

- On the Firefox Accounts web servers, Amplitude metrics are logged to stdout locally by web server code in the `fxa-auth-server`, `fxa-content-server`, and `fxa-payments-server` packages in the [FxA monorepo](https://github.com/mozilla/fxa).
  - The web server code uses the `mozlog` library to log to stdout.
  - For more details, including an explanation of what kinds of user events are sent to Amplitude, see the [ecosystem metrics docs](https://mozilla.github.io/ecosystem-platform/docs/fxa-engineering/fxa-metrics).
- On the web servers, Docker routes stdout to a number of consumers, including a local Stackdriver agent.
- The Stackdriver agent forwards logs to the GCP Stackdriver service.
- Stackdriver fans out logs to a Google Cloud Pub/Sub queue, as well as BigQuery and some kind of durable S3-like backup service.
- The fxa-amplitude-send queue workers process events off the queue, and send to Ampltiude.

#### Code details

##### API Usage

The `/pubsub` dir contains two queue worker implementations:

- `pubsub/synchronous-pull.js` - queue worker in current production use.
  - Amplitude API usage: uses Amplitude's [Batch API].
  - Pub/Sub API usage: uses synchronous pull to connect to the Pub/Sub queue, rather than the streaming pull used by `pubsub/index.js`, to work around connection errors ([#117](https://github.com/mozilla/fxa-amplitude-send/issues/117)). See [PubSub docs] for more details.
  - Pub/Sub connection: this worker uses the C++ `grpc` npm library, which successfully works around the memory leak ([#116](https://github.com/mozilla/fxa-amplitude-send/issues/116)).

- `pubsub/index.js` - older worker, not currently in use
  - Amplitude API usage: uses Amplitude's [Batch API]. Also contains legacy references to Amplitude's [Identify API] and the deprecated [HTTP API].
  - Pub/Sub API usage: this file uses streaming pull to connect to the Pub/Sub queue (see [PubSub docs]). It loses its connection to the Pub/Sub queue over time ([#117](https://github.com/mozilla/fxa-amplitude-send/issues/117)). Updating versions might fix this ([#118](https://github.com/mozilla/fxa-amplitude-send/issues/118)).
  - Pub/Sub connection: uses the grpc-js library provided by the GCP pubsub library, which seems to leak memory ([#116](https://github.com/mozilla/fxa-amplitude-send/issues/116)). Updating versions might fix this, also ([#118](https://github.com/mozilla/fxa-amplitude-send/issues/118)).

[Batch API]: https://developers.amplitude.com/#Batch-Event-Upload
[HTTP API]: https://help.amplitude.com/hc/en-us/articles/204771828-HTTP-API
[Identify API]: https://help.amplitude.com/hc/en-us/articles/205406617
[PubSub docs]: https://cloud.google.com/pubsub/docs/pull

##### Input validation

The pubsub code currently validates events using a minimal syntactic input schema, expressed in JS: see the [`isEventOk`](https://github.com/mozilla/fxa-amplitude-send/blob/master/pubsub/utils.js#L24) function in the synchronous-pull worker (note the [duplicate](https://github.com/mozilla/fxa-amplitude-send/blob/master/pubsub/index.js#L282-L289) in the index.js streaming-pull worker).

### Other code, docs TBD:

#### Marketing code

* /bin/marketing-localfs.js
* /bin/marketing-sqs.js
* /marketing.js

#### Sync code

* /bin/sync.js
* /sync-common.js
* /sync-cron.sh
* /sync-events.js
* /sync-summary.js

#### Test code

* test.sh
* test.stage.sh
* fixtures.txt

#### Unknown

* /check-events.js (doesn't seem to be used?)
* Makefile vs npm run scripts

