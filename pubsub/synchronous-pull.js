// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, you can obtain one at https://mozilla.org/MPL/2.0/.

'use strict'

const logger = require('pino')({
  // https://cloud.google.com/logging/docs/agent/configuration#timestamp-processing
  timestamp: () => `,"time":"${(Date.now() / 1000.0).toFixed(3)}000000"`
})
const PubSub = require('@google-cloud/pubsub')
const retry = require('async-retry')
const utils = require('./utils')

const { AMPLITUDE_API_KEY, HMAC_KEY, MAX_EVENTS_PER_BATCH, PUBSUB_PROJECT, PUBSUB_SUBSCRIPTION } = process.env
const MAX_RETRIES = process.env.MAX_RETRIES ? parseInt(process.env.MAX_RETRIES, 10) : 3

if (! AMPLITUDE_API_KEY || ! HMAC_KEY || ! MAX_EVENTS_PER_BATCH || ! PUBSUB_PROJECT || ! PUBSUB_SUBSCRIPTION) {
  logger.fatal({type: 'startup.error'}, 'Error: You must set AMPLITUDE_API_KEY, HMAC_KEY, MAX_EVENTS_PER_BATCH, PUBSUB_PROJECT, and PUBSUB_SUBSCRIPTION environment variables')
  process.exit(1)
}

async function main () {
  const client = new PubSub.v1.SubscriberClient({
    projectId: PUBSUB_PROJECT,
  })
  const formattedSubscription = client.subscriptionPath(
    PUBSUB_PROJECT,
    PUBSUB_SUBSCRIPTION,
  )
  const pubsubPullRequest = {
    subscription: formattedSubscription,
    maxMessages: MAX_EVENTS_PER_BATCH,
  }

  let isProcessing = true
  process.on('SIGINT', () => {
    isProcessing = false
  })
  process.on('SIGTERM', () => {
    isProcessing = false
  })

  while (isProcessing) {
    let pubsubPullResponse = await client.pull(pubsubPullRequest).catch(error => {
      logger.error({ type: 'pubsub.pull.error', error }, 'pubsub.pull() error: %s', error)
    })

    if (!pubsubPullResponse) {
      continue
    }
    pubsubPullResponse = pubsubPullResponse[0].receivedMessages

    let minPublishedTime = '2100', maxPublishedTime = '2000'

    let messages = pubsubPullResponse.map((m) => {
      let parsed = utils.parseMessage(JSON.parse(Buffer.from(m.message.data, 'base64').toString()).jsonPayload, HMAC_KEY)

      if (m.message.attributes['logging.googleapis.com/timestamp'] < minPublishedTime) {
        minPublishedTime = m.message.attributes['logging.googleapis.com/timestamp']
      } else if (m.message.attributes['logging.googleapis.com/timestamp'] > maxPublishedTime) {
        maxPublishedTime = m.message.attributes['logging.googleapis.com/timestamp']
      }

      if (parsed.identify) {
        return [parsed.identify, parsed.httpapi]
      } else if (parsed.httpapi) {
        return [parsed.httpapi]
      } else {
        return []
      }
    }).flat()

    const amplitudeResponse = await retry(async bail => {
      return await utils.sendMessages(messages, AMPLITUDE_API_KEY)
    }, {
      onRetry: error => {
        logger.error({type: 'amplitude.batch.error', amplitudeError: error.toString(), messages, response: error })
      },
      retries: MAX_RETRIES,
    })

    const pubsubAckRequest = {
      subscription: formattedSubscription,
      ackIds: pubsubPullResponse.map(m => m.ackId)
    }
    const pubsubAckResponse = await client.acknowledge(pubsubAckRequest)

    logger.info({
      type: 'events.processed',
      minPublishedTime,
      maxPublishedTime,
      inputCount: pubsubPullResponse.length,
      outputCount: messages.length,
      amplitudeResponse
    })
  }
}

main()

process.on('SIGINT', function() {
  process.exit()
})
