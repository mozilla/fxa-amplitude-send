// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, you can obtain one at https://mozilla.org/MPL/2.0/.

'use strict'

const async = require('async')
const crypto = require('crypto')
const equal = require('fast-deep-equal');
const is = require('check-types')
const logger = require('pino')({
  // https://cloud.google.com/logging/docs/agent/configuration#timestamp-processing
  timestamp: () => `,"time":"${(Date.now() / 1000.0).toFixed(3)}000000"`
})
const { lookup } = require('lookup-dns-cache')
const { PubSub } = require('@google-cloud/pubsub')
const request = require('request-promise')
const retry = require('async-retry')

const { AMPLITUDE_API_KEY, HMAC_KEY, MAX_EVENTS_PER_BATCH, MAX_RUN_TIME, PUBSUB_PROJECT, PUBSUB_TOPIC, PUBSUB_SUBSCRIPTION } = process.env

if (! AMPLITUDE_API_KEY || ! HMAC_KEY || ! MAX_EVENTS_PER_BATCH || ! MAX_RUN_TIME || ! PUBSUB_PROJECT || ! PUBSUB_SUBSCRIPTION || ! PUBSUB_TOPIC) {
  logger.fatal({type: 'startup.error'}, 'Error: You must set AMPLITUDE_API_KEY, HMAC_KEY, MAX_EVENTS_PER_BATCH, MAX_RUN_TIME, PUBSUB_PROJECT, PUBSUB_TOPIC and PUBSUB_SUBSCRIPTION environment variables')
  process.exit(1)
}

async function main () {
  const pubsub = new PubSub({
    projectId: PUBSUB_PROJECT
  })
  const topic = pubsub.topic(PUBSUB_TOPIC)
  const subscription = topic.subscription(PUBSUB_SUBSCRIPTION, {
    ackDeadline: 300,
    flowControl: {
      maxExtension: 5 * 60
    }
  })

  subscription.on('message', message => {
    cargo.push(message)
  })

  const cargo = async.cargo(async (payload) => {
    let identify_events = []
    let http_events = payload.map(message => {
      let event = JSON.parse(Buffer.from(message.data, 'base64').toString()).jsonPayload

      if (event.Fields) {
        event = event.Fields
    
        if (event.op && event.data) {
          event = JSON.parse(event.data)
        } else {
          if (is.nonEmptyString(event.event_properties)) {
            event.event_properties = JSON.parse(event.event_properties)
          }
    
          if (is.nonEmptyString(event.user_properties)) {
            event.user_properties = JSON.parse(event.user_properties)
          }
        }
      }

      if (! isEventOk(event)) {
        logger.warn({ type: 'event.malformed', event }, 'Dropping malformed event')
        message.ack()
        return null
      }

      if (event.user_id) {
        event.user_id = hash(event.user_id)
      }
    
      event.insert_id = hash(event.user_id, event.device_id, event.session_id, event.event_type, event.time)
      
      if (IDENTIFY_VERBS.some(verb => is.assigned(event.user_properties[verb]))) {
        identify_events.push({
          device_id: event.device_id,
          user_id: event.user_id,
          user_properties: splitIdentifyPayload(event.user_properties),
        })
      }

      return event
    }).filter(message => !!message)

    // Filter out exact duplicate identify events to help prevent rate limiting
    identify_events = identify_events.filter((outer_event, index, self) => {
      return index === self.findIndex((inner_event) => {
        return equal(outer_event, inner_event)
      })
    })

    // Filter out duplicate user ids in identify events to help prevent rate limiting
    identify_events = identify_events.filter((outer_event, index, self) => {
      return index === self.findIndex((inner_event) => {
        return outer_event.user_id === inner_event.user_id
      })
    })

    if (identify_events.length > 0) {
      let identifying_response = await retry (async bail => {
        let identifying_response = await request('https://api.amplitude.com/identify', {
          method: 'POST',
          lookup,
          formData: {
            api_key: AMPLITUDE_API_KEY,
            identification: JSON.stringify(identify_events)
          },
          timeout: 5 * 1000
        })

        return identifying_response
      }, {
        minTimeout: 15000,
        maxTimeout: 60000,
        onRetry: error => {
          logger.error({type: 'identify.error', error: error})
        }
      })
      
      if (identifying_response == 'success') {
        logger.info({type: 'identify.success', count: identify_events.length}, 'Identify success')
      } else {
        throw new Error('Something goofed in the Identify API')
      }
    }

    let http_response = await retry (async bail => {
      let http_response = await request('https://api.amplitude.com/2/httpapi', {
        method: 'POST',
        lookup,
        json: true,
        body: {
          api_key: AMPLITUDE_API_KEY,
          events: http_events
        },
        timeout: 5 * 1000
      }, {
        onRetry: error => {
          logger.error({type: 'http.error', error: error})
        }
      })

      return http_response
    })

    if (http_response.code == 200) {
      logger.info({type: 'http.success', count: http_events.length}, 'HTTP success')
    } else {
      logger.info(http_response)
      throw new Error('Something goofed in the HTTP API')
    }

    payload.forEach(message => message.ack())
  }, MAX_EVENTS_PER_BATCH)

  setTimeout(() => {
    subscription.close();
  }, MAX_RUN_TIME);
}

main()

function hash (...properties) {
  const hmac = crypto.createHmac('sha256', HMAC_KEY)

  properties.forEach(property => {
    if (property) {
      hmac.update(`${property}`)
    }
  })

  return hmac.digest('hex')
}

function isEventOk (event) {
  return (
    is.nonEmptyString(event.device_id) ||
    is.nonEmptyString(event.user_id)
  ) &&
    is.nonEmptyString(event.event_type) &&
    is.positive(event.time)
}

const IDENTIFY_VERBS = [ '$set', '$setOnce', '$add', '$append', '$unset' ]
const IDENTIFY_VERBS_SET = new Set(IDENTIFY_VERBS)

function splitIdentifyPayload (properties) {
  return Object.entries(properties).reduce((payload, [ key, value ]) => {
    if (IDENTIFY_VERBS_SET.has(key)) {
      payload[key] = value
      properties[key] = undefined
    }
    return payload
  }, {})
}
