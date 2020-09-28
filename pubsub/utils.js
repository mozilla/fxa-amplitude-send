// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, you can obtain one at https://mozilla.org/MPL/2.0/.

'use strict'

const crypto = require('crypto')
const is = require('check-types')
const logger = require('pino')({
  // https://cloud.google.com/logging/docs/agent/configuration#timestamp-processing
  timestamp: () => `,"time":"${(Date.now() / 1000.0).toFixed(3)}000000"`
})
const { lookup } = require('lookup-dns-cache')
const request = require('request-promise')

function hash (HMAC_KEY, ...properties) {
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

function parseMessage (event, HMAC_KEY) {
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
    return {}
  }

  // Work around some invalid session_ids
  if (is.string(event.session_id)) {
    // Try converting it to a number first
    let new_session_id = parseInt(event.session_id, 10)
    if (isNaN(new_session_id)) {
      // It's a string, set session_id to -1
      new_session_id = -1
    }
    logger.error({ type: 'amplitude.validation.error', new_session_id, old_session_id: event.session_id.toString() }, 'Invalid session_id')
    event.session_id = new_session_id
  }

  if (event.user_id) {
    event.user_id = hash(HMAC_KEY, event.user_id)
  }

  event.insert_id = hash(HMAC_KEY, event.user_id, event.device_id, event.session_id, event.event_type, event.time)

  let identify
  if (IDENTIFY_VERBS.some(verb => is.assigned(event.user_properties[verb]))) {
    identify = {
      device_id: event.device_id,
      event_type: '$identify',
      user_id: event.user_id,
      user_properties: splitIdentifyPayload(event.user_properties),
    }
  }

  return {
    httpapi: event,
    identify,
  }
}

function sendMessages (events, AMPLITUDE_API_KEY) {
  return request('https://api.amplitude.com/batch', {
    method: 'POST',
    lookup,
    json: true,
    body: {
      api_key: AMPLITUDE_API_KEY,
      events: events
    },
    timeout: 5 * 1000
  })
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

module.exports = {
  hash,
  isEventOk,
  parseMessage,
  sendMessages,
  splitIdentifyPayload,
}
