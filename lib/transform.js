/*eslint func-style: ["error", "declaration", { "allowArrowFunctions": true }]*/

'use strict'

const crypto = require('crypto')
const { many } = require('stream-chain')

const IDENTIFY_VERBS = [ '$set', '$setOnce', '$add', '$append', '$unset' ]

const contains_identify_verbs = (event) => {
  return Object.keys(event.user_properties).some(property => IDENTIFY_VERBS.includes(property))
}

const generate_identify_events = (event) => {
  return IDENTIFY_VERBS.filter(verb => event.user_properties[verb]).map(verb => {
    return {
      user_properties: {
        [verb]: event.user_properties[verb]
      },
      device_id: event.device_id,
      user_id: event.user_id
    }
  })
}

const delete_identify_properties = (event) => {
  IDENTIFY_VERBS.filter(verb => event.user_properties[verb]).forEach(verb => {
    delete event.user_properties[verb]
  })
}

const valid = (event) => {
  return ( event.device_id || event.user_id ) && event.event_type && event.time
}

const transform = (options) => {
  if (! options.hmacKey) {
    throw new Error('hmacKey must be set')
  }

  const hmacKey = options.hmacKey

  return (data) => {
    // { key: 1, value: { ... } }
    let event = data.value

    // Mozlog 2.0 puts optional properties in `Fields` object, and stringifies objects
    if (event.Fields && typeof event.Fields === 'object') {
      event = event.Fields

      if (event.event_properties && typeof event.event_properties === 'string') {
        event.event_properties = JSON.parse(event.event_properties)
      }
      if (event.user_properties && typeof event.user_properties === 'string') {
        event.user_properties = JSON.parse(event.user_properties)
      }
    }

    if (! valid(event)) {
      console.log({ error: 'Invalid event', event })
      return
    }

    const insert_id_hmac = crypto.createHmac('sha256', hmacKey)

    if (event.user_id) {
      event.user_id = crypto.createHmac('sha256', hmacKey)
        .update(event.user_id)
        .digest('hex')
      insert_id_hmac.update(event.user_id)
    }

    if (event.device_id) {
      insert_id_hmac.update(event.device_id)
    }

    if (event.session_id) {
      insert_id_hmac.update(event.session_id)
    }

    insert_id_hmac.update(event.event_type)
    insert_id_hmac.update(event.time.toString())
    event.insert_id = insert_id_hmac.digest('hex')

    // Generate identify API events
    if (contains_identify_verbs(event)) {
      const new_events = generate_identify_events(event)
      delete_identify_properties(event)
      new_events.push(event)
      return many(new_events)
    }

    return event
  }
}

module.exports = transform
