/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

'use strict'

const csv = require('csv-parser')
const fs = require('fs')
const s3 = require('s3')

const DATE_FORMAT = /^20[1-9][0-9]-[01][0-9]-[0-3][1-9]$/
const AWS_ACCESS_KEY = process.env.FXA_AWS_ACCESS_KEY
const AWS_SECRET_KEY = process.env.FXA_AWS_SECRET_KEY
const AWS_S3_BUCKET = 'net-mozaws-prod-us-west-2-pipeline-analysis'
const AWS_S3_PREFIX = 'fxa-flow/data/flow-'
const AWS_S3_SUFFIX = '.csv'

const SERVICES = {
  // TODO: populate this if we want the service property to be correct
}

const GROUPS = {
  connectDevice: 'fxa_connect_device',
  email: 'fxa_email',
  emailFirst: 'fxa_email_first',
  login: 'fxa_login',
  registration: 'fxa_reg',
  settings: 'fxa_pref',
  sms: 'fxa_sms'
}

const ENGAGE_SUBMIT_EVENT_GROUPS = {
  'connect-another-device': GROUPS.connectDevice,
  'enter-email': GROUPS.emailFirst,
  'force-auth': GROUPS.login,
  install_from: GROUPS.connectDevice,
  signin_from: GROUPS.connectDevice,
  signin: GROUPS.login,
  signup: GROUPS.registration,
  sms: GROUPS.connectDevice
}

const VIEW_EVENT_GROUPS = {
  'connect-another-device': GROUPS.connectDevice,
  'enter-email': GROUPS.emailFirst,
  'force-auth': GROUPS.login,
  signin: GROUPS.login,
  signup: GROUPS.registration,
  sms: GROUPS.connectDevice
}

const CONNECT_DEVICE_FLOWS = {
  'connect-another-device': 'cad',
  install_from: 'store_buttons',
  signin_from: 'signin',
  sms: 'sms'
}

const EVENTS = {
  'flow.reset-password.submit': {
    group: GROUPS.login,
    event: 'forgot_submit'
  }
}

const FUZZY_EVENTS = new Map([
  [ /^flow\.([\w-]+)\.engage$/, {
    isDynamicGroup: true,
    group: eventCategory => ENGAGE_SUBMIT_EVENT_GROUPS[eventCategory],
    event: 'engage'
  } ],
  [ /^flow\.[\w-]+\.forgot-password$/, {
    group: GROUPS.login,
    event: 'forgot_pwd'
  } ],
  [ /^flow\.[\w-]+\.have-account$/, {
    group: GROUPS.registration,
    event: 'have_account'
  } ],
  [ /^flow\.((?:install|signin)_from)\.\w+$/, {
    group: GROUPS.connectDevice,
    event: 'engage'
  } ],
  [ /^flow\.([\w-]+)\.submit$/, {
    isDynamicGroup: true,
    group: eventCategory => ENGAGE_SUBMIT_EVENT_GROUPS[eventCategory],
    event: 'submit'
  } ],
  [ /^flow\.([\w-]+)\.view$/, {
    isDynamicGroup: true,
    group: eventCategory => VIEW_EVENT_GROUPS[eventCategory],
    event: 'view'
  } ]
])

const EVENT_PROPERTIES = {
  [GROUPS.connectDevice]: mapConnectDeviceFlow,
  [GROUPS.emailFirst]: mapService,
  [GROUPS.login]: mapService,
  [GROUPS.registration]: mapService
}

const USER_PROPERTIES = {
  [GROUPS.connectDevice]: mapFlowId,
  [GROUPS.emailFirst]: mixProperties(mapFlowId, mapUtmProperties),
  [GROUPS.login]: mixProperties(mapFlowId, mapUtmProperties),
  [GROUPS.registration]: mixProperties(mapFlowId, mapUtmProperties)
}

if (process.argv.length !== 3) {
  console.error(`Usage: ${process.argv[1]} {YYYY-MM-DD | LOCAL PATH}`)
  process.exit(1)
}

Promise.resolve()
  .then(processData)
  .then(() => process.exit(0))
  .catch(error => {
    console.error(error.stack)
    process.exit(1)
  })

function processData () {
  return new Promise(resolve => {
    createStream()
      .pipe(csv({
        headers: [
          'timestamp', 'type', 'flow_id', 'flow_time', 'ua_browser',
          'ua_version', 'ua_os', 'context', 'entrypoint', 'migration',
          'service', 'utm_campaign', 'utm_content', 'utm_medium',
          'utm_source', 'utm_term', 'locale', 'uid'
        ]
      }))
      .on('data', row => {
        const event = transformEvent(row)
        if (event) {
          console.log(JSON.stringify(event))
        }
      })
      .on('end', resolve)
  })
}

function createStream () {
  const arg = process.argv[2]

  if (DATE_FORMAT.test(arg)) {
    if (! AWS_ACCESS_KEY || ! AWS_SECRET_KEY) {
      throw new Error('You must set AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables')
    }

    return s3
      .createClient({
        s3Options: {
          accessKeyId: AWS_ACCESS_KEY,
          secretKey: AWS_SECRET_KEY
        }
      })
      .downloadStream({
        Bucket: AWS_S3_BUCKET,
        Key: `${AWS_S3_PREFIX}${arg}${AWS_S3_SUFFIX}`
      })
  }

  return fs.createReadStream(arg)
}

function transformEvent (event) {
  if (! event) {
    return
  }

  const eventType = event.type
  let mapping = EVENTS[eventType]
  let eventCategory

  if (! mapping) {
    for (const [ key, value ] of FUZZY_EVENTS.entries()) {
      const match = key.exec(eventType)
      if (match) {
        mapping = value
        if (match.length === 2) {
          eventCategory = match[1]
        }
        break
      }
    }
  }

  if (mapping) {
    let group = mapping.group
    if (mapping.isDynamicGroup) {
      group = group(eventCategory)
      if (! group) {
        return
      }
    }

    return Object.assign({
      op: 'amplitudeEvent',
      time: event.time,
      user_id: event.uid,
      event_type: `${group} - ${mapping.event}`,
      session_id: event.timestamp - event.flow_time,
      event_properties: mapEventProperties(group, mapping.event, eventCategory, event),
      user_properties: mapUserProperties(group, eventCategory, event),
      language: event.locale
    }, mapOs(event))
  }
}

function mixProperties (...mappers) {
  return (event, eventCategory, data) =>
    Object.assign({}, ...mappers.map(m => m(event, eventCategory, data)))
}

function mapEventProperties (group, event, eventCategory, data) {
  return EVENT_PROPERTIES[group](event, eventCategory, data)
}

function mapUserProperties (group, eventCategory, data) {
  return Object.assign(
    {},
    mapBrowser(data),
    mapEntrypoint(data),
    USER_PROPERTIES[group](eventCategory, data)
  )
}

function mapOs (data) {
  const { ua_os } = data
  if (ua_os) {
    return { os_name: ua_os }
  }
}

function mapBrowser (data) {
  const { ua_browser, ua_version } = data
  if (ua_browser) {
    return { ua_browser, ua_version }
  }
}

function mapEntrypoint (data) {
  const { entrypoint } = data
  if (entrypoint) {
    return { entrypoint }
  }
}

function mapConnectDeviceFlow (event, eventCategory) {
  const connect_device_flow = CONNECT_DEVICE_FLOWS[eventCategory]
  if (connect_device_flow) {
    return { connect_device_flow }
  }
}

function mapService (event, eventCategory, data) {
  const { service } = data.service
  if (service) {
    let serviceName, clientId

    if (service === 'sync') {
      serviceName = service
    } else {
      serviceName = SERVICES[service] || 'undefined_oauth'
      clientId = service
    }

    return { service: serviceName, oauth_client_id: clientId }
  }
}

function mapFlowId (eventCategory, data) {
  const { flow_id } = data
  if (flow_id) {
    return { flow_id }
  }
}

function mapUtmProperties (eventCategory, data) {
  return {
    utm_campaign: data.utm_campaign,
    utm_content: data.utm_content,
    utm_medium: data.utm_medium,
    utm_source: data.utm_source,
    utm_term: data.utm_term
  }
}

