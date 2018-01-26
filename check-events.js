#!/usr/bin/env node

'use strict'

const fs = require('fs')
const path = require('path')

const TIME_FORMAT = /(201[78])-([0-9]{2})-([0-9]{2})-([0-9]{2})-([0-9]{2})/
const CATEGORY_FORMAT = /^logging\.s3\.fxa\.([a-z]+)_server/
const VERBOSE = false

const args = process.argv

if (args.length !== 4) {
  usage()
}

let from = TIME_FORMAT.exec(args[2])
let until = TIME_FORMAT.exec(args[3])

if (! from || ! until) {
  usage()
}

from = from.slice(1)
until = until.slice(1)

const cwd = process.cwd()
const fileNames = fs.readdirSync(cwd)

const missingDeviceIds = createStat()
const missingSessionIds = createStat()
const missingDeviceAndSessionIds = createStat()
const futureSessionIds = createStat()
const futureTimes = createStat()

const users = new Map()

const events = fileNames.reduce((previousEvents, fileName) => {
  let time = TIME_FORMAT.exec(fileName)
  if (! time) {
    return
  }

  time = time.slice(1)
  if (! isInRange(time)) {
    return
  }

  let category = CATEGORY_FORMAT.exec(fileName)
  if (! category) {
    return
  }

  category = category[1]
  if (! previousEvents[category]) {
    return
  }

  const target = timestamp(time)
  const isContentServerEvent = category === 'content'

  const text = fs.readFileSync(path.join(cwd, fileName), { encoding: 'utf8' })
  const lines = text.split('\n')
  const data = lines
    .filter(line => !! line.trim() && line.indexOf("amplitudeEvent") !== -1)
    .map((line, index) => {
      let event
      try {
        event = JSON.parse(line)
        if (event.Fields) {
          event = event.Fields
        }
      } catch (_) {
        event = {}
      }

      const datum = {
        file: fileName,
        line: index + 1,
        event
      }

      const deviceId = event.device_id
      const sessionId = event.session_id

      if (! deviceId) {
        if (! sessionId) {
          missingDeviceAndSessionIds[category].push(datum)
        } else {
          missingDeviceIds[category].push(datum)
        }
      } else if (! sessionId) {
        missingSessionIds[category].push(datum)
      }

      if (sessionId > target) {
        futureSessionIds[category].push(datum)
      }

      if (event.time > target) {
        futureTimes[category].push(datum)
      }

      const uid = event.user_id
      if (isContentServerEvent) {
        const user = users.get(uid) || {
          deviceSessions: new Map(),
          sessionDevices: new Map()
        }

        const deviceSessions = user.deviceSessions.get(deviceId) || new Set()
        deviceSessions.add(sessionId)
        user.deviceSessions.set(deviceId, deviceSessions)

        const sessionDevices = user.sessionDevices.get(sessionId) || new Set()
        sessionDevices.add(deviceId)
        user.sessionDevices.set(sessionId, sessionDevices)

        users.set(uid, user)
      }

      return datum
    })

  previousEvents[category] = previousEvents[category].concat(data)
  return previousEvents
}, createStat())

displayStat(events, 'EVENTS')
displayStatVerbose(missingDeviceIds, 'MISSING device_id')
displayStatVerbose(missingSessionIds, 'MISSING session_id')
displayStatVerbose(missingDeviceAndSessionIds, 'MISSING device_id AND session_id')
displayStatVerbose(futureSessionIds, 'FUTURE session_id')
displayStatVerbose(futureTimes, 'FUTURE time')

const conflictingDeviceIds = []
const conflictingSessionIds = []

events.auth.forEach(datum => {
  const event = datum.event
  const user = users.get(event.user_id) || {
    deviceSessions: new Map(),
    sessionDevices: new Map()
  }

  const deviceId = event.device_id
  const sessionId = event.session_id

  const deviceSessions = user.deviceSessions.get(deviceId)
  if (deviceSessions && ! deviceSessions.has(sessionId)) {
    conflictingSessionIds.push(datum)
  }

  const sessionDevices = user.sessionDevices.get(sessionId)
  if (sessionDevices && ! sessionDevices.has(deviceId)) {
    conflictingDeviceIds.push(datum)
  }
})

console.log('CONFLICTING device_id:', conflictingDeviceIds.length)
if (VERBOSE) {
  conflictingDeviceIds.forEach(datum => console.log(datum))
}

console.log('CONFLICTING session_id:', conflictingSessionIds.length)
if (VERBOSE) {
  conflictingSessionIds.forEach(datum => console.log(datum))
}

function usage () {
  console.error(`Usage: node ${args[1]} FROM UNTIL`)
  console.error('FROM and UNTIL are both YYYY-MM-DD-hh-mm')
  process.exit(1)
}

function isInRange (time) {
  return satisfiesOrEquals(time, from, (lhs, rhs) => lhs - rhs) &&
    satisfiesOrEquals(time, until, (lhs, rhs) => rhs - lhs)
}

function satisfiesOrEquals (subject, object, diff) {
  let result = true

  object.some((item, index) => {
    const d = diff(+subject[index], +item)
    if (d > 0) {
      return true
    }

    if (d < 0) {
      result = false
      return true
    }

    return false
  })

  return result
}

function createStat () {
   return {
    content: [],
    auth: []
  }
}

function timestamp (time) {
  return Date.parse(`${time[0]}-${time[1]}-${time[2]}T${time[3]}:${time[4]}:59.999`)
}

function displayStat (stat, description) {
  const categories = Object.keys(stat).map(key => ({ category: key, count: stat[key].length }))
  const count = categories.reduce((sum, item) => sum + item.count, 0)
  const categoryCounts = categories.map(item => `${item.category}: ${item.count}`).join(', ')

  console.log(`${description}: ${count} (${categoryCounts})`)
}

function displayStatVerbose (stat, description) {
  displayStat(stat, description)

  if (VERBOSE) {
    Object.keys(stat).forEach(key => stat[key].forEach(datum => console.log(datum)))
  }
}

