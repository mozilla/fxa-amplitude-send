/*eslint func-style: ["error", "declaration", { "allowArrowFunctions": true }]*/

'use strict'

const { chain } = require('stream-chain')
const StreamValues = require('stream-json/streamers/StreamValues')
const transform = require('../lib/transform')

const FXA_AMPLITUDE_API_KEY = process.env.FXA_AMPLITUDE_API_KEY
const FXA_AMPLITUDE_HMAC_KEY = process.env.FXA_AMPLITUDE_HMAC_KEY

const pipeline = chain([
  StreamValues.withParser(),
  transform({ hmacKey: FXA_AMPLITUDE_HMAC_KEY }),
  (data) => console.log(JSON.stringify(data))
])

pipeline.on('error', error => {
  throw error
})

process.stdin.pipe(pipeline)
