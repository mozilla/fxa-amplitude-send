'use strict'

const JSONStream = require('JSONStream')
const { Storage } = require('@google-cloud/storage')
const transform = require('stream-transform')

//const projectId = 'moz-fx-fxa-nonprod-375e'
const projectId = 'moz-fx-fxa-prod-0712'
const bucketName = 'fxa-amplitude-event-export'

const storage = new Storage({
  projectId
})
const bucket = storage.bucket(bucketName)

async function main () {
  const files = (await bucket.getFiles())[0]
  const file = files.pop()

  const readStream = file.createReadStream()
}

main()
