const { handler } = require("./export_parquet")   // your lambda file name

async function runTest() {

  const event = {}
  const context = {}

  const result = await handler(event, context)

  console.log("Lambda Result:", result)
}

runTest()