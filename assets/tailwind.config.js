const plugin = require("tailwindcss/plugin")
const fs = require("fs")
const path = require("path")

module.exports = {
  content: [
    "./js/**/*.js",
    "../lib/ex_storage_service_web.ex",
    "../lib/ex_storage_service_web/**/*.*ex"
  ],
  theme: {
    extend: {},
  },
  plugins: []
}
