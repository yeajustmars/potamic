(ns potamic.test-data)

(def valid-redis-uris
  ["redis://localhost:6379/0"
   "redis://USER:PASS@localhost:1234"
   "redis://USER:PASS@localhost:12345/1"
   "redis://:PASS@localhost:6379"
   "redis://:PASS@localhost:6379/0"
   "redis://111.222.233.244:6379/0"
   "redis://USER:PASS@111.222.233.244:1234"
   "redis://USER:PASS@111.222.233.244:12345"
   "redis://:PASS@111.222.233.244:6379"
   "redis://:PASS@111.222.233.244:6379/0"])

(def valid-uri-parse-mappings
  "Map of the form: `{REDIS-URI CLJ-HASH}`."
  {"redis://localhost:6379/0"
   {:scheme "redis" :host "localhost" :user nil :password nil :port 6379 :db 0}
  "redis://USER:PASS@localhost:1234"
  {:scheme "redis" :host "localhost" :user "USER" :password "PASS" :port 1234 :db nil}
  "redis://scooby:doo@123.124.125.126:6666/1"
  {:scheme "redis" :host "123.124.125.126" :user "scooby" :password "doo" :port 6666 :db 1}})

