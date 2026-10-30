# Potamic

Potamic is a stream-based message queue implementation built over Redis and Kvrocks. It simplifies reading and writing as multiple consumers, automatically tracking pending entries and processed messages using native stream and consumer group structures.

### Index

1. [Get Potamic](#get-potamic)
2. [Connection URIs & Modes](#connection-uris-and-modes)
3. [Usage Examples](#usage-examples)
4. [Docs](#docs)
5. [Running Locally](#running-locally)
6. [License](#license)

---

## Get Potamic

Add the following to your `deps.edn`:

```clojure
io.github.yeajustmars/potamic #:git{:tag "VERSION" :sha "HASH"}

```

---

## Connection URIs & Modes

Potamic supports both standard Redis and Kvrocks backends.

* **Redis (`redis://`)**: Standard Redis connection.
* **Kvrocks (`kvrocks://`)**: Automatically applies the `:kvrocks` backend, triggering legacy namespace creation behavior.
* **Kvrocks in Redis Mode (`?mode=redis`)**: By appending `?mode=redis` to a Kvrocks URI, Potamic will bypass namespace creation and treat the connection as standard Redis, which is required for Kvrocks v2.15.0+ utilizing standard database indexing (`SELECT 0-N`).

**URI Examples:**

```clojure
;; Standard Redis
"redis://localhost:6379/0"

;; Kvrocks (Legacy Namespace Mode)
"kvrocks://:secret@127.0.0.1:6666/0"

;; Kvrocks (Redis-Compatible DB Mode)
"kvrocks://127.0.0.1:6666/3?mode=redis"

```

---

## Usage Examples

Potamic is broken down into three primary namespaces: `db` (connections), `queue` (streams and groups), and `sentinel` (automated watchers).

### 1. Connecting & Basic Setup

```clojure
(require '[potamic.db :as db]
         '[potamic.queue :as q])

;; Connect to your datastore
(def conn (db/make-conn :uri "redis://localhost:6379/0"))

;; Create a queue and a default consumer group
(q/create-queue! :my/queue conn :group :my/workers)

```

### 2. Publishing Messages

Because `put` can add more than one message, on success it returns a vector of ID strings.

```clojure
;; Put one or more messages onto the queue
(q/put :my/queue {:job "a" :payload 1} {:job "b" :payload 2})
;;=> [["1683660166747-0" "1683660166747-1"] nil]

```

### 3. Basic Reading (No Consumer Groups)

Wraps Redis' `XREAD` and `XRANGE`.

```clojure
;; Read all messages
(q/read :my/queue)

;; Read a range of messages
(q/read-range :my/queue :start '- :end '+ :count 10)

```

### 4. Consumer Groups (The Worker Lifecycle)

When using consumer groups, workers read messages, process them, and then explicitly acknowledge them to remove them from the Pending Entries List (PEL).

```clojure
;; Read the next 2 messages as a specific consumer
(let [[msgs ?err] (q/read-next! 2 :from :my/queue :as :worker-a)]
  (when msgs
    (doseq [m msgs]
      (println "Processing ID:" (:id m) "Msg:" (:msg m)))

    ;; Acknowledge messages to clear them from the PEL
    (let [ids (map :id msgs)]
      (apply q/set-processed! :my/queue ids))))

```

### 5. Managing Pending Messages

```clojure
;; Get a summary of all pending messages across all consumers
(q/read-pending-summary :my/queue)
;;=> [{:total 2 :start "1683745855445-0" :end "1683745855445-1" :consumers #:worker{:a 2}} nil]

;; Get deep details for a specific consumer's pending messages
(q/read-pending 10 :from :my/queue :for :worker-a)

```

### 6. Using Sentinels

Sentinels provide time-oriented watchers that run on a background `core.async` loop, executing a handler function at a specific frequency.

```clojure
(require '[potamic.sentinel :as s])

;; Create a Sentinel watcher
(def watcher
  (s/create-sentinel
    {:queue-uri "kvrocks://127.0.0.1:6666/0?mode=redis"
     :queue-name :jobs/queue
     :queue-group :jobs/workers
     :frequency 2000 ;; Runs every 2 seconds
     :handler (fn [this]
                (let [[msgs _] (q/read-next! 5 :from :jobs/queue :as :sentinel-worker)]
                  (when (seq msgs)
                    (println "Sentinel processed" (count msgs) "messages!")
                    (apply q/set-processed! :jobs/queue (map :id msgs)))))}))

;; Start the watcher (runs indefinitely in the background)
(s/start-sentinel! watcher)

;; Stop the watcher
(s/stop-sentinel! watcher)

```

---

## Docs

### Online

[https://yeajustmars.github.io/potamic](https://www.google.com/search?q=https://yeajustmars.github.io/potamic)

### Local

```shell
git clone https://github.com/yeajustmars/potamic.git

cd potamic

open docs/index.html

```



---

## Running Locally

### Testing

You'll need 2 terminals open; 1 for Redis and 1 for the test runner.

**Redis:**

```shell
cd /path/to/potamic

bin/db

```



**Test Runner:**

```shell
cd /path/to/potamic

bin/test
# or
bin/test --watch

```



### REPL

```shell
cd /path/to/potamic

bin/repl

```



### Generating Docs

```shell
cd /path/to/potamic

bin/doc

```



---

## License

Distributed under the [EPL v1.0](https://www.google.com/search?q=https://raw.githubusercontent.com/yeajustmars/potamic/main/LICENSE) (same as Clojure).

Copyright © 2023 @yeajustmars
