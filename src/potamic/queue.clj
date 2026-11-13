(ns potamic.queue
  "Implements a stream-based message queue over Redis or Kvrocks."
  {:added "0.1"
   :author "@yeajustmars"}
  (:refer-clojure :exclude [read])
  (:require [clojure.walk :as walk]
            [potamic.connection :refer [pcar]]
            [potamic.db :as db]
            [potamic.errors :as e]
            [potamic.queue.queues :as queues]
            [potamic.queue.validation :as qv]
            [potamic.util :as util]
            [potamic.validation :as v]
            [taoensso.carmine :as car]))

(defn get-queue
  "Returns queue spec for `queue-name`."
  [queue-name]
  (get @queues/queues_ queue-name))

(defn get-queues
  "Get all, or a subset, of queues created via `potamic.queue/create-queue!`.
  Return value varies depending on `x` input type."
  ([] @queues/queues_)
  ([x]
   (cond
     (instance? java.util.regex.Pattern x)
     (into {} (filter (fn [[k v]]
                        (or (re-find x (name k))
                            (re-find x (str v))))
                      @queues/queues_))

     :else
     (get @queues/queues_ x))))

(defn- -unknown-queue-err
  [fn-name queue-name]
  (e/error {:potamic/err-type :potamic/args-err
            :potamic/err-fatal? false
            :potamic/err-msg (str "Unknown queue provided to " fn-name)
            :potamic/err-data {:queue-name queue-name}}))

(defn- -unwrap-pcar
  "Throws if Carmine returned an error-as-value Throwable."
  [res]
  (cond
    (instance? Throwable res)
    (throw res)

    (sequential? res)
    (if-let [err (first (filter #(instance? Throwable %) res))]
      (throw err)
      res)

    :else res))

(defn- -set-default-group-name
  [queue-name]
  (keyword (str (subs (str queue-name) 1) "-group")))

(defn- -check-group-exists
  [^Exception e]
  (let [msg (.getMessage e)]
    (if (re-find #"(?i)(consumer.+group.+?already\s+exists|BUSYGROUP)" msg)
      [:group-exists nil]
      [nil (util/make-exception e)])))

(defn- -initialize-stream
  "Note/hack: it seems sometimes we get error-as-value (ret) and other times this throws.
  This is why we call -check-group-exists in both cases."
  [conn queue-name group-name init-id]
  (try
    (let [ret-or-ex-info (pcar conn
                               (car/xgroup-create
                                 (util/->str queue-name)
                                 (util/->str group-name)
                                 init-id
                                 :mkstream))]
      (if (= "OK" ret-or-ex-info)
        [:group-created nil]
        (-check-group-exists ret-or-ex-info)))
    (catch Exception e
      (-check-group-exists e))))

(defn create-queue!
  "Creates (or resets) a queue spec and, if it doesn't exist, optionally
  creates the stream key and consumer group. Returns vector of `[status ?err]`."
  [queue-name conn & {:keys [group init-id]}]
  (let [group-name (or group (-set-default-group-name queue-name))
        init-id (or init-id 0)
        args {:conn conn
              :queue-name queue-name
              :init-id init-id
              :group group-name}]
    (if-let [args-err (v/invalidate qv/CreateQueueArgs args)]
      [nil (e/error {:potamic/err-type :potamic/args-err
                     :potamic/err-fatal? false
                     :potamic/err-msg "Invalid args provided to potamic.queue/create-queue!"
                     :potamic/err-data {:args (util/remove-conn args)
                                        :err args-err}})]
      (let [[stream-status ?err] (-initialize-stream conn queue-name group-name init-id)]
        (if ?err
          [nil ?err]
          (let [spec-exists? (boolean (get-queue queue-name))
                spec {:queue-name queue-name
                      :queue-conn conn
                      :group-name group-name
                      :redis-queue-name (util/->str queue-name)
                      :redis-group-name (util/->str group-name)}]
            (swap! queues/queues_ assoc queue-name spec)
            [(if spec-exists?
               (case stream-status
                 :group-created :updated-with-new-stream
                 :group-exists :updated-with-existing-stream)
               (case stream-status
                 :group-created :created-with-new-stream
                 :group-exists :created-with-existing-stream))
             nil]))))))

(defn put
  "Put message(s) onto a queue. Returns vector of `[?msg-ids ?err]`."
  [queue-name & xs]
  (if-let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (let [x (first xs)
          id-set? (or (string? x)
                      (keyword? x)
                      (symbol? x))
          id (if id-set? (name x) "*")
          msgs (map util/encode-map-vals
                    (if id-set? (rest xs) xs))]
      (try
        (let [r (-unwrap-pcar
                  (pcar conn
                        :as-pipeline
                        (mapv #(apply car/xadd qname id (reduce into [] %))
                              msgs)))]
          [r nil])
        (catch Exception e
          (let [err {:potamic/err-type :potamic/internal-err
                     :potamic/err-fatal? false
                     :potamic/err-msg (.getMessage e)
                     :potamic/err-data {:queue-name queue-name
                                        :err (util/make-exception e)}}]
            [nil err]))))
    [nil (-unknown-queue-err "potamic.queue/put" queue-name)]))

(defn- -make-read-result
  "Return lazy sequence of messages for `read*` functions."
  [r]
  (map (fn [[id msg-kvs]]
         {:id id
          :msg (->> (apply hash-map msg-kvs)
                    (map (fn [[k v]] [(util/<-str k) v]))
                    (into {}))})
       r))

(defn read
  "Reads messages from a queue via `XREAD`. Returns vector of `[?msgs ?err]`."
  [queue-name & {:keys [start block] cnt :count :or {start 0}}]
  (if-let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (try
      (let [cmd (util/prep-cmd
                  [(when cnt [:count cnt])
                   (when block [:block (util/time->milliseconds block)])
                   [:streams qname start]])
            raw (-unwrap-pcar (pcar conn (apply car/xread cmd)))
            res (-> raw first second -make-read-result)]
        [(seq res) nil])
      (catch Exception e
        [nil (util/make-exception e)]))
    [nil (-unknown-queue-err "potamic.queue/read" queue-name)]))

(defn read-range
  "Reads a range of messages from a queue via `XRANGE` (oldest to newest).
  Returns vector of `[?msgs ?err]`."
  [queue-name & {:keys [start end] cnt :count :as opts}]
  (let [start (or start "-")
        end (or end "+")
        args (assoc opts
                    :queue-name queue-name
                    :start start
                    :end end
                    :count cnt)]
    (if-let [args-err (v/invalidate qv/ReadRangeArgs args)]
      [nil
       (e/error {:potamic/err-type :potamic/args-err
                 :potamic/err-fatal? false
                 :potamic/err-msg "Invalid args provided to potamic.queue/read-range"
                 :potamic/err-data {:args args :err args-err}})]
      (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
        (try
          (let [cmd (util/prep-cmd [[qname start end]
                                    (when cnt [:count cnt])])
                raw (-unwrap-pcar (pcar conn (apply car/xrange cmd)))
                res (-make-read-result raw)]
            [(seq res) nil])
          (catch Exception e
            [nil (util/make-exception e)]))))))

(defn read-rev-range
  "Reads a reverse range of messages from a queue via `XREVRANGE` (newest to oldest).
  `:start` defaults to `\"+\"` (newest) and `:end` defaults to `\"-\"` (oldest).
  Returns vector of `[?msgs ?err]`."
  [queue-name & {:keys [start end] cnt :count :as opts}]
  (let [start (or start "+")
        end (or end "-")
        args (assoc opts
                    :queue-name queue-name
                    :start start
                    :end end
                    :count cnt)]
    (if-let [args-err (v/invalidate qv/ReadRangeArgs args)]
      [nil
       (e/error {:potamic/err-type :potamic/args-err
                 :potamic/err-fatal? false
                 :potamic/err-msg "Invalid args provided to potamic.queue/read-rev-range"
                 :potamic/err-data {:args args :err args-err}})]
      (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
        (try
          (let [cmd (util/prep-cmd [["XREVRANGE" qname start end]
                                    (when cnt [:count cnt])])
                raw (-unwrap-pcar (pcar conn (car/redis-call cmd)))
                res (-make-read-result raw)]
            [(seq res) nil])
          (catch Exception e
            [nil (util/make-exception e)]))))))

(defn read-next!
  "Reads next message(s) from a queue as consumer for queue group via `XREADGROUP`,
  side-effecting Redis' Pending Entries List. Returns vector of `[?msgs ?err]`."
  [consume & {:keys [from as block]}]
  (if-let [{qname :redis-queue-name
            group :redis-group-name
            conn :queue-conn} (get-queue from)]
    (try
      (let [cmd (util/prep-cmd
                  [[:group group as]
                   (when block [:block (util/time->milliseconds block)])
                   (when (not= consume :all) [:count consume])
                   [:streams qname ">"]])
            raw (-unwrap-pcar (pcar conn (apply car/xreadgroup cmd)))
            res (-> raw first second -make-read-result)]
        [(seq res) nil])
      (catch Exception e
        [nil (util/make-exception e)]))
    [nil (-unknown-queue-err "potamic.queue/read-next!" from)]))

(defn- -make-pending-summary
  "Returns `summary` map for `potamic.queue/read-pending-summary`."
  [raw-response]
  (let [[total start end consumers] raw-response]
    {:total total
     :start start
     :end end
     :consumers (into {} (for [[k v] consumers]
                           [(util/<-str k)
                            (util/->int v)]))}))

(defn read-pending-summary
  "Lists summary of all pending messages across consumers for `queue-name`.
  Returns vector of `[?summary ?err]`."
  [queue-name]
  (if-let [{qname :redis-queue-name
            group :redis-group-name
            conn :queue-conn} (get-queue queue-name)]
    (try
      (let [cmd (util/prep-cmd [[qname group]])
            raw (-unwrap-pcar (pcar conn (apply car/xpending cmd)))
            res (-make-pending-summary raw)]
        [res nil])
      (catch Exception e
        [nil (util/make-exception e)]))
    [nil (-unknown-queue-err "potamic.queue/read-pending-summary" queue-name)]))

(defn- -make-pending-result
  [r]
  (mapv (fn [[id c ms n]]
          {:id id
           :consumer c
           :milliseconds-since-delivered ms
           :times-delivered n})
        r))

(defn read-pending
  "Lists details of pending messages for a `queue`/`group` pair. Optionally,
  a `consumer` (`:for`) may be provided for sub-filtering. Returns vector of `[?details ?err]`."
  [count* & {:keys [from start end] consumer :for :as opts}]
  (let [start (or start "-")
        end (or end "+")
        args (assoc opts
                    :from from
                    :for consumer
                    :start start
                    :end end
                    :count count*)]
    (if-let [args-err (v/invalidate qv/ReadPendingArgs args)]
      [nil
       (e/error {:potamic/err-type :potamic/args-err
                 :potamic/err-fatal? false
                 :potamic/err-msg "Invalid args provided to potamic.queue/read-pending"
                 :potamic/err-data {:args args :err args-err}})]
      (let [{qname :redis-queue-name
             group :redis-group-name
             conn :queue-conn} (get-queue from)]
        (try
          (let [cmd (util/prep-cmd [[qname group start end count*]
                                    (when consumer consumer)])
                raw (-unwrap-pcar (pcar conn (apply car/xpending cmd)))
                res (-make-pending-result raw)]
            [(lazy-seq res) nil])
          (catch Exception e
            [nil
             (e/error {:potamic/err-type :potamic/args-err
                       :potamic/err-msg (.getMessage e)
                       :potamic/err-data {:args args
                                          :err (util/make-exception e)}})]))))))

(defn claim-pending!
  "Claims ownership of pending message(s) via `XCLAIM` for consumer `:as` when
  idle time >= `:min-idle-ms`. Returns vector of `[?msgs ?err]`."
  [queue-name & {:keys [as min-idle-ms ids group] :as opts}]
  (let [args (assoc opts :queue-name queue-name)]
    (if-let [args-err (v/invalidate qv/ClaimPendingArgs args)]
      [nil
       (e/error {:potamic/err-type :potamic/args-err
                 :potamic/err-fatal? false
                 :potamic/err-msg "Invalid args provided to potamic.queue/claim-pending!"
                 :potamic/err-data {:args args :err args-err}})]
      (let [{qname :redis-queue-name
             default-group :redis-group-name
             conn :queue-conn} (get-queue queue-name)
            resolved-group (if group (util/->str group) default-group)]
        (try
          (let [cmd (util/prep-cmd [["XCLAIM" qname resolved-group as min-idle-ms]
                                    ids])
                raw (-unwrap-pcar (pcar conn (car/redis-call cmd)))
                res (-make-read-result raw)]
            [(seq res) nil])
          (catch Exception e
            [nil
             (e/error {:potamic/err-type :potamic/db-err
                       :potamic/err-fatal? false
                       :potamic/err-msg (.getMessage e)
                       :potamic/err-data {:args args
                                          :err (util/make-exception e)}})]))))))

(defn autoclaim-pending!
  "Atomically scans and claims stale PEL messages via `XAUTOCLAIM` starting at `:start`
  (default `\"0-0\"`). Returns vector of `[{:next-start-id ID :msgs ?msgs} ?err]`."
  [queue-name & {:keys [as min-idle-ms start group] cnt :count :as opts}]
  (let [start (or start "0-0")
        args (assoc opts :queue-name queue-name :start start)]
    (if-let [args-err (v/invalidate qv/AutoclaimPendingArgs args)]
      [nil
       (e/error {:potamic/err-type :potamic/args-err
                 :potamic/err-fatal? false
                 :potamic/err-msg "Invalid args provided to potamic.queue/autoclaim-pending!"
                 :potamic/err-data {:args args :err args-err}})]
      (let [{qname :redis-queue-name
             default-group :redis-group-name
             conn :queue-conn} (get-queue queue-name)
            resolved-group (if group (util/->str group) default-group)]
        (try
          (let [cmd (util/prep-cmd [["XAUTOCLAIM" qname resolved-group as min-idle-ms start]
                                    (when cnt ["COUNT" cnt])])
                [next-id raw-entries] (-unwrap-pcar (pcar conn (car/redis-call cmd)))
                msgs (seq (-make-read-result raw-entries))]
            [{:next-start-id next-id
              :msgs msgs}
             nil])
          (catch Exception e
            [nil
             (e/error {:potamic/err-type :potamic/db-err
                       :potamic/err-fatal? false
                       :potamic/err-msg (.getMessage e)
                       :potamic/err-data {:args args
                                          :err (util/make-exception e)}})]))))))

(defn set-processed!
  "Removes message(s) from Redis' Pending Entries List via `XACK`.
  Returns vector of `[?n-acked ?err]`."
  [queue-name & msg-ids]
  (if-let [{qname :redis-queue-name
            group :redis-group-name
            conn :queue-conn} (get-queue queue-name)]
    (if (empty? msg-ids)
      [0 nil]
      (try
        (let [cmd (util/prep-cmd [(into [qname group] msg-ids)])
              n-acked (-unwrap-pcar (pcar conn (apply car/xack cmd)))]
          [n-acked nil])
        (catch Exception e
          [nil (util/make-exception e)])))
    [nil (-unknown-queue-err "potamic.queue/set-processed!" queue-name)]))

(defn del-msg!
  "Deletes specific message ID(s) from a stream via `XDEL`.
  Returns vector of `[?n-deleted ?err]`."
  [queue-name & msg-ids]
  (if-let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (if (empty? msg-ids)
      [0 nil]
      (try
        (let [cmd (util/prep-cmd [(into ["XDEL" qname] msg-ids)])
              n-deleted (-unwrap-pcar (pcar conn (car/redis-call cmd)))]
          [n-deleted nil])
        (catch Exception e
          [nil (util/make-exception e)])))
    [nil (-unknown-queue-err "potamic.queue/del-msg!" queue-name)]))

(defn trim!
  "Trims a stream via `XTRIM` using either `:maxlen` or `:minid` (with optional `:approx?`).
  Returns vector of `[?n-trimmed ?err]`."
  [queue-name & {:keys [maxlen minid approx?] :as opts}]
  (let [args (assoc opts :queue-name queue-name)]
    (if-let [args-err (v/invalidate qv/TrimQueueArgs args)]
      [nil
       (e/error {:potamic/err-type :potamic/args-err
                 :potamic/err-fatal? false
                 :potamic/err-msg "Invalid args provided to potamic.queue/trim!"
                 :potamic/err-data {:args args :err args-err}})]
      (let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)
            op (if (some? maxlen) "MAXLEN" "MINID")
            mod-sym (if approx? "~" "=")
            threshold (if (some? maxlen) maxlen minid)]
        (try
          (let [cmd (util/prep-cmd [["XTRIM" qname op mod-sym threshold]])
                n-trimmed (-unwrap-pcar (pcar conn (car/redis-call cmd)))]
            [n-trimmed nil])
          (catch Exception e
            [nil
             (e/error {:potamic/err-type :potamic/db-err
                       :potamic/err-fatal? false
                       :potamic/err-msg (.getMessage e)
                       :potamic/err-data {:args args
                                          :err (util/make-exception e)}})]))))))

(defn stream-len
  "Returns the number of entries in a stream via `XLEN` as `[?len ?err]`."
  [queue-name]
  (if-let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (try
      [(-unwrap-pcar (pcar conn (car/redis-call ["XLEN" qname]))) nil]
      (catch Exception e
        [nil (util/make-exception e)]))
    [nil (-unknown-queue-err "potamic.queue/stream-len" queue-name)]))

(defn stream-info
  "Returns stream metadata via `XINFO STREAM` as `[?info-map ?err]`."
  [queue-name]
  (if-let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (try
      (let [raw (-unwrap-pcar (pcar conn (car/redis-call ["XINFO" "STREAM" qname])))]
        [(walk/keywordize-keys (apply hash-map raw)) nil])
      (catch Exception e
        [nil (util/make-exception e)]))
    [nil (-unknown-queue-err "potamic.queue/stream-info" queue-name)]))

(defn stream-groups
  "Returns consumer group metadata via `XINFO GROUPS` as `[?groups ?err]`."
  [queue-name]
  (if-let [{qname :redis-queue-name conn :queue-conn} (get-queue queue-name)]
    (try
      (let [raw (-unwrap-pcar (pcar conn (car/xinfo-groups qname)))]
        [(mapv #(walk/keywordize-keys (apply hash-map %)) raw) nil])
      (catch Exception e
        [nil (util/make-exception e)]))
    [nil (-unknown-queue-err "potamic.queue/stream-groups" queue-name)]))

(defn stream-consumers
  "Returns consumer metadata for the queue's group via `XINFO CONSUMERS` as `[?consumers ?err]`."
  [queue-name]
  (if-let [{qname :redis-queue-name
            group :redis-group-name
            conn :queue-conn} (get-queue queue-name)]
    (try
      (let [raw (-unwrap-pcar (pcar conn (car/redis-call ["XINFO" "CONSUMERS" qname group])))]
        [(mapv #(walk/keywordize-keys (apply hash-map %)) raw) nil])
      (catch Exception e
        [nil (util/make-exception e)]))
    [nil (-unknown-queue-err "potamic.queue/stream-consumers" queue-name)]))

(defn destroy-queue!
  "Destroys a queue spec, the stream key and the consumer group associated with
  it. Without the `:unsafe` option, an error will be returned if there are
  pending messages. Returns vector of `[status ?err]`."
  [queue-name conn & {:keys [unsafe]}]
  (let [args {:conn conn :queue-name queue-name :unsafe unsafe}]
    (if-let [args-err (v/invalidate qv/DestroyQueueArgs args)]
      [nil (e/error {:potamic/err-type :potamic/args-err
                     :potamic/err-fatal? false
                     :potamic/err-msg "Invalid args provided to potamic.queue/destroy-queue"
                     :potamic/err-data {:args (util/remove-conn args)
                                        :err args-err}})]
      (let [spec (get-queue queue-name)
            qname-str (util/->str queue-name)
            key-exists? (db/key-exists? qname-str conn)]
        (if-not key-exists?
          (do
            (swap! queues/queues_ dissoc queue-name)
            (if spec
              [:spec-destroyed_stream-nonexistent nil]
              [:spec-nonexistent_stream-nonexistent nil]))
          (let [groups (mapv #(walk/keywordize-keys (apply hash-map %))
                             (-unwrap-pcar (pcar conn (car/xinfo-groups qname-str))))
                has-pending? (boolean (some #(pos-int? (or (:pending %) 0)) groups))]
            (if (and (not unsafe) has-pending?)
              [nil
               (e/error
                 {:potamic/err-type :potamic/db-err
                  :potamic/err-fatal? false
                  :potamic/err-msg (str "Cannot destroy " queue-name ", it has pending messages")
                  :potamic/err-data {:args (util/remove-conn args)
                                     :groups groups}})]
              (try
                (swap! queues/queues_ dissoc queue-name)
                (-unwrap-pcar
                  (pcar conn
                        :as-pipeline
                        (conj (mapv #(car/xgroup-destroy qname-str (:name %)) groups)
                              (car/del qname-str))))
                (if spec
                  [:spec-destroyed_stream-destroyed nil]
                  [:spec-nonexistent_stream-destroyed nil])
                (catch Exception e
                  [nil
                   (e/error {:potamic/err-type :potamic/db-err
                             :potamic/err-fatal? false
                             :potamic/err-msg (.getMessage e)
                             :potamic/err-data
                             {:queue-name queue-name
                              :err (util/make-exception e)}})])))))))))
