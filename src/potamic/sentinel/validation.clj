(ns potamic.sentinel.validation
  "Validation for `st.queue` library."
  {:added "5.0"
   :author "Chad Angelelli"}
  (:require [malli.core :as malli]
            [potamic.queue.validation :refer [QueueValue]]
            [potamic.db.validation :refer [Conn]]
            [potamic.validation :as v]))

(def CreateSentinelArgs
  (malli/schema
    [:map {:closed true}
     [:queue-conn Conn]
     [:queue-name QueueValue]
     [:queue-group QueueValue]
     [:frequency int?]
     [:handler (v/f fn? "Handler must be a function")]
     [:init-id {:optional true} int?]
     [:start-offset {:optional true} int?]]))

