(ns potamic.queue.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v]
            [potamic.db.validation :as dbv]
            [potamic.queue.queues :as queues]))

(def queue-exists? (v/f (fn [x] (get @queues/queues_ x)) "Unknown queue"))

(def QueueValue [:or keyword? symbol? string?])

(def CreateQueueArgs
  (malli/schema
    [:map {:closed true}
     [:queue-name QueueValue]
     [:conn dbv/Conn]
     [:group {:optional true} [:or keyword? symbol? string? nil?]]
     [:init-id {:optional true} [:or int? string?]]]))

(def DestroyQueueArgs
  (malli/schema
    [:map {:closed true}
     [:queue-name [:and QueueValue]]
     [:conn dbv/Conn]
     [:unsafe {:optional true} [:maybe :boolean]]]))

(def ReadPendingArgs
  (malli/schema
    [:map {:closed true}
     [:count int?]
     [:from [:and QueueValue queue-exists?]]
     [:for {:optional true} [:or keyword? symbol? string?]]
     [:start {:optional true} [:or keyword? symbol? string?]]
     [:end {:optional true} [:or keyword? symbol? string?]]]))

(def ReadRangeArgs
  (malli/schema
    [:map {:closed true}
     [:queue-name [:and QueueValue queue-exists?]]
     [:count {:optional true} [:or int? nil?]]
     [:start {:optional true} [:or keyword? symbol? string?]]
     [:end {:optional true} [:or keyword? symbol? string?]]]))
