(ns potamic.queue.validation
  (:require [malli.core :as malli]
            [potamic.validation :as v]
            [potamic.db.validation :as dbv]
            [potamic.queue.queues :as queues]))

(def queue-exists? (v/f (fn [x] (get @queues/queues_ x)) "Unknown queue"))

(def valid-queue-value? [:or keyword? symbol? string?])

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

(def ClaimPendingArgs
  (malli/schema
    [:map {:closed true}
     [:queue-name [:and QueueValue queue-exists?]]
     [:as QueueValue]
     [:min-idle-ms int?]
     [:ids [:sequential [:or keyword? symbol? string?]]]
     [:group {:optional true} [:or keyword? symbol? string? nil?]]]))

(def AutoclaimPendingArgs
  (malli/schema
    [:map {:closed true}
     [:queue-name [:and QueueValue queue-exists?]]
     [:as QueueValue]
     [:min-idle-ms int?]
     [:start {:optional true} [:or keyword? symbol? string?]]
     [:count {:optional true} [:or int? nil?]]
     [:group {:optional true} [:or keyword? symbol? string? nil?]]]))

(def TrimQueueArgs
  (malli/schema
    [:and
     [:map {:closed true}
      [:queue-name [:and QueueValue queue-exists?]]
      [:maxlen {:optional true} [:maybe int?]]
      [:minid {:optional true} [:maybe [:or keyword? symbol? string?]]]
      [:approx? {:optional true} [:maybe :boolean]]]
     [:fn {:error/message "Must specify either :maxlen or :minid (not both)"}
      (fn [{:keys [maxlen minid]}]
        (not= (some? maxlen) (some? minid)))]]))
