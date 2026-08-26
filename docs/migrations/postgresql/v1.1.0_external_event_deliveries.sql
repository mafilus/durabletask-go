-- durabletask-go v1.1.0
--
-- Run this migration before any v1.1.0 process accepts external deliveries.
-- It is safe to run more than once.
CREATE TABLE IF NOT EXISTS ExternalEventDeliveries (
    InstanceID TEXT NOT NULL,
    EventName TEXT NOT NULL,
    DeliveryID TEXT NOT NULL,

    PRIMARY KEY (InstanceID, EventName, DeliveryID)
);
