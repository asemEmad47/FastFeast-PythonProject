-- ============================================================
-- FastFeast Data Warehouse — DDL Script
-- Schema: FASTFEASTDWH
-- ============================================================

-- ────────────────────────────────────────────────────────────
-- DateDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.DateDim (
    date_id         INT             NOT NULL,
    full_date       DATE            NOT NULL,
    day             VARCHAR         NOT NULL,
    month           VARCHAR         NOT NULL,
    quarter         VARCHAR         NOT NULL,
    year            INT             NOT NULL,
    day_of_week     INT             NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,

    CONSTRAINT pk_date PRIMARY KEY (date_id)
);

-- ────────────────────────────────────────────────────────────
-- CitiesDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.CitiesDim (
    city_id         INT             NOT NULL,
    city_name       VARCHAR         NOT NULL,
    country         VARCHAR         NOT NULL,
    timezone        VARCHAR         NOT NULL,

    CONSTRAINT pk_city PRIMARY KEY (city_id)
);

-- ────────────────────────────────────────────────────────────
-- RegionsDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.RegionsDim (
    region_id           INT             NOT NULL,
    region_name         VARCHAR         NOT NULL,
    city_id             INT             NOT NULL,
    delivery_base_fee   FLOAT           NOT NULL,

    CONSTRAINT pk_region PRIMARY KEY (region_id)
);

-- ────────────────────────────────────────────────────────────
-- ChannelsDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.ChannelsDim (
    channel_id      INT             NOT NULL,
    channel_name    VARCHAR         NOT NULL,

    CONSTRAINT pk_channel PRIMARY KEY (channel_id)
);

-- ────────────────────────────────────────────────────────────
-- ReasonsDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.ReasonsDim (
    reason_id               INT             NOT NULL,
    reason_name             VARCHAR         NOT NULL,
    reason_category_name    VARCHAR         NOT NULL,
    severity_level          INT             NOT NULL,
    typical_refund_pct      FLOAT           NOT NULL,

    CONSTRAINT pk_reason PRIMARY KEY (reason_id)
);

-- ────────────────────────────────────────────────────────────
-- AgentsDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.AgentsDim (
    agent_id                INT             NOT NULL,
    agent_name              VARCHAR         NOT NULL,
    agent_email             VARCHAR         NOT NULL,
    agent_phone             INT             NOT NULL,
    skill_level             VARCHAR         NOT NULL,
    hire_date               DATE            NOT NULL,
    team_name               VARCHAR,
    avg_handle_time_min     DOUBLE          NOT NULL,
    resolution_rate         FLOAT,
    csat_score              FLOAT,
    is_active               BOOLEAN         NOT NULL,
    created_at              TIMESTAMP_NTZ   NOT NULL,
    updated_at              TIMESTAMP_NTZ   NOT NULL,

    CONSTRAINT pk_agent PRIMARY KEY (agent_id)
);

-- ────────────────────────────────────────────────────────────
-- CustomersDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.CustomersDim (
    customer_id         INT             NOT NULL,
    full_name           VARCHAR,
    email               VARCHAR,
    phone               INT,
    signup_date         DATE            NOT NULL,
    gender              VARCHAR         NOT NULL,
    created_at          TIMESTAMP_NTZ   NOT NULL,
    updated_at          TIMESTAMP_NTZ   NOT NULL,
    segment_name        VARCHAR,
    priority_support    BOOLEAN         NOT NULL,
    discount_pct        FLOAT           NOT NULL,

    CONSTRAINT pk_customer PRIMARY KEY (customer_id)
);

-- ────────────────────────────────────────────────────────────
-- DriversDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.DriversDim (
    driver_id               INT             NOT NULL,
    driver_name             VARCHAR         NOT NULL,
    driver_phone            INT             NOT NULL,
    national_id             INT             NOT NULL,
    shift                   VARCHAR         NOT NULL,
    vehicle_type            VARCHAR         NOT NULL,
    hire_date               DATE            NOT NULL,
    rating_avg              FLOAT,
    on_time_rate            FLOAT,
    cancel_rate             FLOAT,
    completed_deliveries    INT             NOT NULL,
    is_active               BOOLEAN         NOT NULL,
    created_at              TIMESTAMP_NTZ   NOT NULL,
    updated_at              TIMESTAMP_NTZ   NOT NULL,

    CONSTRAINT pk_driver PRIMARY KEY (driver_id)
);

-- ────────────────────────────────────────────────────────────
-- RestaurantsDim
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.RestaurantsDim (
    restaurant_id       INT             NOT NULL,
    category_name       VARCHAR,
    restaurant_name     VARCHAR         NOT NULL,
    price_tier          VARCHAR         NOT NULL,
    rating_avg          FLOAT,
    prep_time_avg_min   INT             NOT NULL,
    is_active           BOOLEAN         NOT NULL,
    created_at          TIMESTAMP_NTZ   NOT NULL,
    updated_at          TIMESTAMP_NTZ   NOT NULL,

    CONSTRAINT pk_restaurant PRIMARY KEY (restaurant_id)
);

-- ────────────────────────────────────────────────────────────
-- FactTickets
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FASTFEASTDWH.FactTickets (
    transaction_id              INT             NOT NULL AUTOINCREMENT,
    ticket_id                   VARCHAR         NOT NULL,
    order_id                    VARCHAR         NOT NULL,
    customer_id                 INT             NOT NULL,
    agent_id                    INT             NOT NULL,
    driver_id                   INT             NOT NULL,
    restaurant_id               INT             NOT NULL,
    region_id                   INT             NOT NULL,
    reason_id                   INT             NOT NULL,
    channel_id                  INT             NOT NULL,
    date_key_created            INT             NOT NULL,
    date_key_first_response     INT,
    date_key_resolved           INT,
    status                      VARCHAR         NOT NULL,
    order_amount                DOUBLE          NOT NULL,
    delivery_fee                DOUBLE          NOT NULL,
    discount_amount             FLOAT           NOT NULL,
    total_amount                DOUBLE          NOT NULL,
    revenue_impact              DOUBLE,
    refund_amount               DOUBLE,
    resolution_time_min         DOUBLE,
    first_response_time_min     DOUBLE,
    sla_first_response_breached BOOLEAN         NOT NULL,
    sla_resolution_breached     BOOLEAN         NOT NULL,

    CONSTRAINT pk_fact_tickets PRIMARY KEY (transaction_id)
);