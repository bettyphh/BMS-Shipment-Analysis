-- use database;
-- use schema ;


-- shipment analysis from Brent
--BMSDSVPilot: Portal 127
WITH planned_shipment_duration (shipment_id, planned_duration)
     AS (SELECT ss.shipment_id,
                Sum(duration)
         FROM   planned_routes
         JOIN   shipment_segments ss on planned_routes.shipment_segment_id = ss.id
         WHERE 1=1
         AND ss.state NOT IN ( 'cancelled', 'failed' )
         GROUP  BY shipment_id),
         
BMS_shipment_events AS(
 SELECT shipments.id AS OH_ID,
       extra_reference_numbers.broker AS Broker_Ref,
       shipments.state AS Shipment_Status,
       shipments.CREATED_AT as Shipment_Created_TS,
       to_char(shipments.CREATED_AT, 'mm/dd/yyyy') as Shipment_Date_Created,
       to_char(shipments.CREATED_AT, 'HH24:MI') as Shipment_Time_created,
       Json_extract_path_text(location_templates.kinds, 'origin.title')  AS Origin,
       locations.id as origin_location_id,
       shipment_events.ext_c_at as Departure_TS,
       to_char(shipment_events.ext_c_at, 'mm/dd/yyyy') as Departure_Date,
       to_char(shipment_events.ext_c_at, 'HH24:MI') as Departure_Time,
       Json_extract_path_text(location_templates_locations.kinds, 'destination.title') AS Destination,
       final_delivery_locations_shipments.ID as destination_location_id,
       deliver_shipment_events_locations.ext_c_at as Delivery_TS,
       to_char(deliver_shipment_events_locations.ext_c_at, 'mm/dd/yyyy') as Delivery_Date,
       to_char(deliver_shipment_events_locations.ext_c_at, 'HH24:MI') as Delivery_Time,
       array_agg(DISTINCT(companies.legal_name)) AS Carrier,
       array_agg(DISTINCT concat_ws(users.first_name, users.last_name)) AS Drivers,
       count(distinct users.ID) as num_drivers,
       count(accepted_driver_access_links_shipments_join.id) as num_access_links,
       array_agg(DISTINCT devices.imei) AS Device_IMEI,
       array_agg(DISTINCT device_types.name) AS Device_Type,
       portals.name AS portal_name,
       portals.id AS portals_id,
       enter_delivery_shipment_events_locations.ext_c_at       AS enter_delivery_ts,
       finish_shipment_events_locations.ext_c_at               AS finish_shipment_ts,
       sd.planned_duration,
       datediff("minutes", shipments.created_at, departure_ts )  AS departure_delay,
       datediff("minutes", departure_ts, deliver_shipment_events_locations.ext_c_at)  AS actual_duration
FROM shipments
INNER JOIN access_links
  ON access_links.shipment_id = shipments.id
  AND access_links.opportunity = 'broker_opportunity'
  AND access_links.rule = 1
INNER JOIN employees
  ON employees.id = access_links.employee_id
INNER JOIN portals
  ON portals.id = employees.portal_id
INNER JOIN planned_shipment_duration sd
  ON sd.shipment_id = shipments.id
INNER JOIN shipment_segments as start_segment
  ON start_segment.shipment_id = shipments.id
  AND start_segment."START" = true
INNER JOIN shipment_segments as end_segment
  ON end_segment.shipment_id = shipments.id
  AND end_segment.final = true
LEFT OUTER JOIN extra_reference_numbers
  ON extra_reference_numbers.shipment_id = shipments.id
LEFT OUTER JOIN access_links actual_carrier_access_links_shipments_join
  ON actual_carrier_access_links_shipments_join.shipment_id = shipments.id
  AND actual_carrier_access_links_shipments_join.accepted = TRUE
  AND actual_carrier_access_links_shipments_join.opportunity = 'carrier_opportunity'
  AND actual_carrier_access_links_shipments_join.generation = 0
LEFT OUTER JOIN employees actual_carrier_employees_shipments_join
  ON actual_carrier_employees_shipments_join.id = actual_carrier_access_links_shipments_join.employee_id
LEFT OUTER JOIN companies
  ON companies.id = actual_carrier_employees_shipments_join.company_id
LEFT OUTER JOIN access_links accepted_driver_access_links_shipments_join
  ON accepted_driver_access_links_shipments_join.shipment_id = shipments.id
  AND accepted_driver_access_links_shipments_join.accepted = TRUE
  AND accepted_driver_access_links_shipments_join.opportunity = 'driver_opportunity'
LEFT OUTER JOIN employees actual_driver_employees_shipments_join
  ON actual_driver_employees_shipments_join.id = accepted_driver_access_links_shipments_join.employee_id
LEFT OUTER JOIN users
  ON users.id = actual_driver_employees_shipments_join.user_id
LEFT OUTER JOIN shipment_devices
  ON shipment_devices.shipment_id = shipments.id
  AND shipment_devices.status != 'hidden'
LEFT OUTER JOIN devices
  ON devices.id = shipment_devices.device_id
LEFT OUTER JOIN device_types
  ON device_types.id = devices.device_type_id
LEFT OUTER JOIN locations
  ON locations.shipment_segment_id = start_segment.id
  AND locations.type = 'PickupLocation'
  AND locations."START" = TRUE
LEFT OUTER JOIN shipment_events
  ON shipment_events.location_id = locations.id
  AND shipment_events.type = 'DepartureShipmentEvent'
LEFT OUTER JOIN location_templates
  ON location_templates.id = locations.location_template_id
LEFT OUTER JOIN locations final_delivery_locations_shipments
ON final_delivery_locations_shipments.shipment_segment_id = end_segment.id
  AND final_delivery_locations_shipments.type = 'DeliveryLocation'
  AND final_delivery_locations_shipments.final = TRUE
LEFT OUTER JOIN shipment_events deliver_shipment_events_locations
  ON deliver_shipment_events_locations.location_id = final_delivery_locations_shipments.id
  AND deliver_shipment_events_locations.type = 'DeliverShipmentEvent'
LEFT OUTER JOIN shipment_events enter_delivery_shipment_events_locations
  ON enter_delivery_shipment_events_locations.location_id = final_delivery_locations_shipments.id
  AND enter_delivery_shipment_events_locations.type = 'EnterDeliveryLocationShipmentEvent'
LEFT OUTER JOIN shipment_events finish_shipment_events_locations
  ON finish_shipment_events_locations.location_id = final_delivery_locations_shipments.id
  AND finish_shipment_events_locations.type = 'FinishShipmentEvent'
LEFT OUTER JOIN location_templates location_templates_locations
  ON location_templates_locations.id = final_delivery_locations_shipments.location_template_id
WHERE portals.id = 81
  -- AND "shipments"."created_at" BETWEEN '2020-01-01' AND '2020-09-21'
 -- AND ("shipments"."created_at" >= (date(now() - interval '8' day) + interval '0' second)
 -- AND "shipments"."created_at" < (date(now() - interval '1' day) - interval '1' second))
-- AND shipments.created_at BETWEEN  dateadd(day, -8, current_date) AND dateadd(day, -1, current_date)
GROUP BY shipments.id,
         shipments.state,
         shipments.CREATED_AT,
         extra_reference_numbers.broker,
         location_templates.kinds,
         locations.id,
         shipment_events.ext_c_at,
         location_templates_locations.kinds,
         final_delivery_locations_shipments.id,
         deliver_shipment_events_locations.ext_c_at,
         companies.legal_name,
         portals.id,
         portal_name,
         enter_delivery_shipment_events_locations.ext_c_at,
         finish_shipment_events_locations.ext_c_at,
         sd.planned_duration
)

// Select all
SELECT *
FROM BMS_shipment_events

// Shipment_Created_TS
// count of total shipments by Shipment_Created_TS: day
SELECT TO_DATE(Shipment_Created_TS) AS shipment_date, COUNT(DISTINCT OH_ID)
FROM BMS_shipment_events
GROUP BY 1
ORDER BY 1


// count of total shipments by Shipment_Created_TS: week
SELECT WEEK(Shipment_Created_TS) AS shipment_week, COUNT(DISTINCT OH_ID)
FROM BMS_shipment_events
GROUP BY 1
ORDER BY 1


// count of total shipments by Shipment_Created_TS: month 
SELECT MONTH(Shipment_Created_TS) AS shipment_month, COUNT(DISTINCT OH_ID)
FROM BMS_shipment_events
GROUP BY 1
ORDER BY 1



// Departure_Date
// count of total shipments by Delivery_Date: day
SELECT TO_DATE(Departure_TS) AS shipment_date, COUNT(DISTINCT OH_ID)
FROM BMS_shipment_events
GROUP BY 1
ORDER BY 1


//// count of total shipments by Departure_Date: week
SELECT WEEK(Departure_TS) AS shipment_week, COUNT(DISTINCT OH_ID)
FROM BMS_shipment_events
GROUP BY 1
ORDER BY 1


// count of total shipments by Departure_Date: month 
SELECT MONTH(Departure_TS) AS shipment_month, COUNT(DISTINCT OH_ID)
FROM BMS_shipment_events
GROUP BY 1
ORDER BY 1
