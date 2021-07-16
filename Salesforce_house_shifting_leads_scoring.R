
#library("tidyverse")
library("dplyr")
library("googlesheets4")
library("ggplot2")
#gs4_auth()

workbook = "https://docs.google.com/spreadsheets/d/1ugNaxM6bexg1yi0a-g6LO0EeOxJHdpRjxVVAZR8S0zk/"

data =data.frame(read_sheet(workbook, sheet = "Open Leads"))

#data = as.data.frame(data)
#names(data)
#data_filtered = filter(data,data$Calling.Timestamp == "Call not done" | data$Calling.Timestamp == "") 

data$days_remaining_for_shifting = difftime(data$House.Shifting.Date, as.Date(data$House.Shifting.Opportunity..Created.Date),units = "days") 

#data_filtered$days_remaining_for_shifting_WA =   ifelse(data_filtered$WA_Tentative.Shifting.Date == "NOW", 0,
#                                                        ifelse(data_filtered$WA_Tentative.Shifting.Date == "Within 2 Days",1,
#                                                               ifelse(data_filtered$WA_Tentative.Shifting.Date == "After 2 Days", 2, "")))

# data_filtered$days_remaining_for_shifting = ifelse(data_filtered$Source == "WhatsApp",data_filtered$days_remaining_for_shifting_WA,data_filtered$days_remaining_for_shifting)
# data_filtered$days_remaining_for_shifting = as.numeric(data_filtered$days_remaining_for_shifting)
# class(data$days_remaining_for_shifting)

customer_mobiles =  paste("'",
                          paste(unique(data$Mobile), collapse = "','"),
                          "'",
                          sep="")
customer_mobiles_1 =  paste("''",
                            paste(unique(data$Mobile), collapse = "'',''"),
                            "''",
                            sep="")

data_filtered = select(data, c("Mobile", "Source","Pickup.Address","Drop.Address","days_remaining_for_shifting","Count.of.order.interests"))

# Libraries
pckg_names <- c('RPostgreSQL',
                'futile.logger',
                'gsubfn',
                'data.table',
                'dplyr',
                
                'lubridate')
load_pckgs <- function(pckg_names){
  sapply(pckg_names, library, character.only = TRUE)
}
load_pckgs(pckg_names)
# Remote db connections
connect_to_db <- function(source){
  drv <- dbDriver("PostgreSQL")
  source("C:/Users/Porter Admin/Documents/connections.R")
  flog.info(fn$identity("Connecting to `source`"))
  if(source == "modeling"){
    connection <- dbConnect(
      drv,
      host = modeling_host,
      port = modeling_port,
      user = modeling_user,
      password = modeling_pwd,
      dbname = modeling_db
    )
  }else if (source == "oms"){
    connection <- dbConnect(
      drv,
      host = oms_host,
      port = oms_port,
      user = oms_user,
      password = oms_pwd,
      dbname = oms_db
    )
  }else if (source == "staging_pop"){
    connection <- dbConnect(
      drv,
      host = staging_pop_host,
      port = staging_pop_port,
      user = staging_pop_user,
      password = staging_pop_pwd,
      dbname = staging_pop_db
    )
  }else if (source == "staging_analytics"){
    connection <- dbConnect(
      drv,
      host = staging_analytics_host,
      port = staging_analytics_port,
      user = staging_analytics_user,
      password = staging_analytics_pwd,
      dbname = staging_analytics_db
    )
  }else if (source == "sfms"){
    connection <- dbConnect(
      drv,
      host = sf_host,
      port = sf_port,
      user = sf_user,
      password = sf_pwd,
      dbname = sf_db
    )
  }else if (source == "redshift"){
    connection <- dbConnect(
      drv,
      host = redshift_host,
      port = redshift_port,
      user = redshift_user,
      password = redshift_pwd,
      dbname = redshift_db
    )
  }
  return(connection)
}
run_query <- function(source, query_statement){
  # Runs a Query Statement on the connection source
  # Output: Query Data
  # Define Connection from Source
  connection <- connect_to_db(source)
  start_time <- Sys.time()
  # Get Query Data
  query_data <- tryCatch({
    dbGetQuery(
      conn = connection,
      statement = query_statement )
  }, error = function(e) {
    print('Error')
  }, finally = {
    dbDisconnect(conn = connection)
  })
  end_time <- Sys.time()  
  print(end_time-start_time)
  # Return Query Data
  return(query_data)
}

query = fn$identity(" WITH house_shifting_customers AS (select mobile from customers where mobile in (`customer_mobiles`)
)

, customers_who_installed_apps AS (
  SELECT
  *
    FROM
  dblink('host=sfms-prod-psql-replica.porter.in port=5432 user=analytics_mradul_badaya password=resfeber123 dbname=sfms_production',
         'SELECT     
															    install.mobile,
															    install.source,
															    (request_ts + INTERVAL ''5.5 Hrs'')::DATE AS app_install_date,
															    detail.geo_region_curated AS geo_region,
															    CASE WHEN attrib.attribution = ''attribution_paid'' then 1 
															         else 0 END AS lead_channel_attribution
															  
															FROM sf_customer_signup_requests  install
															LEFT JOIN sf_lead_Spot_detail_phones  phone
															ON install.mobile = phone.number
															
															LEFT JOIN sf_lead_spot_details  detail
															ON detail.id = phone.detail_id
															
															LEFT JOIN sf_lead_spot_registration_attributions attrib
															ON attrib.lead_id = detail.lead_spot_id
																WHERE install.mobile IN (`customer_mobiles_1`)'
         
  ) AS T (mobile VARCHAR,
          source VARCHAR,
          app_install_date DATE,
          geo_region VARCHAR,
          lead_channel_attribution INT)
)
,customer_list AS (
  SELECT DISTINCT ON (mobile,source)
  *
    FROM customers_who_installed_apps
  ORDER BY mobile,source,app_install_date DESC
)
, final_customer_list AS(
  select 
  a.mobile,b.source,b.app_install_date,b.geo_region,b.lead_channel_attribution
  FROM  house_shifting_customers a
  left join customer_list B
  on a.mobile = b.mobile)

,customer_order_data AS (
  SELECT 
  a.*,
  
  cus.geo_region_id,    
  cus.id AS customer_id,
  MAX (to_timestamp(pickup_time + 19800))::DATE AS last_order_date,
  (MAX (to_timestamp(pickup_time + 19800))::DATE - a.app_install_date) AS gap_between_app_install_first_order,
  COALESCE (COUNT (order_id),0) AS total_orders 
  
  FROM final_customer_list a
  LEFT JOIN customers cus
  ON a.mobile = cus.mobile
  
  LEFT JOIN orders o
  ON cus.id = o.customer_id
  AND o.status = 4
  
  WHERE 
  a.mobile is not NULL
  GROUP BY 1,2,3,4,5,6,7
  
)
,house_shifting_keyword AS (
  SELECT DISTINCT ON (a.customer_id)
  
  a.customer_id,
  a.last_order_date,
  o.order_id,
  CASE WHEN o.to_address ILIKE '%Apartment%' OR 
  o.to_address ILIKE '%Housing%' OR 
  o.to_address ILIKE '%Sociecty%' OR 
  o.to_address ILIKE '%Tower%' OR 
  o.to_address ILIKE '%Block%' OR 
  o.to_address ILIKE '%Prestige%' or
  o.to_address ILIKE '%Brigade%' or
  o.to_address ILIKE '%Sobha%' or
  o.to_address ILIKE '%Godrej%' or
  o.to_address ILIKE '%Mahindra%' or
  o.to_address ILIKE '%Birla%' or
  o.to_address ILIKE '%Ark%' or
  o.to_address ILIKE '%Countryside%' or
  o.to_address ILIKE '%Lodha%' or
  o.to_address ILIKE '%Runwal%' or
  o.to_address ILIKE '%Kalpataru%' or
  o.to_address ILIKE '%Dosti%' or
  o.to_address ILIKE '%Wadhwa%' or
  o.to_address ILIKE '%Piramal%' or
  o.to_address ILIKE '%Jain%' or
  o.to_address ILIKE '%Marg%' or
  o.to_address ILIKE '%Unitech%' or
  o.to_address ILIKE '%DLF%' or
  o.to_address ILIKE '%Raheja%' or
  o.to_address ILIKE '%DGR%' or
  o.to_address ILIKE '%Adarsh%' or
  o.to_address ILIKE '%Ashiana%' or
  o.to_address ILIKE '%Parsvnath%' or
  o.to_address ILIKE '%Nagpal%' or
  o.to_address ILIKE '%Fortune%' or
  o.to_address ILIKE '%Ramky%' or
  o.to_address ILIKE '%Salapuria%' or
  o.to_address ILIKE '%Sri Aditya%' or
  o.to_address ILIKE '%Jayabheri%' or
  o.to_address ILIKE '%Manbhum%' or
  o.to_address ILIKE '%Rustomjee%' or
  o.from_address ILIKE '%Apartment%' OR o.from_address ILIKE '%Housing%' OR
  o.from_address ILIKE '%Sociecty%' OR 
  o.from_address ILIKE '%Tower%' OR 
  o.from_address ILIKE '%Block%' OR 
  o.from_address  ILIKE '%Prestige%' or
  o.from_address  ILIKE '%Brigade%' or
  o.from_address  ILIKE '%Sobha%' or
  o.from_address  ILIKE '%Godrej%' or
  o.from_address  ILIKE '%Mahindra%' or
  o.from_address  ILIKE '%Birla%' or
  o.from_address  ILIKE '%Ark%' or
  o.from_address  ILIKE '%Countryside%' or
  o.from_address  ILIKE '%Lodha%' or
  o.from_address  ILIKE '%Runwal%' or
  o.from_address  ILIKE '%Kalpataru%' or
  o.from_address  ILIKE '%Dosti%' or
  o.from_address  ILIKE '%Wadhwa%' or
  o.from_address  ILIKE '%Piramal%' or
  o.from_address  ILIKE '%Jain%' or
  o.from_address  ILIKE '%Marg%' or
  o.from_address  ILIKE '%Unitech%' or
  o.from_address  ILIKE '%DLF%' or
  o.from_address  ILIKE '%Raheja%' or
  o.from_address  ILIKE '%DGR%' or
  o.from_address  ILIKE '%Adarsh%' or
  o.from_address  ILIKE '%Ashiana%' or
  o.from_address  ILIKE '%Parsvnath%' or
  o.from_address  ILIKE '%Nagpal%' or
  o.from_address  ILIKE '%Fortune%' or
  o.from_address  ILIKE '%Ramky%' or
  o.from_address  ILIKE '%Salapuria%' or
  o.from_address  ILIKE '%Sri Aditya%' or
  o.from_address  ILIKE '%Jayabheri%' or
  o.from_address  ILIKE '%Manbhum%' or
  o.from_address  ILIKE '%Rustomjee%' 
  THEN 'house_shifting_keyword_trip'
  ELSE 'other_keyword_used' END AS trip_keyword_flag
  
  
  FROM customer_order_data a
  LEFT JOIN completed_spot_orders_mv o
  ON a.customer_id = o.customer_id
  AND a.last_order_date = o.order_date
  
  WHERE a.last_order_date IS NOT NULL 
  
  ORDER BY a.customer_id,pickup_time 
)
,custs AS (
  SELECT DISTINCT customer_id FROM customer_order_data WHERE customer_id IS NOT NULL
)

,helper_category_attempts AS (
  SELECT 
  a.customer_id,
  COALESCE(COUNT(dil.id),0) AS helper_category_attempts
  
  FROM customer_order_data a
  LEFT JOIN demand_inventory_logging_v2 dil
  ON a.customer_id = dil.customer_id
  AND dil.vehicle_id IN (96,98,99)
  
  WHERE a.customer_id IS NOT NULL 
  GROUP BY 1
)

,vehicle_used AS(
  select customer_id, drop_location, vehicle_id vech from demand_inventory_logging_v2 d where id in
  (select b.c from 
   (select a.customer_id, max(id)c
     from customer_order_data a
     LEFT JOIN demand_inventory_logging_v2 dil
     ON a.customer_id = dil.customer_id
     WHERE a.customer_id IS NOT NULL 
     GROUP BY 1)b) and customer_id IS NOT NULL 
)

,customer_app_install_data AS (
  SELECT *
    FROM dblink(
      'host=commons-prod-redshift.porter.in port=5439 user=anant password=Resfeber123 dbname=porter',
      format('SELECT 
     RIGHT(user_type_id, len(user_type_id) -9) AS customer_id,
     application_name,
     CASE WHEN application_name IN (''Packers & Movers by NoBroker'', ''LYNK'') THEN ''competitor''
          WHEN application_name IN (''Netflix'',''LinkedIn'',''Quora'',''Spotify'',''CRED'') THEN ''luxury''
          WHEN application_name IN (''Swiggy'', ''Ola'',''Uber'',''Zomato'') THEN ''daily convenience''
          WHEN application_name IN (''Flipkart'', ''Amazon'',''Myntra'',''Curefit'',''Jiomart'',''Snapdeal'') THEN ''E-Commerce'' 
          WHEN application_name IN (''ADDA - The Apartment Super App'',''MyGate'',''JioGate'',''NoBrokerHOOD'') THEN ''house_shifting''
          END AS app_type

FROM alfred.user_application_logs 
WHERE RIGHT(user_type_id, len(user_type_id) -9)  IN %s
AND  application_name IN (''Packers & Movers by NoBroker'',''Netflix'',''LinkedIn'',''Quora'',''Spotify'' ,''CRED'', ''LYNK'',
                            ''Swiggy'', ''Ola'',''Uber'',''Zomato'',''Flipkart'', ''Amazon'',''Myntra'',''ADDA - The Apartment Super App'',''MyGate'',''JioGate'',''NoBrokerHOOD'',''Curefit'',''Jiomart'',''Snapdeal'') 
AND is_installed',
             (SELECT '(''' || string_agg(DISTINCT customer_id::text, ''',''') || ''')' FROM custs)      
      )
    ) AS T (
      customer_id INT ,
      application_name VARCHAR ,
      app_type VARCHAR 
    )
)

,customer_app_installs AS (
  SELECT 
  customer_id,
  ARRAY_AGG (application_name) AS app_installed
  
  FROM dblink(
    'host=commons-prod-redshift.porter.in port=5439 user=anant password=Resfeber123 dbname=porter',
    format('SELECT 
     RIGHT(user_type_id, len(user_type_id) -9) AS customer_id,
     application_name
        
FROM alfred.user_application_logs 
WHERE RIGHT(user_type_id, len(user_type_id) -9)  IN %s
AND  application_name IN (''Packers & Movers by NoBroker'',''Netflix'',''LinkedIn'',''Quora'',''Spotify'' ,''CRED'', ''LYNK'',
                            ''Swiggy'', ''Ola'',''Uber'',''Zomato'',''Flipkart'', ''Amazon'',''Myntra'', ''ADDA - The Apartment Super App'',''MyGate'',''JioGate'',''NoBrokerHOOD'',''Curefit'',''Jiomart'',''Snapdeal'') 
AND is_installed',
           (SELECT '(''' || string_agg(DISTINCT customer_id::text, ''',''') || ''')' FROM custs)      
    )
  ) AS T (
    customer_id INT ,
    application_name VARCHAR 
  )
  GROUP BY 1
)
,app_totals AS (
  SELECT 
  customer_id, 
  COALESCE(COUNT (a.*) FILTER (WHERE app_type = 'competitor'),0) AS competitor_apps_installed,
  COALESCE(COUNT (a.*) FILTER (WHERE app_type = 'luxury'),0) AS luxury_apps_installed,
  COALESCE(COUNT (a.*) FILTER (WHERE app_type = 'daily convenience'),0) AS daily_convenience_apps_installed,
  COALESCE(COUNT (a.*) FILTER (WHERE app_type = 'E-Commerce'),0) AS ECommerce_apps_installed,
  COALESCE(COUNT (a.*) FILTER (WHERE app_type = 'house_shifting'),0) AS house_shifting_apps_installed
  
  FROM customer_app_install_data A
  
  GROUP BY 1
)
,customer_device_data AS (
  SELECT *
    FROM dblink(
      'host=commons-prod-redshift.porter.in port=5439 user=anant password=Resfeber123 dbname=porter',
      format('
SELECT 
    customer_id,
    device_model,
    device_platform_name

FROM 
    (    
SELECT 
    customer_id,
    device_model,
    device_platform_name,    
    ROW_NUMBER () OVER (PARTITION by customer_id ORDER BY event_timestamp desc) AS record
FROM awsma.customer_app_events
WHERE customer_id IN %s
/*AND event_timestamp > CURRENT_DATE - INTERVAL ''5 days''*/
ORDER BY event_timestamp DESC
    ) AS A
WHERE record = 1',
             (SELECT '(''' || string_agg(DISTINCT customer_id::text, ''',''') || ''')' FROM custs)
      )
    ) AS T (
      
      customer_id INT ,
      device_model varchar,
      device_platform_name VARCHAR 
      
    )
)


,device_signature AS (
  SELECT 
  a.customer_id,
  device_model,
  CASE WHEN device_platform_name = 'iOS' OR device_model IN  ('SM-G9%','Pixel%','ONEPLUS%','SM-N9%')
  THEN 1 
  ELSE 0 END AS mobile_device_type
  FROM customer_device_data a
  ORDER BY a.customer_id
)
,final_data AS (
  SELECT 
  geo_regions.name as city,
  a.customer_id,
  a.mobile,
  a.app_install_date,
  a.lead_channel_attribution,
  a.last_order_date,
  a.gap_between_app_install_first_order,
  a.total_orders,
  keywrd.order_id,
  device_model,
  mobile_device_type,
  keywrd.trip_keyword_flag,
  helper.helper_category_attempts,
  COALESCE(app.competitor_apps_installed,0) AS competitor_apps_installed,
  COALESCE (app.luxury_apps_installed,0) AS luxury_apps_installed,
  COALESCE (app.daily_convenience_apps_installed,0) AS daily_convenience_apps_installed,
  COALESCE (app.ECommerce_apps_installed,0) AS ECommerce_apps_installed,
  COALESCE (app.house_shifting_apps_installed,0) AS house_shifting_apps_installed,
  apps.app_installed,
  v.vech, v.drop_location
  
  FROM customer_order_data a
  LEFT JOIN helper_category_attempts helper
  ON a.customer_id = helper.customer_id
  
  LEFT JOIN app_totals app
  ON a.customer_id = app.customer_id
  
  LEFT JOIN house_shifting_keyword keywrd
  ON a.customer_id = keywrd.customer_id
  
  LEFT JOIN device_signature device
  ON a.customer_id = device.customer_id
  
  LEFT JOIN customer_app_installs apps
  ON a.customer_id = apps.customer_id
  
  LEFT JOIN geo_regions 
  ON a.geo_region_id = geo_regions.id
  
  LEFT JOIN vehicle_used v 
  ON a.customer_id = v.customer_id
  
  
  ORDER BY app_install_date,customer_id)

,final_data_with_flags AS (
  SELECT *,
  CASE WHEN luxury_apps_installed > 0 THEN 1 ELSE 0 END AS luxury_apps_flag,
  CASE WHEN competitor_apps_installed > 0 THEN 1 ELSE 0 END AS competitor_apps_flag,
  CASE WHEN daily_convenience_apps_installed > 0 THEN 1 ELSE 0 END AS daily_convenience_apps_flag,
  CASE WHEN ECommerce_apps_installed > 0 THEN 1 ELSE 0 END AS ECommerce_apps_flag,
  CASE WHEN house_shifting_apps_installed > 0 THEN 1 ELSE 0 END AS house_shifting_apps_flag,
  CASE WHEN helper_category_attempts > 0 THEN 1 ELSE 0 END AS helper_flag,
  CASE WHEN total_orders > 0 THEN 0 ELSE 1 END AS LTO_0_flag,
  CASE WHEN trip_keyword_flag ='house_shifting_keyword_trip' THEN 1 ELSE 0 END AS trip_flag
  FROM final_data)

SELECT *,
(COALESCE(lead_channel_attribution,0) * 1) + (COALESCE(competitor_apps_flag,0) * 3) + (COALESCE(luxury_apps_flag,0) * 5) + (COALESCE(ECommerce_apps_flag,0) * 3) + 
(COALESCE(daily_convenience_apps_flag,0) * 3) + (COALESCE(house_shifting_apps_flag,0) * 5) + (COALESCE(helper_flag,0) * 1) + 
(COALESCE(LTO_0_flag,0) * 1) + (COALESCE(mobile_device_type,0) * 5) AS Weighted_score, CURRENT_TIMESTAMP + '5.5 hours'::INTERVAL AS created_at
FROM final_data_with_flags
")

query_result = run_query("oms",query)
query_result$mobile = as.numeric(query_result$mobile)

result = unique(merge(x = query_result, y = data_filtered, by.x = "mobile",by.y = "Mobile", all.x = TRUE))
result$source_weights = ifelse(result$Source == "app"|result$Source == "website" | result$Source == "CC Leads" | result$Source == "app_banner",5,
                               ifelse(result$Source == "Missed call" | result$Source == "Whatsapp", 3,
                                      1))

result$trip_flag_gs= ifelse(result$Pickup.Address %ilike% 'Housing'| result$Pickup.Address %ilike% 'Apartment' |
                              result$Pickup.Address	%ilike%	'Sociecty'	|
                              result$Pickup.Address	%ilike%	'Tower'	|
                              result$Pickup.Address	%ilike%	'Block'	|
                              result$Pickup.Address	%ilike%	'Prestige'	|
                              result$Pickup.Address	%ilike%	'Brigade'	|
                              result$Pickup.Address	%ilike%	'Sobha'	|
                              result$Pickup.Address	%ilike%	'Godrej'	|
                              result$Pickup.Address	%ilike%	'Mahindra'	|
                              result$Pickup.Address	%ilike%	'Birla'	|
                              result$Pickup.Address	%ilike%	'Ark'	|
                              result$Pickup.Address	%ilike%	'Countryside'	|
                              result$Pickup.Address	%ilike%	'Lodha'	|
                              result$Pickup.Address	%ilike%	'Runwal'	|
                              result$Pickup.Address	%ilike%	'Kalpataru'	|
                              result$Pickup.Address	%ilike%	'Dosti'	|
                              result$Pickup.Address	%ilike%	'Wadhwa'	|
                              result$Pickup.Address	%ilike%	'Piramal'	|
                              result$Pickup.Address	%ilike%	'Jain'	|
                              result$Pickup.Address	%ilike%	'Marg'	|
                              result$Pickup.Address	%ilike%	'Unitech'	|
                              result$Pickup.Address	%ilike%	'DLF'	|
                              result$Pickup.Address	%ilike%	'Raheja'	|
                              result$Pickup.Address	%ilike%	'DGR'	|
                              result$Pickup.Address	%ilike%	'Adarsh'	|
                              result$Pickup.Address	%ilike%	'Ashiana'	|
                              result$Pickup.Address	%ilike%	'Parsvnath'	|
                              result$Pickup.Address	%ilike%	'Nagpal'	|
                              result$Pickup.Address	%ilike%	'Fortune'	|
                              result$Pickup.Address	%ilike%	'Ramky'	|
                              result$Pickup.Address	%ilike%	'Salapuria'	|
                              result$Pickup.Address	%ilike%	'Sri Aditya'	|
                              result$Pickup.Address	%ilike%	'Jayabheri'	|
                              result$Pickup.Address	%ilike%	'Manbhum'	|
                              result$Pickup.Address	%ilike%	'Rustomjee' |
                              result$Drop.Address	%ilike%	'Housing'|	result$Drop.Address	%ilike%	'Apartment'	|
                              result$Drop.Address	%ilike%	'Sociecty'	|		
                              result$Drop.Address	%ilike%	'Tower'	|			
                              result$Drop.Address	%ilike%	'Block'	|		
                              result$Drop.Address	%ilike%	'Prestige'	|			
                              result$Drop.Address	%ilike%	'Brigade'	|		
                              result$Drop.Address	%ilike%	'Sobha'	|			
                              result$Drop.Address	%ilike%	'Godrej'	|		
                              result$Drop.Address	%ilike%	'Mahindra'	|			
                              result$Drop.Address	%ilike%	'Birla'	|		
                              result$Drop.Address	%ilike%	'Ark'	|			
                              result$Drop.Address	%ilike%	'Countryside'	|		
                              result$Drop.Address	%ilike%	'Lodha'	|			
                              result$Drop.Address	%ilike%	'Runwal'	|		
                              result$Drop.Address	%ilike%	'Kalpataru'	|			
                              result$Drop.Address	%ilike%	'Dosti'	|		
                              result$Drop.Address	%ilike%	'Wadhwa'	|			
                              result$Drop.Address	%ilike%	'Piramal'	|		
                              result$Drop.Address	%ilike%	'Jain'	|			
                              result$Drop.Address	%ilike%	'Marg'	|		
                              result$Drop.Address	%ilike%	'Unitech'	|			
                              result$Drop.Address	%ilike%	'DLF'	|		
                              result$Drop.Address	%ilike%	'Raheja'	|			
                              result$Drop.Address	%ilike%	'DGR'	|		
                              result$Drop.Address	%ilike%	'Adarsh'	|			
                              result$Drop.Address	%ilike%	'Ashiana'	|		
                              result$Drop.Address	%ilike%	'Parsvnath'	|			
                              result$Drop.Address	%ilike%	'Nagpal'	|		
                              result$Drop.Address	%ilike%	'Fortune'	|			
                              result$Drop.Address	%ilike%	'Ramky'	|		
                              result$Drop.Address	%ilike%	'Salapuria'	|			
                              result$Drop.Address	%ilike%	'Sri	Aditya'	|	
                              result$Drop.Address	%ilike%	'Jayabheri'	|			
                              result$Drop.Address	%ilike%	'Manbhum'	|		
                              result$Drop.Address	%ilike%	'Rustomjee'	,1 ,0)

result$trip_flag_gs = ifelse(result$trip_flag_gs == 0, result$trip_flag, result$trip_flag_gs)
result = result %>% rename(society.flag = trip_flag_gs)

result$remaining_days_flag = ifelse(result$days_remaining_for_shifting== 0, 5,
                                    ifelse(result$days_remaining_for_shifting == 1, 3,
                                           ifelse(result$days_remaining_for_shifting >=2, 1, 0)))

result$remaining_days_flag[is.na(result$remaining_days_flag)] = 0


# leads_data_1 = result %>% group_by (mobile,Source) %>% summarise(count = n())
# leads_data_1 = leads_data_1 %>% group_by(mobile) %>% summarise(count_source = n())
# 
# leads_data_2 = result %>% group_by (mobile) %>% summarise(count_mobile_number = n())
# 
# result = result %>% left_join(leads_data_2)
# result = result %>% left_join(leads_data_1)

result$source_count_weights = ifelse(result$Count.of.order.interests >= 10, 10,
                                     ifelse( (result$Count.of.order.interests ==2 | result$Count.of.order.interests ==3) ,30,
                                             ifelse( (result$Count.of.order.interests >= 4 | result$Count.of.order.interests <= 6) ,50,
                                                     ifelse( (result$Count.of.order.interests >=4 | result$Count.of.order.interests <= 9) ,50,0))))

result$weighted_score = result$weighted_score + (result$society.flag*3) + result$source_count_weights
result$weighted_score = result$weighted_score + result$source_weights + result$remaining_days_flag
result_final = select(result,c('mobile',	'city',	'customer_id',	'app_install_date',	'lead_channel_attribution',	'last_order_date',	
                               'total_orders',	'device_model',	'trip_keyword_flag',	'helper_category_attempts',
                               'competitor_apps_installed',	'luxury_apps_installed',	'daily_convenience_apps_installed',
                               'ecommerce_apps_installed',	'house_shifting_apps_installed',	'app_installed','Source',
                               'society.flag', 'days_remaining_for_shifting',	'weighted_score','created_at'))

result_final = result_final[order(result_final$weighted_score, decreasing = TRUE),]
result_final$app_installed = gsub('"','',result_final$app_installed)

unscored_leads = unique(merge(x = data_filtered, y = query_result, by.x = "Mobile",by.y = "mobile", all.x = TRUE))
unscored_leads = unscored_leads %>% filter(is.na(as.character(created_at)))

unscored_leads$trip_flag_gs= ifelse(unscored_leads$Pickup.Address %ilike% 'Housing'| unscored_leads$Pickup.Address %ilike% 'Apartment' |
                                      unscored_leads$Pickup.Address	%ilike%	'Sociecty'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Tower'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Block'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Prestige'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Brigade'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Sobha'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Godrej'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Mahindra'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Birla'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Ark'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Countryside'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Lodha'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Runwal'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Kalpataru'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Dosti'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Wadhwa'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Piramal'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Jain'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Marg'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Unitech'	|
                                      unscored_leads$Pickup.Address	%ilike%	'DLF'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Raheja'	|
                                      unscored_leads$Pickup.Address	%ilike%	'DGR'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Adarsh'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Ashiana'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Parsvnath'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Nagpal'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Fortune'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Ramky'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Salapuria'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Sri Aditya'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Jayabheri'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Manbhum'	|
                                      unscored_leads$Pickup.Address	%ilike%	'Rustomjee' |
                                      unscored_leads$Drop.Address	%ilike%	'Housing'|	unscored_leads$Drop.Address	%ilike%	'Apartment'	|
                                      unscored_leads$Drop.Address	%ilike%	'Sociecty'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Tower'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Block'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Prestige'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Brigade'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Sobha'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Godrej'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Mahindra'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Birla'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Ark'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Countryside'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Lodha'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Runwal'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Kalpataru'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Dosti'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Wadhwa'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Piramal'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Jain'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Marg'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Unitech'	|			
                                      unscored_leads$Drop.Address	%ilike%	'DLF'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Raheja'	|			
                                      unscored_leads$Drop.Address	%ilike%	'DGR'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Adarsh'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Ashiana'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Parsvnath'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Nagpal'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Fortune'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Ramky'	|		
                                      unscored_leads$Drop.Address	%ilike%	'Salapuria'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Sri	Aditya'	|	
                                      unscored_leads$Drop.Address	%ilike%	'Jayabheri'	|			
                                      unscored_leads$Drop.Address	%ilike%	'Manbhum'	|		
                                      unscored_leads$Drop.Address %ilike%	'Rustomjee'	,1 ,0)

unscored_leads$source_weights = ifelse(unscored_leads$Source == "app"|unscored_leads$Source == "website"||result$Source == "CC Leads" | unscored_leads$Source == "app_banner",5,
                                       ifelse(unscored_leads$Source == "Missed call" | unscored_leads$Source == "Whatsapp", 3,
                                              1))
unscored_leads = unscored_leads %>% rename(society.flag = trip_flag_gs)

unscored_leads$remaining_days_flag = ifelse(unscored_leads$days_remaining_for_shifting== 0, 5,
                                            ifelse(unscored_leads$days_remaining_for_shifting == 1, 3,
                                                   ifelse(unscored_leads$days_remaining_for_shifting >=2, 1, 0)))
unscored_leads$remaining_days_flag[is.na(unscored_leads$remaining_days_flag)] = 0

# leads_data_1 = unscored_leads %>% group_by (Mobile.Number,Source) %>% summarise(count = n())
# leads_data_1 = leads_data_1 %>% group_by(Mobile.Number) %>% summarise(count_source = n())
# 
# leads_data_2 = unscored_leads %>% group_by (Mobile.Number) %>% summarise(count_mobile_number = n())
# 
# unscored_leads = unscored_leads %>% left_join(leads_data_2)
# unscored_leads = unscored_leads %>% left_join(leads_data_1)

unscored_leads$source_count_weights = ifelse( unscored_leads$Count.of.order.interests >= 10, 10,
                                              ifelse( (unscored_leads$Count.of.order.interests ==2 | unscored_leads$Count.of.order.interests ==3) ,30,
                                                      ifelse((unscored_leads$Count.of.order.interests >= 4 | unscored_leads$Count.of.order.interests <= 6) ,50,
                                                             ifelse( (unscored_leads$Count.of.order.interests >=4 | unscored_leads$Count.of.order.interests <= 9) ,50,0))))



unscored_leads$weighted_score = unscored_leads$source_weights + (unscored_leads$society.flag) * 3 + unscored_leads$remaining_days_flag + unscored_leads$source_count_weights

unscored_leads = select(unscored_leads,c("Mobile" ,"Source","Pickup.Address","Drop.Address","society.flag","days_remaining_for_shifting","weighted_score"))
unscored_leads$created_at = timestamp()

unscored_leads = unscored_leads[order(unscored_leads$weighted_score, decreasing = TRUE),]

ss =gs4_create(name = "Sales force House Shifting Leads Scores", sheets = c("Active leads scores","Historical scores","Remaining leads scores","Remaining leads Historical scores"))

result_final$days_remaining_for_shifting = as.character(result_final$days_remaining_for_shifting)
sheet_write(result_final,ss="1cVgo7CzlaSY3OQ1w3fEsj3T0avtaT8RTgIGJ24FZGDI", sheet = "Active leads scores" )
#sheet_append(result_final,ss="1fuqSmrm40joyuQ87Y28MumCBSoLIr2CwzgTDuoSJZec",sheet = "Historical scores" )
write.table(result_final,"C:/Users/Public/Salesforce Active leads Historical scores.csv", append = TRUE,col.names = FALSE,row.names=FALSE, sep = ",",quote = TRUE)

unscored_leads$days_remaining_for_shifting = as.character(unscored_leads$days_remaining_for_shifting)
sheet_write(unscored_leads,ss="1cVgo7CzlaSY3OQ1w3fEsj3T0avtaT8RTgIGJ24FZGDI", sheet = "Remaining leads scores" )
#sheet_append(unscored_leads,ss= "1fuqSmrm40joyuQ87Y28MumCBSoLIr2CwzgTDuoSJZec",sheet = "Remaining leads Historical scores" )
write.table(unscored_leads,"C:/Users/Public/Salesforce Remaining leads Historical scores.csv", append = TRUE,col.names = FALSE,row.names=FALSE, sep = ",",quote = TRUE)

