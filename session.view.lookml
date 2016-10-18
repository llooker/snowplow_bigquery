# View for the session table (PDT). This table uses session_prep during its build process.
# Authors: Erin Franz (erin@looker.com), Kevin Marr (marr@looker.com)
- view: sessions_pre_grouping
  derived_table:
    sql: |
       SELECT
        CONCAT(domain_userid, cast(domain_sessionidx as string)) as session_pkey
          , domain_userid
          , domain_sessionidx
          , min(collector_tstamp) as start_at
          , max(collector_tstamp) as last_event_at
          , min(dvce_tstamp) AS dvce_min_tstamp
          , max(dvce_tstamp) AS dvce_max_tstamp
          , count(1) as number_of_events
          , count(distinct(floor(dvce_tstamp/30)))/2 AS time_engaged_with_minutes
        from snowplow.event
        where domain_userid is not null
          and domain_sessionidx is not null
          and domain_userid != ''
          and dvce_tstamp IS NOT NULL
          and dvce_tstamp  > timestamp('2000-01-01') -- Prevent SQL errors
          and dvce_tstamp < timestamp('2030-01-01') -- Prevent SQL errors
        group by 1, 2, 3
        
- view: sessions_pre_window
  derived_table:
    sql: |
      select CONCAT(domain_userid, cast(domain_sessionidx as string)) as session_pkey
          -- -- geo fields
          , first_value(tr_country) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as tr_country 
          -- , first_value(geo_region) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as geo_region
          , first_value(tr_city) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as geo_city
          
          -- -- landing page fields
          , first_value(page_urlhost) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as landing_page_urlhost
          , first_value(page_urlpath) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as landing_page_urlpath
          
          -- exit page fields
          , last_value(page_urlhost ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as exit_page_urlhost
          , last_value(page_urlpath ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as exit_page_urlpath
          
          -- browser fields
          , first_value(br_name ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_name
          , first_value(br_family ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_family
          , first_value(br_version ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_version
          , first_value(br_type ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_type
          , first_value(br_renderengine ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_renderengine
          , first_value(br_lang ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_lang
          , first_value(br_features_director ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_director
          , first_value(br_features_flash ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_flash
          , first_value(br_features_gears ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_gears
          , first_value(br_features_java ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_java
          , first_value(br_features_pdf ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_pdf
          , first_value(br_features_quicktime ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_quicktime
          , first_value(br_features_realplayer ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_realplayer
          , first_value(br_features_silverlight ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_silverlight
          , first_value(br_features_windowsmedia ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_features_windowsmedia
          , first_value(br_cookies ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as br_cookies
          
          -- os fields
          , first_value(os_name ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as os_name
          , first_value(os_family ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as os_family
          , first_value(os_manufacturer ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as os_manufacturer
          , first_value(os_timezone ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as os_timezone
          
          -- device fields
          , first_value(dvce_type ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as dvce_type
          , first_value(dvce_ismobile ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as dvce_ismobile
          -- , first_value(dvce_screenwidth ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as dvce_screenwidth
          , first_value(dvce_screenheight ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as dvce_screenheight
          
          -- marketing fields
          , first_value((CASE WHEN mkt_source = '' OR refr_medium = 'internal' THEN NULL ELSE mkt_source END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as mkt_source
          , first_value((CASE WHEN mkt_medium = '' OR refr_medium = 'internal' THEN NULL ELSE mkt_medium END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as mkt_medium
          , first_value((CASE WHEN mkt_campaign = '' OR refr_medium = 'internal' THEN NULL ELSE mkt_campaign END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as mkt_campaign
          , first_value((CASE WHEN mkt_term = '' OR refr_medium = 'internal' THEN NULL ELSE mkt_term END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as mkt_term
          , first_value((CASE WHEN mkt_content = '' OR refr_medium = 'internal' THEN NULL ELSE mkt_content END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as mkt_content
          
          -- referrer fields
          , first_value((CASE WHEN refr_source = '' OR refr_medium = 'internal' THEN NULL ELSE refr_source END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as refr_source
          , first_value((CASE WHEN refr_medium = '' OR refr_medium = 'internal' THEN NULL ELSE refr_medium END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as refr_medium
          , first_value((CASE WHEN refr_term = '' OR refr_medium = 'internal' THEN NULL ELSE refr_term END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as refr_term
          , first_value((CASE WHEN refr_urlhost = '' OR refr_medium = 'internal' THEN NULL ELSE refr_urlhost END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as refr_urlhost
          , first_value((CASE WHEN refr_urlpath = '' OR refr_medium = 'internal' THEN NULL ELSE refr_urlpath END) ) over (partition by domain_userid, domain_sessionidx order by dvce_tstamp rows between unbounded preceding and unbounded following) as refr_urlpath
          
        from snowplow.event
        where domain_userid is not null
          and domain_sessionidx is not null
          and domain_userid != ''
          and dvce_tstamp IS NOT NULL
          and dvce_tstamp  > timestamp('2000-01-01') -- Prevent SQL errors
          and dvce_tstamp < timestamp('2030-01-01') -- Prevent SQL errors
          
- view: session
  derived_table:
  
    # Rebuilds at midnight database time. Adjust as needed.
    #sql_trigger_value: select current_date

    #indexes: [start_at, domain_userid, domain_sessionidx]
    #distkey: domain_userid
  
    sql: |
      select a.domain_userid as domain_userid
        , a.session_pkey as session_pkey
        , a.domain_sessionidx as domain_sessionidx
        , a.start_at as start_at
        , least(date_add(a.last_event_at, 1, 'minute'), a.start_at) as end_at
        , a.number_of_events as number_of_events
        , a.time_engaged_with_minutes as time_engaged_with_minutes
        , b.*
      from ${sessions_pre_grouping.SQL_TABLE_NAME} as a
        inner join ${sessions_pre_window.SQL_TABLE_NAME} as b
          on a.session_pkey = b.session_pkey


  fields:


# Basic Session Fields #

  - dimension: session_pkey
    primary_key: true
    hidden: true
    sql: ${TABLE}.session_pkey

  - dimension: domain_userid
    sql: ${TABLE}.domain_userid

  - dimension: domain_session_index
    type: number
    sql: ${TABLE}.domain_sessionidx

  - dimension_group: start
    type: time
    timeframes: [raw, time, date, week, month]
    sql: ${TABLE}.start_at

  - dimension_group: end
    type: time
    timeframes: [raw, time, date, week, month]
    datatype: epoch
    sql: ${TABLE}.end_at/1000000

  - dimension: number_of_events
    type: number
    sql: ${TABLE}.number_of_events

  - dimension: duration_minutes
    type: number
    value_format_name: decimal_2
    sql: DATEDIFF(MINUTE(${start_raw}), MINUTE((SEC_TO_TIMESTAMP(${end_raw}/1000000))))  
  
  - dimension: time_engaged_with_minutes
    sql: ${TABLE}.time_engaged_with_minutes

  - dimension: bounced
    type: yesno
    sql: ${number_of_events} = 1

  - dimension: is_first_session
    type: yesno
    sql: ${domain_session_index} = 1
  
  - dimension: new_vs_returning_visitor
    sql_case:
      new: ${domain_session_index} = 1
      returning: ${domain_session_index} > 1
      else: unknown
      
  - measure: count
    type: count
    drill_fields: count_drill*
  
  - measure: bounced_session_count
    type: count
    filter: 
      bounced: yes
    drill_fields: count_drill*
  
  - measure: bounce_rate
    type: number
    sql: CAST(${bounced_session_count} as float)/(CASE WHEN ${count} = 0 THEN NULL ELSE ${count} END)

  - measure: average_number_of_events
    type: average
    value_format: '0.00'
    sql: ${number_of_events}

  - measure: average_duration_minutes
    type: average
    value_format: '0.00'
    sql: ${duration_minutes}

  - measure: sessions_per_user
    type: number
    sql: CAST(${count} as float)/(CASE WHEN ${count} = 0 THEN NULL ELSE ${count} END)
    value_format: '0.00'

  - measure: user.count
    type: count_distinct
    sql: ${domain_userid}
    drill_fields: [user.domain_userid, user.id, user.ip_address, location.city, location.country]
  
  - measure: average_time_engaged_minutes
    type: average
    sql: ${time_engaged_with_minutes}
  
  - measure: sessions_from_new_visitors_count
    type: count
    filters:
      domain_session_index: 1
    drill_fields: count_drill*
  
  - measure: sessions_from_returning_visitors_count
    type: count
    filter: 
      domain_session_index: '>1'
    drill_fields: count_drill*
  
  - measure: percent_new_visitor_sessions
    type: number
    value_format: '#.00%'
    sql: CAST(${sessions_from_new_visitors_count} as float)/(CASE WHEN ${count} = 0 THEN NULL ELSE ${count} END)

  - measure: percent_returning_visitor_sessions
    type: number
    value_format: '#.00%'
    sql: CAST(${sessions_from_returning_visitors_count} as float)/(CASE WHEN ${count} = 0 THEN NULL ELSE ${count} END)
 
 
# Geo Fields #
  
  - dimension: geography_country
    sql: ${TABLE}.geo_country

  - dimension: geography_region
    sql: ${TABLE}.geo_region

  - dimension: geography_city
    sql: ${TABLE}.geo_city


# Landing and Exit Pages #
  
  - dimension: landing_page_urlhost
    sql: ${TABLE}.landing_page_urlhost

  - dimension: landing_page_urlpath
    sql: ${TABLE}.landing_page_urlpath

  - dimension: exit_page_urlhost
    sql: ${TABLE}.exit_page_urlhost

  - dimension: exit_page_urlpath
    sql: ${TABLE}.exit_page_urlpath


# Browser Fields #
  
  - dimension: browser
    sql: ${TABLE}.br_name
  
  - dimension: browser_family
    sql: ${TABLE}.br_family

  - dimension: browser_version
    sql: ${TABLE}.br_version
    
  - dimension: browser_type
    sql: ${TABLE}.br_type
    
  - dimension: browser_renderengine
    sql: ${TABLE}.br_renderengine
    
  - dimension: browser_language
    sql: ${TABLE}.br_lang
    
  - dimension: browser_has_director_plugin
    type: yesno
    sql: ${TABLE}.br_features_director
    
  - dimension: browser_has_flash_plugin
    type: yesno
    sql: ${TABLE}.br_features_flash
    
  - dimension: browser_has_gears_plugin
    type: yesno
    sql: ${TABLE}.br_features_gears
    
  - dimension: browser_has_java_plugin
    type: yesno
    sql: ${TABLE}.br_features_java
    
  - dimension: browser_has_pdf_plugin
    type: yesno
    sql: ${TABLE}.br_features_pdf
    
  - dimension: browser_has_quicktime_plugin
    type: yesno
    sql: ${TABLE}.br_features_quicktime
    
  - dimension: browser_has_realplayer_plugin
    type: yesno
    sql: ${TABLE}.br_features_realplayer
    
  - dimension: browser_has_silverlight_plugin
    type: yesno
    sql: ${TABLE}.br_features_silverlight
    
  - dimension: browser_has_windowsmedia_plugin
    type: yesno
    sql: ${TABLE}.br_features_windowsmedia
    
  - dimension: browser_supports_cookies
    type: yesno
    sql: ${TABLE}.br_cookies
  
  
# OS Fields #
    
  - dimension: operating_system
    sql: ${TABLE}.os_name
    
  - dimension: operating_system_family
    sql: ${TABLE}.os_family
    
  - dimension: operating_system_manufacturer
    sql: ${TABLE}.os_manufacturer
    
    
# Device Fields #
    
  - dimension: device_type
    sql: ${TABLE}.dvce_type
    
  - dimension: device_is_mobile
    type: yesno
    sql: ${TABLE}.dvce_ismobile
    
  - dimension: device_screen_width
    sql: ${TABLE}.dvce_screenwidth
    
  - dimension: device_screen_height
    sql: ${TABLE}.dvce_screenheight
    

# Referrer Fields (All Acquisition Channels) #
    
  - dimension: referrer_medium
    sql_case:
      email: ${TABLE}.refr_medium = 'email'
      search: ${TABLE}.refr_medium = 'search'
      social: ${TABLE}.refr_medium = 'social'
      other_website: ${TABLE}.refr_medium = 'unknown'
      else: direct
    
  - dimension: referrer_source
    sql: ${TABLE}.refr_source
    
  - dimension: referrer_term
    sql: ${TABLE}.refr_term
    
  - dimension: referrer_url_host
    sql: ${TABLE}.refr_urlhost
  
  - dimension: referrer_url_path
    sql: ${TABLE}.refr_urlpath
    
    
# Marketing Fields (Paid Acquisition Channels)
    
  - dimension: campaign_medium
    sql: ${TABLE}.mkt_medium
  
  - dimension: campaign_source
    sql: ${TABLE}.mkt_source
  
  - dimension: campaign_term
    sql: ${TABLE}.mkt_term
  
  - dimension: campaign_name
    sql: ${TABLE}.mkt_campaign

  - dimension: campaign_content
    sql: ${TABLE}.mkt_content


# Sets #

  sets:
    count_drill:
      - domain_userid
      - domain_sessionidx
      - start_at
      - end_at
      - duration_minutes
      - num_events
      
      