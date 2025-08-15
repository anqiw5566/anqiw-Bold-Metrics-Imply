CREATE OR REPLACE TABLE `unity-data-ads-core-prd.zone_bi_adhoc.sdk_metrics_v1_hourly_080914_agg` AS 

WITH base AS(
SELECT
m.* EXCEPT(sdk_version,source_game_id,placement_id,ad_type,mediation_name,mediation_version,country,iso,developer_id,developer_name,organization_id,core_organization,device_make,device_model,os_version,ett,reason_debug),
    SAFE_CAST(
              (CASE
                WHEN -- version below 4.10.0,
                  -- i.e. major version equal or less than 4
                  SAFE_CAST(SPLIT(sdk_version_name, '.')[SAFE_OFFSET(0)] AS INT64) <= 4
                  -- and minor version less than 10
                  AND SAFE_CAST(SPLIT(sdk_version_name, '.')[SAFE_OFFSET(1)] AS INT64) < 10
                THEN -- old numeric method (4 digits)
                  CONCAT(
                  -- Major Version
                  SPLIT(sdk_version_name, '.')[SAFE_OFFSET(0)],
                  -- Minor Version
                  SPLIT(sdk_version_name, '.')[SAFE_OFFSET(1)],
                  -- Patch Version
                  CONCAT(SPLIT(sdk_version_name, '.')[SAFE_OFFSET(2)], "0")
                )
                ELSE -- new numeric method (5+ digits)
                CONCAT(
                  -- Major Version
                  SPLIT(sdk_version_name, '.')[SAFE_OFFSET(0)],
                  -- Minor Version
                  (CASE WHEN LENGTH(SPLIT(sdk_version_name, '.')[SAFE_OFFSET(1)]) = 1 THEN CONCAT("0", SPLIT(sdk_version_name, '.')[SAFE_OFFSET(1)]) ELSE SPLIT(sdk_version_name, '.')[SAFE_OFFSET(1)]END),
                  -- Patch Version
                  (CASE WHEN LENGTH(SPLIT(sdk_version_name, '.')[SAFE_OFFSET(2)]) = 1 THEN CONCAT("0", SPLIT(sdk_version_name, '.')[SAFE_OFFSET(2)]) ELSE SPLIT(sdk_version_name, '.')[SAFE_OFFSET(2)]END)
                )
              END)
            AS INT64)  --- replace with sdk_version once upstream PR fixed ---
             sdk_version,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started', 'native_load_started', 'native_show_started'),source_game_id,
        IF(producer = "gwl", source_game_id, NULL)) AS source_game_id,   
    IF(producer = "gwv1" AND metric IN ('native_load_started', 'native_show_started'),placement_id,
        IF(producer = "gwl", placement_id, NULL)) AS placement_id,   
    IF(producer = "gwv1" AND metric IN ('native_load_started', 'native_show_started'),ad_type,
        IF(producer = "gwl", ad_type, NULL)) AS ad_type,   
    
    IF(producer = "gwv1" AND metric IN ('ad_viewer_load_complete'),campaign_type,  ----- upstream flow needs to add `ad_viewer_load_complete` metrics
        IF(producer = "gwl", campaign_type, NULL)) AS load_campaign_type,   
    IF(producer = "gwv1" AND metric IN ('ad_viewer_load_complete'),content_type,   ----- upstream flow needs to add `ad_viewer_load_complete` metrics
        IF(producer = "gwl", content_type, NULL)) AS load_content_type,
    
    IF(producer = "gwv1" AND metric IN ('ad_viewer_native_show_call', 'ad_viewer_campaign_start'),campaign_type,
        IF(producer = "gwl", campaign_type, NULL)) AS show_campaign_type,
    IF(producer = "gwv1" AND metric IN ('ad_viewer_native_show_call', 'ad_viewer_campaign_start'),content_type,
        IF(producer = "gwl", content_type, NULL)) AS show_content_type,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),mediation_name,
        IF(producer = "gwl", mediation_name, NULL)) AS mediation_name,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),mediation_version,
        IF(producer = "gwl", mediation_version, NULL)) AS mediation_version,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),producer,
        IF(producer = "gwl", producer, NULL)) AS is_bold,        
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),country,
        IF(producer = "gwl", country, NULL)) AS country,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),iso,
        IF(producer = "gwl", iso, NULL)) AS iso_country,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),developer_id,
        IF(producer = "gwl", developer_id, NULL)) AS developer_id,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),developer_name,
        IF(producer = "gwl", developer_name, NULL)) AS developer_name,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),organization_id,
        IF(producer = "gwl", organization_id, NULL)) AS organization_id,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),core_organization,
        IF(producer = "gwl", core_organization, NULL)) AS core_organization,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),device_make,
        IF(producer = "gwl", device_make, NULL)) AS device_make,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),device_model,
        IF(producer = "gwl", device_model, NULL)) AS device_model,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),os_version,
        IF(producer = "gwl", os_version, NULL)) AS os_version,
    IF(producer = "gwv1" AND metric IN ('native_initialization_started','native_load_started','native_show_started'),ett,
        IF(producer = "gwl", ett, NULL)) AS ett,

    CASE
      WHEN REGEXP_CONTAINS(reason_debug, r"-10(0[0-7]|09|1[7-9]|20)|-120[02]") THEN "Network error"
      WHEN REGEXP_CONTAINS(reason_debug, r"HTTP Response Error code: (?:-999|100|2\.|28\.|403\.|416\.|503\.|502\.|80\.|9\.|22\.)") THEN "Network error"
      WHEN REGEXP_CONTAINS(reason_debug, r"Code=4|Code=640") THEN "File error"
      WHEN reason_debug LIKE '%Gateway request failed %' THEN 'Gateway request failed'
      WHEN reason_debug LIKE '%Gateway was canceled %' THEN 'Gateway was canceled'
      WHEN reason_debug LIKE '%Timed out waiting for 20000 ms%' THEN 'Timeout'
      WHEN reason_debug LIKE '%Gateway request was canceled%' THEN 'Gateway was canceled'
      WHEN reason_debug LIKE '%GatewayException%' THEN 'GatewayException'
      WHEN reason_debug LIKE '%AndroidRuntimeException%' THEN 'AndroidRuntimeException'
      WHEN reason_debug LIKE '%SecurityException%' THEN 'SecurityException'
      WHEN reason_debug LIKE '%kotlinx.coroutines.channels.ClosedSendChannelException%' THEN 'ClosedSendChannelException'
      WHEN reason_debug LIKE '%NoClassDefFoundError%' THEN 'NoClassDefFoundError'
      WHEN reason_debug LIKE '%RuntimeException%' THEN 'RuntimeException'
      WHEN reason_debug LIKE '%java.lang.NullPointerException%' THEN 'java.lang.NullPointerException'
      WHEN reason_debug LIKE '%java.lang.IllegalStateException%' THEN 'java.lang.IllegalStateException'
      WHEN reason_debug LIKE '%java.lang.NoSuchMethodError%' THEN 'java.lang.NoSuchMethodError'
      WHEN reason_debug LIKE '%java.lang.OutOfMemoryError%' THEN 'java.lang.OutOfMemoryError'
      WHEN reason_debug LIKE '%java.io.FileNotFoundException%' THEN 'java.io.FileNotFoundException'
      WHEN reason_debug LIKE '%TimeoutCancellationException%' THEN 'TimeoutCancellationException'
      WHEN reason_debug LIKE '%NumberFormatException%' THEN 'NumberFormatException'
      WHEN reason_debug LIKE '%Could not create a request because URL is invalid%' THEN 'Could not create a request because URL is invalid'
      WHEN reason_debug LIKE '%ENOENT%' THEN 'ENOENT'
      WHEN reason_debug LIKE '%placement not found%' THEN 'placement not found'
      WHEN reason_debug LIKE '%BannerView not found for opportunityId%' THEN 'BannerView not found for opportunityId'
      WHEN reason_debug LIKE '%BannerView has been deleted%' THEN 'BannerView has been deleted'
      WHEN reason_debug LIKE '%Lifecycle Error%' THEN 'Lifecycle Error'
      WHEN reason_debug LIKE '%NullPointerException%' THEN 'NullPointerException'
      WHEN reason_debug LIKE '%ActivityNotFoundException%' THEN 'ActivityNotFoundException'
      WHEN reason_debug LIKE '%BannerView not found for opportunityId%' THEN 'BannerView not found for opportunityId'
      WHEN reason_debug LIKE '%BannerView has been deleted%' THEN 'BannerView has been deleted'
      WHEN reason_debug LIKE '%Lifecycle Error%' THEN 'Lifecycle Error'
      WHEN reason_debug LIKE '%is not valid JSON%' THEN 'Not valid JSON'
      ELSE reason_debug
    END AS reason_debug,
  CASE
      WHEN file_size IS NULL THEN NULL
      WHEN SAFE_CAST(file_size AS INT64) < 1024*1024 THEN '0-1MB'
      WHEN SAFE_CAST(file_size AS INT64) < 2*1024*1024 THEN '1-2MB'
      WHEN SAFE_CAST(file_size AS INT64) < 3*1024*1024 THEN '2-3MB'
      WHEN SAFE_CAST(file_size AS INT64) < 4*1024*1024 THEN '3-4MB'
      WHEN SAFE_CAST(file_size AS INT64) < 5*1024*1024 THEN '4-5MB'
      WHEN SAFE_CAST(file_size AS INT64) < 6*1024*1024 THEN '5-6MB'
      WHEN SAFE_CAST(file_size AS INT64) < 7*1024*1024 THEN '6-7MB'
      WHEN SAFE_CAST(file_size AS INT64) < 8*1024*1024 THEN '7-8MB'
      WHEN SAFE_CAST(file_size AS INT64) < 9*1024*1024 THEN '8-9MB'
      WHEN SAFE_CAST(file_size AS INT64) < 10*1024*1024 THEN '9-10MB'
      ELSE '10MB+' 
    END AS file_size_mb,
  CASE 
      WHEN video_duration_seconds IS NULL THEN NULL
      WHEN SAFE_CAST(video_duration_seconds AS INT64) <= 15 THEN '0-15s'
      WHEN SAFE_CAST(video_duration_seconds AS INT64) <= 30 THEN '16-30s'
      WHEN SAFE_CAST(video_duration_seconds AS INT64) <= 60 THEN '31-60s'
      WHEN SAFE_CAST(video_duration_seconds AS INT64) <= 120 THEN '61-120s'
      ELSE '120s+'
    END AS video_length_bucket,

FROM `unity-data-ads-core-prd.zone_bi_adhoc.sdk_metrics_v1_hourly_080914` m
-- where sdk_version >0
)

SELECT
    submit_hour,
    impression_opportunity_id,
    game_session_id,
    ANY_VALUE(placement_id) AS placement_id,
    ANY_VALUE(ad_format) AS ad_format,
    ANY_VALUE(webview_version) AS webview_version,
    ANY_VALUE(ad_type) AS is_fullscreen,
    ANY_VALUE(load_campaign_type) AS load_campaign_type,
    ANY_VALUE(load_content_type) AS load_content_type,
    ANY_VALUE(show_campaign_type) AS show_campaign_type,
    ANY_VALUE(show_content_type) AS show_content_type,
    ANY_VALUE(is_header_bidding) AS is_header_bidding,
    ANY_VALUE(platform) platform,
    ANY_VALUE(sdk_version) sdk_version,
    ANY_VALUE(source_game_id) source_game_id,
    ANY_VALUE(mediation_name) mediation_name,
    ANY_VALUE(mediation_version) mediation_version,
    ANY_VALUE(1) is_bold,
    ANY_VALUE(country) country,
    ANY_VALUE(iso_country) iso_country,
    ANY_VALUE(developer_id) developer_id,
    ANY_VALUE(developer_name) developer_name,
    ANY_VALUE(organization_id) organization_id,
    ANY_VALUE(core_organization) core_organization,
    ANY_VALUE(device_make) device_make,
    ANY_VALUE(device_model) device_model,
    ANY_VALUE(os_version) os_version,
    ANY_VALUE(ett) AS ett,   

--- hb notification ---
    ANY_VALUE(notif_load_auid) notif_load_auid,
    ANY_VALUE(notif_load_dsp_id) notif_load_dsp_id,
    ANY_VALUE(notif_load_content_type) notif_load_content_type,
    ANY_VALUE(notif_load_ad_format) notif_load_ad_format,
    ANY_VALUE(notif_load_campaign_id) notif_load_campaign_id,
    ANY_VALUE(notif_load_creative_pack_id) notif_load_creative_pack_id,

--- operative ---
    ANY_VALUE(op_show_ad_type) op_show_content_type,
    ANY_VALUE(op_show_ad_format) op_show_ad_format,
    ANY_VALUE(op_show_campaign_id) op_show_campaign_id,
    ANY_VALUE(op_show_audience_id) op_show_audience_id,
    ANY_VALUE(op_show_creative_pack_id) op_show_creative_pack_id,
    ANY_VALUE(op_show_creative_id) op_show_creative_id,

--- creative ---
    ANY_VALUE(file_size) file_size_byte,
    ANY_VALUE(file_size_mb) file_size_mb,
    ANY_VALUE(video_duration_seconds) video_duration_seconds,
    ANY_VALUE(video_length_bucket) video_length_bucket,

--- metrics_v1 ---    
    ANY_VALUE(IF(metric = 'native_load_failure_time', reason_debug, NULL)) AS load_error_reason_debug,
    ANY_VALUE(IF(metric = 'native_show_failure_time', reason_debug, NULL)) AS show_error_reason_debug,
    ANY_VALUE(IF(metric = 'native_show_failure_time', message, NULL)) AS show_error_message,
    ANY_VALUE(IF(metric = 'native_load_failure_time', message, NULL)) AS load_error_message,
    ARRAY_AGG(DISTINCT error_message IGNORE NULLS) AS error_messages,

    MAX(IF(metric = 'native_initialization_started', 1, 0)) AS native_initialization_started,
    MAX(IF(metric = 'native_initialize_task_success_time', 1, 0)) AS native_initialize_task_success_time,
    MAX(IF(metric = 'native_initialize_task_failure_time', 1, 0)) AS native_initialize_task_failure_time,

    MAX(IF(metric = 'native_load_started', 1, 0)) AS native_load_started,
    MAX(IF(metric IN ('native_load_failure_time', 'native_load_time_failure'), 1, 0)) AS native_load_failure_time,
    MAX(IF(metric IN ('native_load_success_time', 'native_load_time_success'), 1, 0)) AS native_load_success_time,
    MAX(IF(metric = 'native_load_started_ad_viewer', 1, 0)) AS native_load_started_ad_viewer,
    MAX(IF(metric = 'native_load_config_success_time', 1, 0)) AS native_load_config_success_time,
    MAX(IF(metric = 'native_load_config_failure_time', 1, 0)) AS native_load_config_failure_time,

    MAX(IF(metric = 'native_show_started', 1, 0)) AS native_show_started,
    MAX(IF(metric IN ('native_show_success_time', 'native_show_time_success'), 1, 0)) AS native_show_success_time,
    MAX(IF(metric IN ('native_show_failure_time', 'native_show_time_failure'), 1, 0)) AS native_show_failure_time,

    MAX(IF(metric = 'ad_viewer_native_show_call', 1, 0)) AS ad_viewer_native_show_call,
    MAX(IF(metric = 'ad_viewer_ad_content_loaded', 1, 0)) AS ad_viewer_ad_content_loaded,
    MAX(IF(metric = 'ad_viewer_ad_content_start', 1, 0)) AS ad_viewer_ad_content_start,
    MAX(IF(metric = 'ad_viewer_ad_content_ended', 1, 0)) AS ad_viewer_ad_content_ended,
    MAX(IF(metric = 'ad_viewer_ad_content_rewarded', 1, 0)) AS ad_viewer_ad_content_rewarded,
    MAX(IF(metric = 'ad_viewer_ad_content_skip', 1, 0)) AS ad_viewer_ad_content_skip,
    MAX(IF(metric = 'native_show_started_ad_viewer', 1, 0)) AS native_show_started_ad_viewer,
    MAX(IF(metric = 'native_show_wv_started', 1, 0)) AS native_show_wv_started,

    SUM(IF(metric = 'native_load_cache_success_time', 1, 0)) AS native_load_cache_success_time_count,
    SUM(IF(metric = 'native_load_cache_failure_time', 1, 0)) AS native_load_cache_failure_time_count,
    MAX(IF(metric = 'native_webview_terminated', 1, 0)) AS webview_terminated,
FROM (select * from base where producer = 'gwv1')
WHERE 1=1
and impression_opportunity_id <> '' and game_session_id is not null
GROUP BY submit_hour,impression_opportunity_id,game_session_id
UNION ALL
SELECT
    submit_hour,
    impression_opportunity_id,
    game_session_id,
    placement_id,
    ad_format,
    webview_version,
    ad_type AS is_fullscreen,
    load_campaign_type,
    load_content_type,
    show_campaign_type,
    show_content_type,
    is_header_bidding,
    platform platform,
    sdk_version sdk_version,
    source_game_id source_game_id,
    mediation_name mediation_name,
    mediation_version mediation_version,
    0 is_bold,
    country country,
    iso_country iso_country,
    developer_id developer_id,
    developer_name developer_name,
    organization_id organization_id,
    core_organization core_organization,
    device_make device_make,
    device_model device_model,
    os_version os_version,
    ett AS ett,   

--- hb notification ---
    NULL AS notif_load_auid,
    NULL AS notif_load_dsp_id,
    NULL AS notif_load_content_type,
    NULL AS notif_load_ad_format,
    NULL AS notif_load_campaign_id,
    NULL AS notif_load_creative_pack_id,

--- operative ---
    NULL AS op_show_content_type,
    NULL AS op_show_ad_format,
    NULL AS op_show_campaign_id,
    NULL AS op_show_audience_id,
    NULL AS op_show_creative_pack_id,
    NULL AS op_show_creative_id,
    
--- creative ---
    NULL AS file_size_byte,
    NULL AS file_size_mb,
    NULL AS video_duration_seconds,
    NULL AS video_length_bucket,

--- metrics_v1 ---
    
    ANY_VALUE(IF(metric = 'native_load_failure_time', reason_debug, NULL)) AS load_error_reason_debug,
    ANY_VALUE(IF(metric = 'native_show_failure_time', reason_debug, NULL)) AS show_error_reason_debug,
    ANY_VALUE(IF(metric = 'native_show_failure_time', message, NULL)) AS show_error_message,
    ANY_VALUE(IF(metric = 'native_load_failure_time', message, NULL)) AS load_error_message,
    ARRAY_AGG(DISTINCT error_message IGNORE NULLS) AS error_messages,

    SUM(IF(metric = 'native_initialization_started', 1, 0)) AS native_initialization_started,
    SUM(IF(metric = 'native_initialize_task_success_time', 1, 0)) AS native_initialize_task_success_time,
    SUM(IF(metric = 'native_initialize_task_failure_time', 1, 0)) AS native_initialize_task_failure_time,

    SUM(IF(metric = 'native_load_started', 1, 0)) AS native_load_started,
    SUM(IF(metric IN ('native_load_failure_time', 'native_load_time_failure'), 1, 0)) AS native_load_failure_time,
    SUM(IF(metric IN ('native_load_success_time', 'native_load_time_success'), 1, 0)) AS native_load_success_time,
    SUM(IF(metric = 'native_load_started_ad_viewer', 1, 0)) AS native_load_started_ad_viewer,
    SUM(IF(metric = 'native_load_config_success_time', 1, 0)) AS native_load_config_success_time,
    SUM(IF(metric = 'native_load_config_failure_time', 1, 0)) AS native_load_config_failure_time,

    SUM(IF(metric = 'native_show_started', 1, 0)) AS native_show_started,
    SUM(IF(metric IN ('native_show_success_time', 'native_show_time_success'), 1, 0)) AS native_show_success_time,
    SUM(IF(metric IN ('native_show_failure_time', 'native_show_time_failure'), 1, 0)) AS native_show_failure_time,

    SUM(IF(metric = 'ad_viewer_native_show_call', 1, 0)) AS ad_viewer_native_show_call,
    SUM(IF(metric = 'ad_viewer_ad_content_loaded', 1, 0)) AS ad_viewer_ad_content_loaded,
    SUM(IF(metric = 'ad_viewer_ad_content_start', 1, 0)) AS ad_viewer_ad_content_start,
    SUM(IF(metric = 'ad_viewer_ad_content_ended', 1, 0)) AS ad_viewer_ad_content_ended,
    SUM(IF(metric = 'ad_viewer_ad_content_rewarded', 1, 0)) AS ad_viewer_ad_content_rewarded,
    SUM(IF(metric = 'ad_viewer_ad_content_skip', 1, 0)) AS ad_viewer_ad_content_skip,
    SUM(IF(metric = 'native_show_started_ad_viewer', 1, 0)) AS native_show_started_ad_viewer,
    SUM(IF(metric = 'native_show_wv_started', 1, 0)) AS native_show_wv_started,

    SUM(IF(metric = 'native_load_cache_success_time', 1, 0)) AS native_load_cache_success_time_count,
    SUM(IF(metric = 'native_load_cache_failure_time', 1, 0)) AS native_load_cache_failure_time_count,
    SUM(IF(metric = 'native_webview_terminated', 1, 0)) AS webview_terminated,

FROM (select * from base where producer = 'gwl')
GROUP BY ALL
