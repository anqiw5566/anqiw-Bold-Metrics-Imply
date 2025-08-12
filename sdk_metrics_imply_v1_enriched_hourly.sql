CREATE OR REPLACE table `unity-data-ads-core-prd.zone_bi_adhoc.sdk_metrics_v1_hourly_080914` AS
SELECT 
m.* EXCEPT (metric_state,scar,metric_type),

--- hb notification ---
o.auction_id AS notif_load_time_auid, 
n.body.request.bid.DSPID AS notif_load_time_dsp_id,
n.body.request.bid.contentType AS notif_load_time_content_type,
n.body.request.placement.adFormat AS notif_load_time_ad_format,
n.body.request.bid.campaignID AS notif_load_time_campaign_id,
n.body.request.bid.creativeID AS notif_load_time_creative_pack_id,

--- operative ---
op.cinfo.adtype AS op_show_time_ad_type,
op.adfmt AS op_show_time_adFormat,
-- op.ctyp AS op_show_time_campaign_type,
op.cinfo.cid AS op_show_time_campaign_id,
op.cinfo.audid AS op_show_time_audience_id,
op.cinfo.cpackid AS op_show_time_creative_pack_id,
op.cinfo.ucid AS op_show_time_creative_id,

--- creative ---
JSON_EXTRACT_SCALAR(file_metadata, '$.fileSize') file_size,

--- developers & games ---
dev.developer_id,
dev.name as developer_name,
game.organization_id,
game.core_organization

FROM `unity-data-ads-core-prd.zone_product_reporting_unity.sdk_metrics_raw_deduped_parsed` m

LEFT JOIN `unity-data-ads-prd.adsgateway.impression_opportunity_v1alpha1` o 
ON m.impression_opportunity_id = o.impression_opportunity_id 
AND m.game_session_id = o.game_session_id 
-- AND DATE(o._lapio_submit_time) =  "2025-08-09"
AND TIMESTAMP_TRUNC(o._lapio_submit_time, HOUR) = "2025-08-09 14:00:00"

LEFT JOIN `unity-ai-data-prd.ads_hb_raw.ads_hb_notification_enriched_v3` n
ON n.body.notification.exchangeAuctionId = o.auction_id 
-- AND DATE(n.submit_date) = "2025-08-09"
AND TIMESTAMP_TRUNC(context.pipeline_context.submit_time, HOUR) = "2025-08-09 14:00:00"

LEFT JOIN `unity-data-ads-prd.serversidemeta.operative_event_v1` op
ON o.auction_id = op.auid
-- AND DATE(op._lapio_submit_time) = "2025-08-09"
AND TIMESTAMP_TRUNC(op._lapio_submit_time, HOUR) = "2025-08-09 14:00:00"

LEFT JOIN `unity-data-ads-core-prd.ads_dimension_data.creatives` c
ON op.cinfo.ucid = c.creative_id

LEFT JOIN `unity-data-ads-core-prd.ads_dimension_data.game_profiles` game
ON m.source_game_id = game.game_id

LEFT JOIN `unity-data-ads-core-prd.ads_dimension_data.developers` dev
ON game.developer_id = dev.developer_id

WHERE 1=1
AND submit_hour = "2025-08-09 14:00:00"
AND DATE(n.submit_date) = "2025-08-09"
AND DATE(o._lapio_submit_time) =  "2025-08-09"
AND DATE(op._lapio_submit_time) = "2025-08-09"
-- AND m.impression_opportunity_id is not null
-- AND m.game_session_id >0
-- AND m.is_header_bidding
GROUP BY ALL



