/* Task 1 */

/* count events by day */
select date,
  count(event) as count_events,
  countIf(event = 'view') as count_views,
  countIf(event = 'click') as count_clicks
from ads_data
group by date
;

/* total number of unique ads and campaigns */
select count(distinct(ad_id)) as ad_id_uniqueCount,
    count(distinct(campaign_union_id)) as campaign_id_uniqueCount
from ads_data
;

/* Task 2 */

/*
lots of activity on some of the ads
Huge!!! number of clicks on 112583
*/

select ad_id,
       count(event) as total_interactions,
       countIf(event = 'view') as total_views,
       countIf(event = 'click') as total_clicks
from ads_data
where date = '2019-04-05'
group by ad_id
order by total_interactions desc
;

/* let's check some key stats per platform on this day and the preceding days */
/* views increased on all platforms, and actually started increasing the previous day */
select date, platform,
       countIf(event = 'view') as count_views,
       countIf(event = 'click') as count_clicks
from ads_data
group by date, platform
order by platform, date
;

/* let's check stats for key offender 112583 */
select date, platform,
       countIf(event = 'click') as count_clicks,
       countIf(event = 'view') as count_views
from ads_data
where ad_id = 112583
group by date, platform
order by platform, date
;

/*
so it only appeared on 2019-04-05 and broke everything
let's look at the stats for some other top-click ads
*/

select date, ad_id, count(event) as count_clicks
from ads_data
where ad_id in
    (select t1.ad_id
    from (
        select ad_id, count(event) as count_clicks
        from ads_data
        where event = 'click' and date = '2019-04-05'
        group by ad_id
        order by count_clicks desc
        ) as t1
    limit 10)
group by date, ad_id
order by ad_id, date
;

/*
 So NONE of the top-10 clicked ads on 2019-04-05 were ever clicked before!
 interesting. a group of ads with high clickability appeared and broke everything.
 */


/* TASK 3 */

/*
CTR top 10
using another counting method just for lulz
*/
select ad_id,
       countIf(event = 'click') as count_clicks,
       countIf(event = 'view') as count_views,
       (count_clicks / count_views) as CTR
from ads_data
group by ad_id
having count_clicks > 0 and count_views > 0
order by CTR desc
limit 10
;

/* mean, median CTRs*/
select avg(t1.CTR) as ctr_mean, median(t1.CTR) as ctr_median
from (
      select ad_id,
             countIf(event = 'click') as count_clicks,
             countIf(event = 'view') as count_views,
             (count_clicks / count_views) as CTR
      from ads_data
      group by ad_id
      having count_clicks > 0
         and count_views > 0
         ) as t1
;

/*
 Task 4
 */

/* let's select all ads which were clicked but never viewed */
select ad_id,
       arraySort(arrayDistinct(groupArray(platform))) as platforms_list,
       count(event) as number_misclicks
from ads_data
where ad_id not in
(select distinct ad_id
from ads_data
where event = 'view')
group by ad_id
;

/* all platforms. let's see if there is any logic behind it */

select time,
       ad_cost,
       ad_cost_type, platform, target_audience_count, client_union_id, campaign_union_id, has_video
from ads_data
where ad_id not in
(select distinct ad_id
from ads_data
where event = 'view')
;

/*
All the events happened on 04-01.
So maybe there was some update that introduced bugs that day?
*/

/*
Task 5
 */

select has_video,
       avg(ctr) as ctr_avg,
       median(ctr) as ctr_median
from
(select ad_id,
       has_video,
        platform,
       countIf(event = 'click') as count_clicks,
       countIf(event = 'view') as count_views,
       count_clicks / count_views as ctr
from ads_data
group by ad_id, has_video, platform
having count_views > 0) as ctr_data
group by has_video
order by has_video
;

select has_video,
       platform,
       avg(ctr) as ctr_avg,
       median(ctr) as ctr_median
from
(select ad_id,
       has_video,
        platform,
       countIf(event = 'click') as count_clicks,
       countIf(event = 'view') as count_views,
       count_clicks / count_views as ctr
from ads_data
group by ad_id, has_video, platform
having count_views > 0) as ctr_data
group by has_video, platform
order by platform, has_video
;

/* so ctr is lower for ads with videos (one exception is avg ctr on ios, but avg is prone to outliers and can be misleading */

select quantile(0.95)(ctr) as q95
from
(select ad_id,
       countIf(event = 'click') as count_clicks,
       countIf(event = 'view') as count_views,
       count_clicks / count_views as ctr
from ads_data
where date = '2019-04-04'
group by ad_id
having count_views > 0)
;

/* Task 6 */

select date,
       sum(multiIf(
           event = 'view' and ad_cost_type = 'CPM',
           ad_cost / 1000,
           event = 'click' and ad_cost_type = 'CPC',
           ad_cost,
           0
           )) as profit
from ads_data
group by date
order by profit desc
;

select date, v.view_profit, c.click_profit,
       v.view_profit + c.click_profit as total_profit
from
    (select date, sum(cost_views) as view_profit
    from
        (select date,
        ad_id,
        ad_cost / 1000 * countIf(event = 'view') as cost_views
        from ads_data
        where ad_cost_type = 'CPM'
        group by date, ad_id, ad_cost
        having cost_views > 0)
        group by date) as v
join
    (select date, sum(cost_clicks) as click_profit
    from
        (select date,
        ad_id,
        ad_cost * countIf(event = 'click') as cost_clicks
        from ads_data
        where ad_cost_type = 'CPC'
        group by date, ad_id, ad_cost
        having cost_clicks > 0)
        group by date) as c on v.date = c.date
order by total_profit desc
;

/* Task 7 */

/* all-time */
select countIf(platform = 'ios') / count(platform) as ios,
       countIf(platform = 'android') / count(platform) as android,
       countIf(platform = 'web') / count(platform) as web,
       ios+android+web as checkTotal
from ads_data
where event = 'view'
;

/* by date */
select date,
       countIf(platform = 'ios') / count(platform) as ios,
       countIf(platform = 'android') / count(platform) as android,
       countIf(platform = 'web') / count(platform) as web,
       ios+android+web as checkTotal
from ads_data
where event = 'view'
group by date
;

/* Task 8 */
select ad_id, firstClick, firstView
from
    (select ad_id,
            arrayElement(arraySort(groupArray(time)), 1) as firstClick
    from ads_data
    where event = 'click'
    group by ad_id) as c
    join
    (select ad_id,
            arrayElement(arraySort(groupArray(time)), 1) as firstView
    from ads_data
    where event = 'view'
    group by ad_id) as v
    on c.ad_id = v.ad_id
where 0 = 0
  and firstClick is not null
  and firstView is not null
  and firstClick < firstView
order by firstClick
;

select uniq(ad_id) as countBugs
from(
    select ad_id,
           platform,
           argMin(event, time) as firstEvent
    from ads_data
    group by ad_id, platform
    having firstEvent = 'click')
;

SELECT ad_id, platform, argMin(event, time) as first_event
      FROM ads_data
      GROUP BY ad_id, platform
      HAVING first_event = 'click'