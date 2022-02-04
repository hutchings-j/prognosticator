import pyathena
from pyathena.pandas.async_cursor import AsyncPandasCursor

# --------------------------------------------------------------------------------------------------------
# Get inputs for Athena query function
# --------------------------------------------------------------------------------------------------------
s3_staging_dir = 's3://aws-athena-query-results-128936286462-us-east-1/'
region_name = 'us-east-1'
py_curs = {'s3_staging_dir': s3_staging_dir, 'region_name': region_name}

# Athena query function
def queryAthena(query, parse_dates=False, na_values = [""]):
    pyathena_conn = pyathena.connect(**py_curs, cursor_class=AsyncPandasCursor)
    cursor = pyathena_conn.cursor(max_workers=4)
    query_id, results = cursor.execute(query, keep_default_na=False, na_values=[""])
    df = results.result().as_pandas()
    return df

def get_attom_data():
    # Get base raw data
    queryAthena("""drop table if exists data_science.stage_attom1;""")
    queryAthena("""create table data_science.stage_attom1 as
    select distinct
        cc_list_id as list_id
        , cc_property_id as property_id
        , titan_id
        , apn
        , address_street_number
        , address_dir_prefix
        , address_street_name
        , address_dir_suffix
        , address_street_suffix
        , case when substring(address_unit_number, 1, 1) = '#' and length(address_unit_number) = 1 then null
            when substring(address_unit_number, 1, 1) = '#' and length(address_unit_number) >= 2 then ltrim(address_unit_number, '#')
            when address_unit_number is not null
                and address_unit_number not in ((case when address_street_number is null then '' else address_street_number end),
                                                (case when address_street_name is null then '' else address_street_name end),
                                                (case when address_dir_prefix is null then '' else address_dir_prefix end),
                                                (case when address_street_suffix is null then '' else address_street_suffix end),
                                                (case when address_dir_suffix is null then '' else address_dir_suffix end),
                                                'n', 's', 'e', 'w')
                then address_unit_number
            else null end as address_unit_number
        , case when substring(address_unit_number, 1, 1) = '#' and length(address_unit_number) >= 2 then '#' else null end as address_unit_type
        , address_city
        , address_state
        , address_zip
        , address_county
        , fips_county_cd
        , cbsa_id
        , case when address_unit_number is not null
            and address_unit_number not in (address_street_number, address_dir_prefix, address_street_suffix, address_dir_suffix)
            then 'Multi Unit' else property_type end as property_type
        , latitude
        , longitude
        , market
        , bedrooms
        , full_baths
        , half_baths
        , year_built
        , gla_sqft as sqft
        , most_recent_sale
        , most_recent_sale_date
        , status
        , status_date
        , price
    from (
        select distinct
            cc_list_id
            , mls_name
            , cc_property_id
            , titan_id
            , case when regexp_replace(apn, '[^2-9a-z]+') = '' then null
                when regexp_replace(apn, '[^1-8a-z]+') = '' then null
                when regexp_replace(apn, '[^1-9]+') = '' then null
                when length(apn) <= 5 then null
                when length(regexp_replace(apn, '[^1-9a-z]+]')) <= 3 then null
                when apn like '%not%' then null
                when apn like '123%' then null
                when apn like '0123%' then null
                when apn like '%acre%' then null
                when apn in ('unknown', 'multiple', 'unavailable') then null
                else apn end as apn
            , ltrim(regexp_replace(address_street_number, '[^0-9]+'), '0') as address_street_number
            , address_dir_prefix
            , address_street_name
            , address_dir_suffix
            , address_street_suffix
            , case when address_unit_number is not null then '#'||address_unit_number
                when substring(address_last_element, 1, 1) = '#' then address_last_element
                when substring(address_last_element, 1, 1) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9') then address_last_element
                when lower(substring(address_last_element, 1, 1)) in ('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n','o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')
                    and (length(address_last_element) = 1 or lower(substring(address_last_element, 2, 1)) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) then address_last_element
                else null end as address_unit_number
            , address_city
            , address_state
            , address_zip
            , address_county
            , fips_county_cd
            , cbsa_id
            , property_type
            , latitude
            , longitude
            , market
            , bedrooms
            , full_baths
            , half_baths
            , year_built
            , gla_sqft
            , basement_size_sqft
            , most_recent_sale
            , most_recent_sale_date
            , status
            , status_date
            , price
        from (
            select distinct
                cc_list_id
                , mls_name
                , cc_property_id
                , titan_id
                , regexp_replace(lower(apn), '[^0-9a-z]+') as apn
                , lower(cc_property_address_number) as address_street_number
                , lower(cc_property_address_dir_prefix) as address_dir_prefix
                , lower(cc_property_address_street_name) as address_street_name
                , lower(cc_property_address_dir_suffix) as address_dir_suffix
                , lower(cc_property_address_street_suffix) as address_street_suffix
                , lower(cc_property_address_unit_number) as address_unit_number
                , lower(cc_property_address_city) as address_city
                , lower(cc_property_address_state) as address_state
                , substring(lower(cc_property_address_postal_code), 1, 5) as address_zip
                , case when address like '%#%'
                        then '#'||lower(regexp_replace(split_part(ltrim(split_part(lower(address), '#', 2), ' '), ' ', 1), '[^0-9a-z]+'))
                    when address like '% ## %'
                        then '#'||lower(regexp_replace(split_part(ltrim(split_part(lower(address), ' ## ', 2), ' '), ' ', 1), '[^0-9a-z]+'))
                    when lower(address) like '%unit%'
                        then '#'||lower(regexp_replace(split_part(ltrim(split_part(lower(address), 'unit', 2), ' '), ' ', 1), '[^0-9a-z]+'))
                    when strpos(address, cc_property_address_street_name) >= 1
                            and strpos(address, cc_property_address_number) >= 1
                        then lower(regexp_replace(reverse(split_part(reverse(lower(address)), ' ', 1)), '[^0-9a-z]+'))
                    end as address_last_element
                , lower(cc_property_address_county) as address_county
                , fips_county_cd
                , cbsa_id
                , case when standardized_property_type in ('Condominium', 'Townhome', 'Duplex', 'Row House', 'General Single Family Attached', 'General Multi Family', 'Triplex', 'Fourplex') then 'Multi Unit'
                    when cc_property_address_unit_number is not null then 'Multi Unit'
                    else standardized_property_type end as property_type
                , case when property_latitude is not null then property_latitude else knock_latitude end as latitude
                , case when property_longitude is not null then property_longitude else knock_longitude end as longitude
                , market
                , bedrooms
                , full_baths
                , half_baths
                , year_built
                , gla_sqft
                , basement_size_sqft
                , most_recent_sale
                , most_recent_sale_date
                , case when status in ('Closed List', 'Cancelled', 'Withdrawn', 'Expired', 'Incomplete', 'Deleted') then 'Delisted'
                    when status = 'Contingent' then 'Active' else status end as status
                , date(date) as status_date
                , case when price > 99999999 then null when price < 50000 then null else price end as price
            from attom.merged_log_v5
            where deleted = False
                and date(date) >= date_add('month', -96, date_trunc('month', current_date))
                and date(date) <= current_date
                and standardized_property_type in ('Condominium', 'Townhome', 'Duplex', 'Mobile', 'Row House', 'General Single Family Attached', 'General Single Family', 'General Single Family Detached', 'Unknown Residential', 'General Multi Family', 'General Manufactured', 'Factory Built', 'Triplex', 'Fourplex')
                and cc_property_address_number is not null
                and cc_property_address_street_name is not null
                and cc_property_address_postal_code is not null))
    where address_street_number != ''
        and address_street_number is not null;
    """)
    print("attom1 completed")
    
    # Identify and relabel property_ids with multiple units based on address_unit_number
    queryAthena("""drop table if exists data_science.stage_attom2;""")
    queryAthena("""create table data_science.stage_attom2 as
    select distinct
        t1.list_id
        , case when t2.aun_cnt >= 2 and t1.address_unit_number is not null then t1.property_id||'_aun_'||t1.address_unit_number
            when t2.sqft_cnt >= 3 and t1.sqft is not null then t1.property_id||'_sqft_'||t1.sqft
            when t2.most_recent_sale_cnt >= 3 and t1.most_recent_sale is not null and t1.most_recent_sale_date is not null
                then t1.property_id||'_mrs_'||cast(most_recent_sale as varchar(10))||'_'||cast(date(most_recent_sale_date) as varchar(10))
                else t1.property_id end as property_id
        , t1.titan_id
        , t1.apn
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_unit_type
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.fips_county_cd
        , t1.cbsa_id
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t1.most_recent_sale
        , t1.most_recent_sale_date
        , t1.status
        , t1.status_date
        , t1.price
    from data_science.stage_attom1 as t1
    left join (
        select
            property_id
            , count(distinct address_unit_number) as aun_cnt
            , count(distinct sqft) as sqft_cnt
            , count(distinct cast(most_recent_sale as varchar(10))||cast(date(most_recent_sale_date) as varchar(10))) as most_recent_sale_cnt
        from data_science.stage_attom1
        group by 1
        having count(distinct address_unit_number) >= 2
            or count(distinct sqft) >= 3
            or count(distinct cast(most_recent_sale as varchar(10))||cast(date(most_recent_sale_date) as varchar(10))) >= 3
        ) as t2
        on t1.property_id = t2.property_id;""")
    print("attom2 completed")
    
    # Remove all status changes after the date of 'Sale', remove any 'Delisted' status changes on the date of 'Sale' for all list_ids
    # Get all list_ids with no sale (almost all of these are currently for sale)
    # Get all list_ids with a sale (these are all historical sales)
    queryAthena("""drop table if exists data_science.stage_attom3;""")
    queryAthena("""create table data_science.stage_attom3 as
    select distinct t1.* from data_science.stage_attom2 as t1
    inner join (
        select
            list_id
            , property_id
            , case when sum(case when status = 'Sale' then 1 else 0 end) >= 1 then 1 else 0 end as sale_ind
        from data_science.stage_attom2
        group by 1, 2
        having sum(case when status = 'Sale' then 1 else 0 end) = 0
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
    union all
    select t1.* from data_science.stage_attom2 as t1
    inner join (
        select
            list_id
            , property_id
            , min(status_date) as sale_date
        from data_science.stage_attom2
        where status = 'Sale'
        group by 1, 2
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
            and ((t1.status_date <= t2.sale_date and t1.status != 'Delisted')
                or (t1.status_date < t2.sale_date and t1.status = 'Delisted'));""")
    print("attom3 completed")
    
    # Keep list records that come after the first active list record
    queryAthena("""drop table if exists data_science.stage_attom4;""")
    queryAthena("""create table data_science.stage_attom4 as
    select t1.* from data_science.stage_attom3 as t1
    inner join (
        select
            property_id
            , list_id
            , min(status_date) as status_date
        from data_science.stage_attom3
        where status = 'Active'
        group by 1, 2
        ) as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id
            and t1.status_date >= t2.status_date;""")
    print("attom4 completed")
    
    # Add delist record to listings that have been active for 12 months with no status change
    queryAthena("""drop table if exists data_science.stage_attom5;""")
    queryAthena("""create table data_science.stage_attom5 as
    with statuses as (
        select
            property_id
            , list_id
            , max(status_date) as last_status_date
            , max(case when status = 'Active' then status_date end) as last_active_date
            , max(case when status = 'Delisted' then status_date end) as last_delisted_date
            , max(case when status = 'Pending' then status_date end) as last_pending_date
            , max(case when status = 'Sale' then status_date end) as last_sale_date
        from data_science.stage_attom4
        group by 1, 2
        )
    select * from data_science.stage_attom4
    union all
    select
        t1.list_id
        , t1.property_id
        , t1.apn
        , t1.titan_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_unit_type
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.fips_county_cd
        , t1.cbsa_id
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t1.most_recent_sale
        , t1.most_recent_sale_date
        , 'Delisted' as status
        , date_add('day', 180, t1.status_date) as status_date
        , t1.price
    from data_science.stage_attom4 as t1
    inner join (
        select * from statuses
        where last_active_date = last_status_date
            and last_active_date < date_add('month', -12, current_date)
            and (last_delisted_date is null or last_delisted_date < last_status_date)
            and (last_pending_date is null or last_pending_date < last_status_date)
            and last_sale_date is null
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id;
    """)
    print("attom5 completed")
    
    # Add Pending status record to listings with Sale status and no Pending status
    queryAthena("""drop table if exists data_science.stage_attom6;""")
    queryAthena("""create table data_science.stage_attom6 as
    select * from data_science.stage_attom5
    union all
    select
        t1.list_id
        , t1.property_id
        , t1.apn
        , t1.titan_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_unit_type
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.fips_county_cd
        , t1.cbsa_id
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t1.most_recent_sale
        , t1.most_recent_sale_date
        , 'Pending' as status
        , min(t1.status_date) as status_date
        , t1.price
    from data_science.stage_attom5 as t1
    inner join (
        select
            list_id
            , property_id
            , case when sum(case when status = 'Pending' then 1 else 0 end) >= 1 then 1 else 0 end as pending_ind
            , case when sum(case when status = 'Sale' then 1 else 0 end) >= 1 then 1 else 0 end as sale_ind
        from data_science.stage_attom5
        group by 1, 2
        having sum(case when status = 'Pending' then 1 else 0 end) = 0
            and sum(case when status = 'Sale' then 1 else 0 end) >= 1
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
    where t1.status = 'Sale'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 31;
    """)
    print("attom6 completed")
    
    # Remove excess delisted records
    queryAthena("""drop table if exists data_science.stage_attom7;""")
    queryAthena("""create table data_science.stage_attom7 as
    select a1.* from data_science.stage_attom6 as a1
    left join (
        select
            t1.*
            , t2.status as prev_status
        from (
            select
                t1.property_id
                , t1.list_id
                , t1.status
                , t1.status_date
                , max(t2.status_date) as prev_status_date
            from data_science.stage_attom6 as t1
            inner join data_science.stage_attom6 as t2
                on t1.list_id = t2.list_id
                    and t1.property_id = t2.property_id
                    and t2.status_date < t1.status_date
            where t1.status = 'Delisted'
            group by 1, 2, 3, 4
            ) as t1
        inner join data_science.stage_attom6 as t2
            on t1.list_id = t2.list_id
                and t1.property_id = t2.property_id
                and t1.prev_status_date = t2.status_date
        ) as a2
        on a1.list_id = a2.list_id
            and a1.property_id = a2.property_id
            and a1.status = a2.status
            and a1.status_date = a2.status_date
            and a2.prev_status = 'Delisted'
    where a2.property_id is null;
    """)
    print("attom7 completed")
    
    # Find records where Delisted is the last status and the previous status is Active and the days between is 365+
    # Adjust the Delisted record status_date to one day prior so it doesn't overlap with a new listing
    queryAthena("""drop table if exists data_science.stage_attom8;""")
    queryAthena("""
    create table data_science.stage_attom8 as
    with statuses as (
        select
            property_id
            , list_id
            , sum(case when status_rank = 1 and status = 'Delisted' then 1 else 0 end) as delisted_ultimate_status
            , sum(case when status_rank = 2 and status = 'Active' then 1 else 0 end) as active_penultimate_status
            , max(case when status_rank = 1 then status_date end) as delisted_ultimate_status_date
            , max(case when status_rank = 2 then status_date end) as active_penultimate_status_date
        from (
            select
                property_id
                , list_id
                , status
                , status_date
                , rank() over (partition by property_id, list_id order by property_id, list_id, status_date desc) as status_rank
            from data_science.stage_attom7 as t1
            order by property_id, list_id, status_date desc)
        where status_rank <= 2
        group by 1, 2
        having sum(case when status_rank = 1 and status = 'Delisted' then 1 else 0 end) >= 1
            and sum(case when status_rank = 2 and status = 'Active' then 1 else 0 end) >= 1
        )
    select distinct
        t1.list_id
        , t1.property_id
        , t1.apn
        , t1.titan_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_unit_type
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.fips_county_cd
        , t1.cbsa_id
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t1.most_recent_sale
        , t1.most_recent_sale_date
        , t1.status
        , case when t2.property_id is not null then date_add('day', -1, t1.status_date) else t1.status_date end as status_date
        , t1.price
    from data_science.stage_attom7 as t1
    left join (
        select * from statuses
        where date_diff('day', active_penultimate_status_date, delisted_ultimate_status_date) > 365
        ) as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id
            and t1.status_date = t2.delisted_ultimate_status_date
            and t1.status = 'Delisted';
    """)
    print("attom8 completed")
    
    # Get initial overlapping listings
    # If listing does not have a Sale or Delisted status, then list_end_date should be the last status_date plus 12 months
    queryAthena("""drop table if exists data_science.stage_attom_overlaps1;""")
    queryAthena("""
    create table data_science.stage_attom_overlaps1 as
    with listings as (
        select distinct
            t1.list_id
            , t1.property_id
            , t2.list_start_date
            , case when sum(case when t1.status in ('Sale', 'Delisted') then 1 else 0 end) >= 1
                then t2.list_end_date else date_add('month', 12, t2.list_end_date) end as list_end_date
            , t2.list_end_date as list_end_date_init
        from data_science.stage_attom8 as t1
        inner join (
            select
                list_id
                , property_id
                , date(min(status_date)) as list_start_date
                , date(max(status_date)) as list_end_date
            from data_science.stage_attom8
            group by 1, 2
            ) as t2
            on t1.list_id = t2.list_id
                and t1.property_id = t2.property_id
                and t1.status_date = t2.list_end_date
        group by 1, 2, 3, 5)
    select distinct
        a1.property_id
        , a1.list_id
        , a2.list_id as list_id_overlap1
        , a1.list_start_date
        , a2.list_start_date as list_start_date_overlap1
        , a1.list_end_date
        , a2.list_end_date as list_end_date_overlap1
        , greatest(a1.list_end_date, a2.list_end_date) as list_end_date_acme
    from listings as a1
    inner join listings as a2
        on a1.property_id = a2.property_id
            and a1.list_id != a2.list_id
            and a1.list_start_date >= a2.list_start_date and a1.list_start_date <= a2.list_end_date;
    """)
    
    # Get records with min and max list_start_date and list_end_date_acme
    # Remove records that are subsumed within another record
    queryAthena("""drop table if exists data_science.stage_attom_overlaps2;""")
    queryAthena("""
    create table data_science.stage_attom_overlaps2 as
    with start_ends as (
        select
            property_id
            , list_start_date
            , list_end_date_acme as list_end_date
            , row_number() over (partition by property_id order by property_id, list_start_date) as list_id
        from (
            select
                property_id
                , min(list_start_date) as list_start_date
                , list_end_date_acme
            from (
                select
                    property_id
                    , list_start_date
                    , max(list_end_date_acme) as list_end_date_acme
                from data_science.stage_attom_overlaps1
                group by 1, 2)
            group by 1, 3)
        )
    select t1.* from start_ends as t1
    left join start_ends as t2
        on t1.property_id = t2.property_id
            and t1.list_start_date < t2.list_start_date
            and t1.list_end_date > t2.list_end_date
    where t2.property_id is null;
    """)
    
    # Delineate list_ids that are have a succeeding overlapping list_id and list_ids that have a preceeding overlapping list_id and those with both
    queryAthena("""drop table if exists data_science.stage_attom_overlaps3;""")
    queryAthena("""
    create table data_science.stage_attom_overlaps3 as
    select
        t1.property_id
        , t1.list_start_date
        , t1.list_end_date
        , case when sum(case when t2.property_id is not null then 1 else 0 end) >= 1 then 1 else 0 end as has_succeeding_ind
        , case when sum(case when t3.property_id is not null then 1 else 0 end) >= 1 then 1 else 0 end as has_preceeding_ind
    from data_science.stage_attom_overlaps2 as t1
    left join data_science.stage_attom_overlaps2 as t2
        on t1.property_id = t2.property_id
            and t2.list_start_date > t1.list_start_date
            and t2.list_start_date <= t1.list_end_date
    left join data_science.stage_attom_overlaps2 as t3
        on t1.property_id = t3.property_id
            and t1.list_start_date > t3.list_start_date
            and t1.list_start_date <= t3.list_end_date
    group by 1, 2, 3;
    """)
    
    queryAthena("""drop table if exists data_science.stage_attom_overlaps;""")
    queryAthena("""create table data_science.stage_attom_overlaps as
    select
        property_id
        , list_start_date
        , list_end_date
        , cast(rank() over (partition by property_id order by property_id, list_end_date) as varchar(10)) as list_id_overlap
    from (
        select distinct
            property_id
            , list_start_date
            , list_end_date
        from data_science.stage_attom_overlaps3
        where has_succeeding_ind = 0
            and has_preceeding_ind = 0
        union all
        select
            t1.property_id
            , t1.list_start_date
            , min(t2.list_end_date) as list_end_date
        from data_science.stage_attom_overlaps3 as t1
        inner join data_science.stage_attom_overlaps3 as t2
            on t1.property_id = t2.property_id
                and t1.list_start_date < t2.list_start_date
        where t1.has_succeeding_ind = 1
            and t1.has_preceeding_ind = 0
            and t2.has_succeeding_ind = 0
            and t2.has_preceeding_ind = 1
        group by 1, 2);
    """)
    print("attom overlaps completed")
    
    # Relabel list_ids that overlap
    # Do not retain most_recent_sale and most_recent_sale_date
    queryAthena("""drop table if exists data_science.stage_attom9;""")
    queryAthena("""
    create table data_science.stage_attom9 as
    select distinct
        case when t2.property_id is not null then 'forge_'||t2.list_id_overlap else t1.list_id end as list_id
        , t1.property_id
        , t1.apn
        , t1.titan_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_unit_type
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.fips_county_cd
        , t1.cbsa_id
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , cast(t1.year_built as integer) as year_built
        , cast(t1.sqft as integer) as sqft
        , case when t1.status = 'Contingent' then 'Active' else t1.status end as status
        , t1.status_date
        , t1.price
    from data_science.stage_attom8 as t1
    left join data_science.stage_attom_overlaps as t2
        on t1.property_id = t2.property_id
            and t1.status_date between t2.list_start_date and t2.list_end_date;
    """)
    print("attom9 completed")
    
    # Remove all status records after the first sale
    # Remove all status changes after the date of 'Sale', remove any 'Delisted' status changes on the date of 'Sale' for all list_ids
    queryAthena("""drop table if exists data_science.stage_attom10;""")
    queryAthena("""
    create table data_science.stage_attom10 as
    select distinct t1.* from data_science.stage_attom9 as t1
    inner join (
        select
            list_id
            , property_id
            , case when sum(case when status = 'Sale' then 1 else 0 end) >= 1 then 1 else 0 end as sale_ind
        from data_science.stage_attom9
        group by 1, 2
        having sum(case when status = 'Sale' then 1 else 0 end) = 0
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
    union all
    select t1.* from data_science.stage_attom9 as t1
    inner join (
        select
            list_id
            , property_id
            , min(status_date) as sale_date
        from data_science.stage_attom9
        where status = 'Sale'
        group by 1, 2
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
            and ((t1.status_date <= t2.sale_date and t1.status != 'Delisted')
                or (t1.status_date < t2.sale_date and t1.status = 'Delisted'));
    """)
    print("attom10 completed")
    
    # Get uniform values per each property_id - list_id for these columns: year_built, sqft, bedrooms, property_type,
    #  latitude, longitude, full_baths, half_baths
    queryAthena("""drop table if exists data_science.stage_attom11;""")
    queryAthena("""
    create table data_science.stage_attom11 as
    with prop_list_uniform as (
        select
            property_id
            , list_id
            , sum(case when property_type = 'Multi Unit' then 1 else 0 end) as multi_unit_cnt
            , round(avg(latitude), 6) as latitude
            , round(avg(longitude), 6) as longitude
            , cast(approx_percentile(bedrooms, 0.5) as integer) as bedrooms
            , cast(approx_percentile(full_baths, 0.5) as integer) as full_baths
            , cast(approx_percentile(half_baths, 0.5) as integer) as half_baths
            , cast(approx_percentile(sqft, 0.5) as integer) as sqft
            , max(year_built) as year_built
        from data_science.stage_attom10
        group by 1, 2)
    select distinct
        t1.list_id
        , t1.property_id
        , t1.apn
        , t1.titan_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_unit_type
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.fips_county_cd
        , t1.cbsa_id
        , case when t2.multi_unit_cnt >= 1 then 'Multi Unit' else 'Single Family' end as property_type
        , t2.latitude
        , t2.longitude
        , t1.market
        , t2.bedrooms
        , t2.full_baths
        , t2.half_baths
        , t2.year_built
        , t2.sqft
        , t1.status
        , case when t1.status = 'Sale' then 2
            when t1.status = 'Pending' then 2
            when t1.status = 'Active' then 3
            when t1.status = 'Delisted' then 4
            end as status_rank
        , t1.status_date
        , t1.price
    from data_science.stage_attom10 as t1
    inner join prop_list_uniform as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id;
    """)
    print("attom11 completed")
    
    # Clean status so only one status per day unless sale and pending are on the same day
    # Clean price so only one price per day per status
    queryAthena("""drop table if exists data_science.stage_attom12;""")
    queryAthena("""
    create table data_science.stage_attom12 as
    with statuses as (
        select
            property_id
            , list_id
            , status_date
            , min(status_rank) as status_rank
        from data_science.stage_attom11
        group by 1, 2, 3
        ),
    prices as (
        select
            property_id
            , list_id
            , status_date
            , status
            , cast(approx_percentile(price, 0.5) as integer) as price
        from data_science.stage_attom11
        group by 1, 2, 3, 4
        )
    select distinct
        t1.list_id
        , t1.property_id
        , t1.apn
        , t1.titan_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_unit_type
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.fips_county_cd
        , t1.cbsa_id
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t1.status
        , t1.status_rank
        , t1.status_date
        , t3.price
    from data_science.stage_attom11 as t1
    inner join statuses as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id
            and t1.status_date = t2.status_date
            and t1.status_rank = t2.status_rank
    inner join prices as t3
        on t1.property_id = t3.property_id
            and t1.list_id = t3.list_id
            and t1.status = t3.status
            and t1.status_date = t3.status_date;
    """)
    print("attom12 completed")
    
    # Remove all status changes after the date of 'Sale', remove any 'Delisted' status changes on the date of 'Sale' for all list_ids
    queryAthena("""drop table if exists data_science.stage_attom13;""")
    queryAthena("""
    create table data_science.stage_attom13 as
    select distinct t1.* from data_science.stage_attom12 as t1
    inner join (
        select
            list_id
            , property_id
            , case when sum(case when status = 'Sale' then 1 else 0 end) >= 1 then 1 else 0 end as sale_ind
        from data_science.stage_attom12
        group by 1, 2
        having sum(case when status = 'Sale' then 1 else 0 end) = 0
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
    union all
    select t1.* from data_science.stage_attom12 as t1
    inner join (
        select
            list_id
            , property_id
            , min(status_date) as sale_date
        from data_science.stage_attom12
        where status = 'Sale'
        group by 1, 2
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
            and ((t1.status_date <= t2.sale_date and t1.status != 'Delisted')
                or (t1.status_date < t2.sale_date and t1.status = 'Delisted'));
    """)
    print("attom13 completed")
    
    # Remove excess delisted records
    queryAthena("""drop table if exists data_science.stage_attom14;""")
    queryAthena("""
    create table data_science.stage_attom14 as
    select a1.* from data_science.stage_attom13 as a1
    left join (
        select
            t1.*
            , t2.status as prev_status
        from (
            select
                t1.property_id
                , t1.list_id
                , t1.status
                , t1.status_date
                , max(t2.status_date) as prev_status_date
            from data_science.stage_attom13 as t1
            inner join data_science.stage_attom13 as t2
                on t1.list_id = t2.list_id
                    and t1.property_id = t2.property_id
                    and t2.status_date < t1.status_date
            where t1.status = 'Delisted'
            group by 1, 2, 3, 4
            ) as t1
        inner join data_science.stage_attom13 as t2
            on t1.list_id = t2.list_id
                and t1.property_id = t2.property_id
                and t1.prev_status_date = t2.status_date
        ) as a2
        on a1.list_id = a2.list_id
            and a1.property_id = a2.property_id
            and a1.status = a2.status
            and a1.status_date = a2.status_date
            and a2.prev_status = 'Delisted'
    where a2.property_id is null;
    """)
    print("attom14 completed")
    
    # Conform data to final look
    queryAthena("""drop table if exists data_science.stage_attom;""")
    queryAthena("""
    create table data_science.stage_attom as
    with list_init as (
        select
            property_id
            , list_id
            , min(status_date) as list_date_init
        from data_science.stage_attom14
        where status = 'Active'
        group by 1, 2
        ),
    next_status as (
        select
            t1.property_id
            , t1.list_id
            , t1.status_date
            , min(t2.status_date) as next_status_date
        from data_science.stage_attom14 as t1
        inner join data_science.stage_attom14 as t2
            on t1.property_id = t2.property_id
                and t1.list_id = t2.list_id
                and t1.status_date < t2.status_date
        group by 1, 2, 3
        ),
    list_price as (
        select
            a1.property_id
            , a1.list_id
            , a1.status
            , a1.status_date
            , cast(approx_percentile(a2.price, 0.5) as integer) as list_price
        from (
            select
                t1.property_id
                , t1.list_id
                , t1.status
                , t1.status_date
                , max(t2.status_date) as prev_status_date
            from data_science.stage_attom14 as t1
            inner join data_science.stage_attom14 as t2
                on t1.property_id = t2.property_id
                    and t1.list_id = t2.list_id
                    and t1.status_date > t2.status_date
            where t1.status = 'Sale'
            group by 1, 2, 3, 4
            ) as a1
        inner join data_science.stage_attom14 as a2
            on a1.property_id = a2.property_id
                and a1.list_id = a2.list_id
                and a1.prev_status_date = a2.status_date
        group by 1, 2, 3, 4
        )
    select distinct
        t1.list_id
        , t1.property_id
        , t1.apn
        , t1.titan_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.fips_county_cd
        , t1.cbsa_id
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t2.list_date_init as list_date
        , t1.status_date
        , lower(t1.status) as status
        , case when t3.next_status_date is null then date('2099-12-31') else t3.next_status_date end as next_status_date
        , case when t1.status = 'Active' and t1.status_date = t2.list_date_init then 1 else 0 end as initial_listing_ind
        , case when t1.status = 'Sale' then 1 else 0 end as sale_ind
        , case when t1.status = 'Pending' then 1 else 0 end as pending_ind
        , case when t1.status = 'Active' then 1 else 0 end as active_ind
        , case when t1.status = 'Delisted' then 1 else 0 end as delisted_ind
        , case when t1.status = 'Sale' then t1.price end as sale_price
        , case when t1.status = 'Sale' then t4.list_price else t1.price end as list_price
        , case when t1.status = 'Pending' then date_diff('day', t2.list_date_init, t1.status_date) end as dom
    from data_science.stage_attom14 as t1
    inner join list_init as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id
    left join next_status as t3
        on t1.property_id = t3.property_id
            and t1.list_id = t3.list_id
            and t1.status_date = t3.status_date
    left join list_price as t4
        on t1.property_id = t4.property_id
            and t1.list_id = t4.list_id
            and t1.status = t4.status
            and t1.status_date = t4.status_date;
    """)
    print("stage_attom completed")


def get_rets_and_final_data():
    # Create state_lookup table with state name and state abbreviation
    queryAthena("""drop table if exists data_science.state_lookup;""")
    queryAthena("""
    create table data_science.state_lookup as
    select
        'dc' as state
        , 'district of columbia' as state_name;""")
    
    queryAthena("""
    insert into data_science.state_lookup values
        ('al', 'alabama'),
        ('ak', 'alaska'),
        ('az', 'arizona'),
        ('ar', 'arkansas'),
        ('ca', 'california'),
        ('co', 'colorado'),
        ('ct', 'connecticut'),
        ('de', 'delaware'),
        ('fl', 'florida'),
        ('ga', 'georgia'),
        ('hi', 'hawaii'),
        ('id', 'idaho'),
        ('il', 'illinois'),
        ('in', 'indiana'),
        ('ia', 'iowa'),
        ('ks', 'kansas'),
        ('ky', 'kentucky'),
        ('la', 'louisiana'),
        ('me', 'maine'),
        ('md', 'maryland'),
        ('ma', 'massachusetts'),
        ('mi', 'michigan'),
        ('mn', 'minnesota'),
        ('ms', 'mississippi'),
        ('mo', 'missouri'),
        ('mt', 'montana'),
        ('ne', 'nebraska'),
        ('nv', 'nevada'),
        ('nh', 'new hampshire'),
        ('nj', 'new jersey'),
        ('nm', 'new mexico'),
        ('ny', 'new york'),
        ('nc', 'north carolina'),
        ('nd', 'north dakota'),
        ('oh', 'ohio'),
        ('ok', 'oklahoma'),
        ('or', 'oregon'),
        ('pa', 'pennsylvania'),
        ('ri', 'rhode island'),
        ('sc', 'south carolina'),
        ('sd', 'south dakota'),
        ('tn', 'tennessee'),
        ('tx', 'texas'),
        ('ut', 'utah'),
        ('vt', 'vermont'),
        ('va', 'virginia'),
        ('wa', 'washington'),
        ('wv', 'west virginia'),
        ('wi', 'wisconsin'),
        ('wy', 'wyoming');""")
    
    # Get base data
    queryAthena("""drop table if exists data_science.stage_rets1;""")
    queryAthena("""
    create table data_science.stage_rets1 as
    select distinct
        t1.list_id
        , case when regexp_replace(t1.apn, '[^2-9a-z]+') = '' then 'no_apn'
            when regexp_replace(t1.apn, '[^1-8a-z]+') = '' then 'no_apn'
            when regexp_replace(t1.apn, '[^1-9]+') = '' then 'no_apn'
            when length(t1.apn) <= 5 then 'no_apn'
            when length(regexp_replace(t1.apn, '[^1-9a-z]+]')) <= 3 then 'no_apn'
            when t1.apn like '%not%' then 'no_apn'
            when t1.apn like '123%' then 'no_apn'
            when t1.apn like '0123%' then 'no_apn'
            when t1.apn like '%acre%' then 'no_apn'
            when t1.apn in ('unknown', 'multiple', 'unavailable') then 'no_apn'
            when t1.apn is null then 'no_apn'
            else t1.apn end as apn
        , case when t1.titan_id is null then 'no_titan_id' else t1.titan_id end as titan_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , case when substring(t1.address_street_name, 1, length(t1.address_street_name)) = t1.address_street_number
                then split_part(split_part(t1.address_street_name, t1.address_street_number, 2), ',', 1)
            else split_part(t1.address_street_name, ',', 1)
            end as address_street_name
        , t1.address_street_suffix
        , t1.address_dir_suffix
        , case when t1.address_unit_number is null then '' else t1.address_unit_number end as address_unit_number
        , t1.address_city
        , case when t2.state is not null then t2.state else t1.address_state end as address_state
        , t1.address_zip
        , t1.address_county
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.property_type
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.sqft
        , t1.year_built
        , t1.status
        , t1.status_date
        , case when t1.list_date < date_add('month', -96, date_trunc('month', current_date)) then null else t1.list_date end as list_date
        , case when t1.pending_date < date_add('month', -96, date_trunc('month', current_date)) then null else t1.pending_date end as pending_date
        , case when t1.sale_date < date_add('month', -96, date_trunc('month', current_date)) then null else t1.sale_date end as sale_date
        , t1.original_list_price
        , t1.list_price
        , t1.sale_price
    from (
        select distinct
            (case when mls_id is null then '_' else mls_id end)||' '||mls_name||' '||mls_unique_id as list_id
            , regexp_replace(lower(apn), '[^0-9a-z]+') as apn
            , titan_id
            , ltrim(regexp_replace(lower(address_components_street_number), '[^0-9]+'), '0') as address_street_number
            , case when lower(address_components_street_predirection) in ('n', 's', 'w', 'e', 'nw', 'ne', 'sw', 'se')
                then lower(address_components_street_predirection) end as address_dir_prefix
            , replace(replace(replace(replace(replace(replace(replace(replace(replace(regexp_replace(lower(address_components_street_name), '[^a-z0-9 ]+'),
                      'drive', 'dr'),
                      'street', 'st'),
                      'lane', 'ln'),
                      'avenue', 'ave'),
                      'road', 'rd'),
                      'court', 'ct'),
                      'circle', 'cir'),
                      'boulevard', 'blvd'),
                      'place', 'pl') as address_street_name
            , replace(replace(replace(replace(replace(replace(replace(replace(replace(regexp_replace(lower(address_components_street_suffix), '[^a-z]+'),
                      'drive', 'dr'),
                      'street', 'st'),
                      'lane', 'ln'),
                      'avenue', 'ave'),
                      'road', 'rd'),
                      'court', 'ct'),
                      'circle', 'cir'),
                      'boulevard', 'blvd'),
                      'place', 'pl') as address_street_suffix
            , case when lower(address_components_street_postdirection) in ('n', 's', 'w', 'e', 'nw', 'ne', 'sw', 'se')
                then lower(address_components_street_postdirection) end as address_dir_suffix
            , case when substring(regexp_replace(lower(address_components_secondary_number), '[^a-z0-9]+'), 1, 3) = 'apt'
                    then ltrim(regexp_replace(lower(address_components_secondary_number), '[^a-z0-9]+'), 'apt')
                when substring(address_components_secondary_number, 1, 1) = '#'
                    then regexp_replace(lower(address_components_secondary_number), '[^a-z0-9]+')
                when substring(regexp_replace(lower(address_components_secondary_number), '[^a-z0-9]+'), 1, 4) = 'unit'
                    then ltrim(regexp_replace(lower(address_components_secondary_number), '[^a-z0-9]+'), 'unit')
                else regexp_replace(lower(address_components_secondary_number), '[^a-z0-9]+') end as address_unit_number
            , regexp_replace(lower(address_components_city), '[^a-z ]+') as address_city
            , case when address_components_state is not null then regexp_replace(lower(address_components_state), '[^a-z ]+')
                else substring(mls_name, 1, 2) end as address_state
            , case when address_components_zipcode = regexp_replace(address_components_zipcode, '[^0-9-]+')
                then lpad(split_part(address_components_zipcode, '-', 1), 5, '0') end as address_zip
            , split_part(split_part(regexp_replace(lower(case when lower(address_components_county) like '%other%' or lower(address_components_county) like '%out of%' then null else address_components_county end), '[^a-z-]+'), ',', 1), 'county', 1) as address_county
            , knock_latitude as latitude
            , knock_longitude as longitude
            , market
            , bedrooms
            , baths_full as full_baths
            , case when baths_half is null then 0 else baths_half end +
                case when baths_three_quarter is null then 0 else baths_three_quarter end +
                case when baths_quarter is null then 0 else baths_quarter end as half_baths
            , case when total_livable_area_sqft > 100 then total_livable_area_sqft end as sqft
            , year_built
            , case when lower(status) = 'active' then 'active'
                when lower(status) in ('active under contract', 'pending') then 'pending'
                when lower(status) in ('canceled', 'cancelled', 'deleted', 'expired', 'withdrawn') then 'delisted'
                when lower(status) = 'closed' and lower(status_mls) = 'sold' then 'sale'
                when lower(status) = 'closed' and lower(status_mls) != 'sold' then 'delisted'
                when lower(status) = 'other' and lower(status_mls) in ('canceled', 'cancelled', 'delete', 'withdrawn/expired') then 'delisted'
                when lower(status) = 'other' and lower(status_mls) = 'contingent' then 'active'
                end as status
            , date(status_change_timestamp) as status_date
            , list_date
            , date(pending_date) as pending_date
            , sold_date as sale_date
            , cast((case when list_price > 99999999 then null when list_price < 50000 then null else list_price end) as integer) as list_price
            , cast((case when sold_price > 99999999 then null when sold_price < 50000 then null else sold_price end) as integer) as sale_price
            , cast((case when original_list_price > 99999999 then null when original_list_price < 50000 then null else original_list_price end) as integer) as original_list_price
            , case when lower(property_type) like '%attached%' then 'Multi Unit'
                when lower(property_type) like 'condo%' then 'Multi Unit'
                when lower(property_type) like 'townho%' then 'Multi Unit'
                when lower(property_type) like 'multi%' then 'Multi Unit'
                when lower(property_type) like '%plex%' then 'Multi Unit'
                else 'Single Family' end as property_type
        from sanvil.rets_schematized_geocoded
        where address_components_street_number is not null
            and address_components_street_number != regexp_replace(address_components_street_number, '[^0]+')
            and address_components_street_name is not null
            and lower(address) not like '%unknown%'
            and regexp_replace(lower(address_components_street_name), '[^a-z0-9]+') not like '%pobox%'
            and substring(address_components_street_number, 1, 1) in ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9')
            and lower(address_components_street_name) not like '%confidential%'
            and date(status_change_timestamp) >= date_add('month', -96, date_trunc('month', current_date))
            and date(status_change_timestamp) <= current_date
        ) as t1
    left join data_science.state_lookup as t2
        on t1.address_state = t2.state_name
    where t1.address_zip is not null
        and t1.address_street_number != ''
        and t1.address_street_number is not null;""")
    print("rets1 completed")
    
    # Remove all apns found in stage_attom that have overlapping list dates
    queryAthena("""drop table if exists data_science.stage_rets2;""")
    queryAthena("""
    create table data_science.stage_rets2 as
    with apn_data as (
        select distinct t1.* from (
            select
                apn
                , address_street_number
                , address_zip
                , list_id
                , least(min(case when status_date is null then current_date else status_date end),
                        min(case when list_date is null then current_date else list_date end),
                        min(case when pending_date is null then current_date else pending_date end),
                        min(case when sale_date is null then current_date else sale_date end)) as min_date
                , greatest(max(case when status_date is null then date('2000-01-01') else status_date end),
                           max(case when list_date is null then date('2000-01-01') else list_date end),
                           max(case when pending_date is null then date('2000-01-01') else pending_date end),
                           max(case when sale_date is null then date('2000-01-01') else sale_date end)) as max_date
            from data_science.stage_rets1
            where apn is not null
            group by 1, 2, 3, 4
            ) as t1
        left join (
            select
                apn
                , address_street_number
                , address_zip
                , list_id
                , min(status_date) as min_date
                , max(status_date) as max_date
            from data_science.stage_attom
            where apn is not null
            group by 1, 2, 3, 4
            ) as t2
            on t1.apn = t2.apn
                and t1.address_zip = t2.address_zip
                and t1.address_street_number = t2.address_street_number
                and ((t2.min_date >= t1.min_date
                      and t2.min_date <= t1.max_date)
                    or (t2.max_date >= t1.min_date
                      and t2.max_date <= t1.max_date))
        where t2.apn is null)
    select distinct t1.* from data_science.stage_rets1 as t1
    inner join apn_data as t2
        on t1.apn = t2.apn
            and t1.address_street_number = t2.address_street_number
            and t1.address_zip = t2.address_zip
            and t1.list_id = t2.list_id;""")
    print("rets2 completed")
    
    # Remove all titan_ids found in stage_attom that have overlapping list dates
    queryAthena("""drop table if exists data_science.stage_rets3;""")
    queryAthena("""
    create table data_science.stage_rets3 as
    with titan_data as (
        select distinct t1.* from (
            select
                titan_id
                , address_street_number
                , address_zip
                , list_id
                , least(min(case when status_date is null then current_date else status_date end),
                        min(case when list_date is null then current_date else list_date end),
                        min(case when pending_date is null then current_date else pending_date end),
                        min(case when sale_date is null then current_date else sale_date end)) as min_date
                , greatest(max(case when status_date is null then date('2000-01-01') else status_date end),
                           max(case when list_date is null then date('2000-01-01') else list_date end),
                           max(case when pending_date is null then date('2000-01-01') else pending_date end),
                           max(case when sale_date is null then date('2000-01-01') else sale_date end)) as max_date
            from data_science.stage_rets2
            where apn is not null
            group by 1, 2, 3, 4
            ) as t1
        left join (
            select
                titan_id
                , address_street_number
                , address_zip
                , list_id
                , min(status_date) as min_date
                , max(status_date) as max_date
            from data_science.stage_attom
            where apn is not null
            group by 1, 2, 3, 4
            ) as t2
            on t1.titan_id = t2.titan_id
                and t1.address_zip = t2.address_zip
                and t1.address_street_number = t2.address_street_number
                and ((t2.min_date >= t1.min_date
                      and t2.min_date <= t1.max_date)
                    or (t2.max_date >= t1.min_date
                      and t2.max_date <= t1.max_date))
        where t2.titan_id is null)
    select distinct t1.* from data_science.stage_rets2 as t1
    inner join titan_data as t2
        on t1.titan_id = t2.titan_id
            and t1.address_street_number = t2.address_street_number
            and t1.address_zip = t2.address_zip
            and t1.list_id = t2.list_id;""")
    print("rets3 completed")
    
    # Remove all addresses found in stage_attom that have overlapping list dates
    # Define property ID
    queryAthena("""drop table if exists data_science.stage_rets4;""")
    queryAthena("""
    create table data_science.stage_rets4 as
    with address_data as (
        select distinct t1.* from (
            select
                address_street_number
                , address_street_name
                , address_unit_number
                , address_zip
                , list_id
                , least(min(case when status_date is null then current_date else status_date end),
                        min(case when list_date is null then current_date else list_date end),
                        min(case when pending_date is null then current_date else pending_date end),
                        min(case when sale_date is null then current_date else sale_date end)) as min_date
                , greatest(max(case when status_date is null then date('2000-01-01') else status_date end),
                           max(case when list_date is null then date('2000-01-01') else list_date end),
                           max(case when pending_date is null then date('2000-01-01') else pending_date end),
                           max(case when sale_date is null then date('2000-01-01') else sale_date end)) as max_date
            from data_science.stage_rets3
            group by 1, 2, 3, 4, 5
            ) as t1
        left join (
            select
                address_street_number
                , address_street_name
                , case when address_unit_number is null then '' else address_unit_number end as address_unit_number
                , address_zip
                , list_id
                , min(status_date) as min_date
                , max(status_date) as max_date
            from data_science.stage_attom
            where apn is not null
            group by 1, 2, 3, 4, 5
            ) as t2
            on t1.address_street_name = t2.address_street_name
                and t1.address_zip = t2.address_zip
                and t1.address_street_number = t2.address_street_number
                and t1.address_unit_number = t2.address_unit_number
                and ((t2.min_date >= t1.min_date
                      and t2.min_date <= t1.max_date)
                    or (t2.max_date >= t1.min_date
                      and t2.max_date <= t1.max_date)))
    select distinct
        t1.address_street_number||t1.address_street_name||(case when t1.address_unit_number is null then '' else t1.address_unit_number end)||t1.address_zip as property_id
        , t1.list_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_street_suffix
        , t1.address_dir_suffix
        , t1.address_unit_number
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.property_type
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.sqft
        , t1.year_built
        , t1.status
        , t1.status_date
        , t1.list_date
        , t1.pending_date
        , t1.sale_date
        , t1.original_list_price
        , t1.list_price
        , t1.sale_price
    from data_science.stage_rets3 as t1
    inner join address_data as t2
        on t1.address_street_name = t2.address_street_name
            and t1.address_street_number = t2.address_street_number
            and t1.address_unit_number = t2.address_unit_number
            and t1.address_zip = t2.address_zip
            and t1.list_id = t2.list_id;""")
    print("rets4 completed")
    
    # Reshape data so that sale_date and pending_date become individual records of a list_id
    # Remove records that come after the first sale date and after the current date
    queryAthena("""drop table if exists data_science.stage_rets5;""")
    queryAthena("""
    create table data_science.stage_rets5 as
    with status_data as (
        select distinct
            property_id
            , list_id
            , address_street_number
            , address_dir_prefix
            , address_street_name
            , address_street_suffix
            , address_dir_suffix
            , address_unit_number
            , address_city
            , address_state
            , address_zip
            , address_county
            , latitude
            , longitude
            , market
            , property_type
            , bedrooms
            , full_baths
            , half_baths
            , sqft
            , year_built
            , 'active' as status
            , list_date as status_date
            , original_list_price
            , list_price
            , sale_price
        from data_science.stage_rets4
        where sale_date is not null
        union all
        select distinct
            property_id
            , list_id
            , address_street_number
            , address_dir_prefix
            , address_street_name
            , address_street_suffix
            , address_dir_suffix
            , address_unit_number
            , address_city
            , address_state
            , address_zip
            , address_county
            , latitude
            , longitude
            , market
            , property_type
            , bedrooms
            , full_baths
            , half_baths
            , sqft
            , year_built
            , 'sale' as status
            , sale_date as status_date
            , original_list_price
            , list_price
            , sale_price
        from data_science.stage_rets4
        where sale_date is not null
        union all
        select distinct
            property_id
            , list_id
            , address_street_number
            , address_dir_prefix
            , address_street_name
            , address_street_suffix
            , address_dir_suffix
            , address_unit_number
            , address_city
            , address_state
            , address_zip
            , address_county
            , latitude
            , longitude
            , market
            , property_type
            , bedrooms
            , full_baths
            , half_baths
            , sqft
            , year_built
            , 'pending' as status
            , pending_date as status_date
            , original_list_price
            , list_price
            , sale_price
        from data_science.stage_rets4
        where pending_date is not null
        union all
        select distinct
            property_id
            , list_id
            , address_street_number
            , address_dir_prefix
            , address_street_name
            , address_street_suffix
            , address_dir_suffix
            , address_unit_number
            , address_city
            , address_state
            , address_zip
            , address_county
            , latitude
            , longitude
            , market
            , property_type
            , bedrooms
            , full_baths
            , half_baths
            , sqft
            , year_built
            , status
            , status_date
            , original_list_price
            , list_price
            , sale_price
        from data_science.stage_rets4
        where status is not null
            and status_date is not null)
    select t1.* from status_data as t1
    inner join (
        select
            property_id
            , list_id
            , min(case when status = 'sale' then status_date else current_date end) as status_date
        from status_data
        where status_date <= current_date
        group by 1, 2
        ) as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id
            and t1.status_date <= t2.status_date;""")
    print("rets5 completed")
    
    # Remove any 'Delisted' status changes on the date of 'Sale' for all list_ids
    queryAthena("""drop table if exists data_science.stage_rets6;""")
    queryAthena("""
    create table data_science.stage_rets6 as
    select distinct t1.* from data_science.stage_rets5 as t1
    left join (
        select distinct
            list_id
            , property_id
            , status_date
        from data_science.stage_rets5
        where status = 'sale'
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
            and t1.status_date = t2.status_date
            and t1.status = 'delisted'
    where t2.property_id is null;""")
    print("rets6 completed")
    
    # Keep list records that come after the first active list record
    queryAthena("""drop table if exists data_science.stage_rets7;""")
    queryAthena("""
    create table data_science.stage_rets7 as
    select t1.* from data_science.stage_rets6 as t1
    inner join (
        select
            property_id
            , list_id
            , min(status_date) as status_date
        from data_science.stage_rets6
        where status = 'active'
        group by 1, 2
        ) as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id
            and t1.status_date >= t2.status_date;""")
    print("rets7 completed")
    
    # Add delist record to listings that have been active for 1 year with no status change
    queryAthena("""drop table if exists data_science.stage_rets8;""")
    queryAthena("""
    create table data_science.stage_rets8 as
    with statuses as (
        select
            property_id
            , list_id
            , max(status_date) as last_status_date
            , max(case when status = 'active' then status_date end) as last_active_date
            , max(case when status = 'delisted' then status_date end) as last_delisted_date
            , max(case when status = 'pending' then status_date end) as last_pending_date
            , max(case when status = 'sale' then status_date end) as last_sale_date
        from data_science.stage_rets7
        group by 1, 2
        )
    select * from data_science.stage_rets7
    union all
    select distinct
        t1.property_id
        , t1.list_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.property_type
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.sqft
        , t1.year_built
        , 'delisted' as status
        , date_add('year', 1, t1.status_date) as status_date
        , t1.original_list_price
        , t1.list_price
        , t1.sale_price
    from data_science.stage_rets7 as t1
    inner join (
        select * from statuses
        where last_active_date = last_status_date
            and last_active_date < date_add('year', -1, current_date)
            and (last_delisted_date is null or last_delisted_date < last_status_date)
            and (last_pending_date is null or last_pending_date < last_status_date)
            and last_sale_date is null
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id;""")
    print("rets8 completed")
    
    # Add Pending status record to listings with Sale status and no Pending status
    queryAthena("""drop table if exists data_science.stage_rets9;""")
    queryAthena("""
    create table data_science.stage_rets9 as
    select * from data_science.stage_rets8
    union all
    select distinct
        t1.property_id
        , t1.list_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.property_type
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.sqft
        , t1.year_built
        , 'pending' as status
        , min(t1.status_date) as status_date
        , t1.original_list_price
        , t1.list_price
        , t1.sale_price
    from data_science.stage_rets8 as t1
    inner join (
        select
            list_id
            , property_id
            , case when sum(case when status = 'pending' then 1 else 0 end) >= 1 then 1 else 0 end as pending_ind
            , case when sum(case when status = 'sale' then 1 else 0 end) >= 1 then 1 else 0 end as sale_ind
        from data_science.stage_rets8
        group by 1, 2
        having sum(case when status = 'pending' then 1 else 0 end) = 0
            and sum(case when status = 'sale' then 1 else 0 end) >= 1
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
    where t1.status = 'sale'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 24, 25, 26;""")
    print("rets9 completed")
    
    # Remove excess delisted records
    queryAthena("""drop table if exists data_science.stage_rets10;""")
    queryAthena("""
    create table data_science.stage_rets10 as
    select distinct
        a1.property_id
        , a1.list_id
        , a1.address_street_number
        , a1.address_dir_prefix
        , a1.address_street_name
        , a1.address_dir_suffix
        , a1.address_street_suffix
        , case when a1.address_unit_number = '' then null else a1.address_unit_number end as address_unit_number
        , a1.address_city
        , a1.address_state
        , a1.address_zip
        , a1.address_county
        , a1.latitude
        , a1.longitude
        , a1.market
        , case when a1.address_unit_number != '' and a1.address_unit_number is not null then 'Multi Unit' else a1.property_type end as property_type
        , a1.bedrooms
        , a1.full_baths
        , a1.half_baths
        , a1.sqft
        , a1.year_built
        , a1.status
        , a1.status_date
        , a1.original_list_price
        , a1.list_price
        , a1.sale_price
    from data_science.stage_rets9 as a1
    left join (
        select
            t1.*
            , t2.status as prev_status
        from (
            select
                t1.property_id
                , t1.list_id
                , t1.status
                , t1.status_date
                , max(t2.status_date) as prev_status_date
            from data_science.stage_rets9 as t1
            inner join data_science.stage_rets9 as t2
                on t1.list_id = t2.list_id
                    and t1.property_id = t2.property_id
                    and t2.status_date < t1.status_date
            where t1.status = 'delisted'
            group by 1, 2, 3, 4
            ) as t1
        inner join data_science.stage_rets9 as t2
            on t1.list_id = t2.list_id
                and t1.property_id = t2.property_id
                and t1.prev_status_date = t2.status_date
        ) as a2
        on a1.list_id = a2.list_id
            and a1.property_id = a2.property_id
            and a1.status = a2.status
            and a1.status_date = a2.status_date
            and a2.prev_status = 'delisted'
    where a2.property_id is null;""")
    print("rets10 completed")
    
    # Get initial overlapping listings
    # If listing does not have a Sale or Delisted status, then list_end_date should be the last status_date plus 6 months
    queryAthena("""drop table if exists data_science.stage_rets_overlaps1;""")
    queryAthena("""
    create table data_science.stage_rets_overlaps1 as
    with listings as (
        select distinct
            t1.list_id
            , t1.property_id
            , t2.list_start_date
            , case when sum(case when t1.status in ('sale', 'delisted') then 1 else 0 end) >= 1
                then t2.list_end_date else date_add('month', 6, t2.list_end_date) end as list_end_date
            , t2.list_end_date as list_end_date_init
        from data_science.stage_rets10 as t1
        inner join (
            select
                list_id
                , property_id
                , date(min(status_date)) as list_start_date
                , date(max(status_date)) as list_end_date
            from data_science.stage_rets10
            group by 1, 2
            ) as t2
            on t1.list_id = t2.list_id
                and t1.property_id = t2.property_id
                and t1.status_date = t2.list_end_date
        group by 1, 2, 3, 5)
    select distinct
        a1.property_id
        , a1.list_id
        , a2.list_id as list_id_overlap1
        , a1.list_start_date
        , a2.list_start_date as list_start_date_overlap1
        , a1.list_end_date
        , a2.list_end_date as list_end_date_overlap1
        , greatest(a1.list_end_date, a2.list_end_date) as list_end_date_acme
    from listings as a1
    inner join listings as a2
        on a1.property_id = a2.property_id
            and a1.list_id != a2.list_id
            and a1.list_start_date >= a2.list_start_date and a1.list_start_date <= a2.list_end_date;
    """)

    
    # Get records with min and max list_start_date and list_end_date_acme
    # Remove records that are subsumed within another record
    queryAthena("""drop table if exists data_science.stage_rets_overlaps2;""")
    queryAthena("""
    create table data_science.stage_rets_overlaps2 as
    with start_ends as (
        select
            property_id
            , list_start_date
            , list_end_date_acme as list_end_date
            , row_number() over (partition by property_id order by property_id, list_start_date) as list_id
        from (
            select
                property_id
                , min(list_start_date) as list_start_date
                , list_end_date_acme
            from (
                select
                    property_id
                    , list_start_date
                    , max(list_end_date_acme) as list_end_date_acme
                from data_science.stage_rets_overlaps1
                group by 1, 2)
            group by 1, 3)
        )
    select t1.* from start_ends as t1
    left join start_ends as t2
        on t1.property_id = t2.property_id
            and t1.list_start_date < t2.list_start_date
            and t1.list_end_date > t2.list_end_date
    where t2.property_id is null;""")
    
    # Delineate list_ids that are have a succeeding overlapping list_id and list_ids that have a preceeding overlapping list_id and those with both
    queryAthena("""drop table if exists data_science.stage_rets_overlaps3;""")
    queryAthena("""
    create table data_science.stage_rets_overlaps3 as
    select
        t1.property_id
        , t1.list_start_date
        , t1.list_end_date
        , case when sum(case when t2.property_id is not null then 1 else 0 end) >= 1 then 1 else 0 end as has_succeeding_ind
        , case when sum(case when t3.property_id is not null then 1 else 0 end) >= 1 then 1 else 0 end as has_preceeding_ind
    from data_science.stage_rets_overlaps2 as t1
    left join data_science.stage_rets_overlaps2 as t2
        on t1.property_id = t2.property_id
            and t2.list_start_date > t1.list_start_date
            and t2.list_start_date <= t1.list_end_date
    left join data_science.stage_rets_overlaps2 as t3
        on t1.property_id = t3.property_id
            and t1.list_start_date > t3.list_start_date
            and t1.list_start_date <= t3.list_end_date
    group by 1, 2, 3;""")
    
    queryAthena("""drop table if exists data_science.stage_rets_overlaps;""")
    queryAthena("""
    create table data_science.stage_rets_overlaps as
    select
        property_id
        , list_start_date
        , list_end_date
        , cast(rank() over (partition by property_id order by property_id, list_end_date) as varchar(10)) as list_id_overlap
    from (
        select distinct
            property_id
            , list_start_date
            , list_end_date
        from data_science.stage_rets_overlaps3
        where has_succeeding_ind = 0
            and has_preceeding_ind = 0
        union all
        select
            t1.property_id
            , t1.list_start_date
            , min(t2.list_end_date) as list_end_date
        from data_science.stage_rets_overlaps3 as t1
        inner join data_science.stage_rets_overlaps3 as t2
            on t1.property_id = t2.property_id
                and t1.list_start_date < t2.list_start_date
        where t1.has_succeeding_ind = 1
            and t1.has_preceeding_ind = 0
            and t2.has_succeeding_ind = 0
            and t2.has_preceeding_ind = 1
        group by 1, 2);""")
    print("rets_overlaps completed")
    
    # Relabel list_ids that overlap
    queryAthena("""drop table if exists data_science.stage_rets11;""")
    queryAthena("""
    create table data_science.stage_rets11 as
    select distinct
        t1.property_id
        , case when t2.property_id is not null then 'forge_'||t2.list_id_overlap else t1.list_id end as list_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , cast(t1.sqft as integer) as sqft
        , cast(t1.year_built as integer) as year_built
        , t1.status
        , t1.status_date
        , t1.original_list_price
        , t1.list_price
        , t1.sale_price
    from data_science.stage_rets10 as t1
    left join data_science.stage_rets_overlaps as t2
        on t1.property_id = t2.property_id
            and t1.status_date between t2.list_start_date and t2.list_end_date;""")
    print("rets11 completed")
    
    # Remove all status records after the first sale
    # Remove all status changes after the date of 'sale', remove any 'delisted' status changes on the date of 'sale' for all list_ids
    queryAthena("""drop table if exists data_science.stage_rets12;""")
    queryAthena("""
    create table data_science.stage_rets12 as
    select distinct t1.* from data_science.stage_rets11 as t1
    inner join (
        select
            list_id
            , property_id
            , case when sum(case when status = 'sale' then 1 else 0 end) >= 1 then 1 else 0 end as sale_ind
        from data_science.stage_rets11
        group by 1, 2
        having sum(case when status = 'sale' then 1 else 0 end) = 0
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
    union all
    select t1.* from data_science.stage_rets11 as t1
    inner join (
        select
            list_id
            , property_id
            , min(status_date) as sale_date
        from data_science.stage_rets11
        where status = 'sale'
        group by 1, 2
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
            and ((t1.status_date <= t2.sale_date and t1.status != 'delisted')
                or (t1.status_date < t2.sale_date and t1.status = 'delisted'));""")
    print("rets12 completed")
    
    # Get uniform values per each property_id - list_id for these columns: year_built, sqft, bedrooms, property_type, latitude, longitude, full_baths, half_baths
    queryAthena("""drop table if exists data_science.stage_rets13;""")
    queryAthena("""
    create table data_science.stage_rets13 as
    with prop_list_uniform as (
        select
            property_id
            , list_id
            , round(avg(latitude), 6) as latitude
            , round(avg(longitude), 6) as longitude
            , cast(approx_percentile(bedrooms, 0.5) as integer) as bedrooms
            , cast(approx_percentile(full_baths, 0.5) as integer) as full_baths
            , cast(approx_percentile(half_baths, 0.5) as integer) as half_baths
            , cast(approx_percentile(sqft, 0.5) as integer) as sqft
            , max(year_built) as year_built
        from data_science.stage_rets12
        group by 1, 2)
    select distinct
        t1.property_id
        , t1.list_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.property_type
        , t2.latitude
        , t2.longitude
        , t1.market
        , t2.bedrooms
        , t2.full_baths
        , t2.half_baths
        , t2.year_built
        , t2.sqft
        , t1.status
        , case when t1.status = 'sale' then 2
            when t1.status = 'pending' then 2
            when t1.status = 'active' then 3
            when t1.status = 'delisted' then 4
            end as status_rank
        , t1.status_date
        , case when t1.status = 'sale' and t1.sale_price is not null then t1.sale_price
            when t1.status = 'sale' and t1.sale_price is null and t1.list_price is not null then t1.list_price
            when t1.status = 'sale' and t1.sale_price is null and t1.list_price is null then t1.original_list_price
            when t1.status != 'sale' and t1.list_price is not null then t1.list_price
            when t1.status != 'sale' and t1.list_price is null and t1.original_list_price is not null then t1.original_list_price
            when t1.status != 'sale' and t1.list_price is null and t1.original_list_price is null and t1.sale_price is not null then t1.sale_price
            end as price
    from data_science.stage_rets12 as t1
    inner join prop_list_uniform as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id;""")
    print("rets13 completed")
    
    # Clean status so only one status per day unless sale and pending are on the same day
    # Clean price so only one price per day per status
    queryAthena("""drop table if exists data_science.stage_rets14;""")
    queryAthena("""
    create table data_science.stage_rets14 as
    with statuses as (
        select
            property_id
            , list_id
            , status_date
            , min(status_rank) as status_rank
        from data_science.stage_rets13
        group by 1, 2, 3
        ),
    prices as (
        select
            property_id
            , list_id
            , status_date
            , status
            , cast(approx_percentile(price, 0.5) as integer) as price
        from data_science.stage_rets13
        group by 1, 2, 3, 4
        )
    select distinct
        t1.property_id
        , t1.list_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t1.status
        , t1.status_rank
        , t1.status_date
        , t3.price
    from data_science.stage_rets13 as t1
    inner join statuses as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id
            and t1.status_date = t2.status_date
            and t1.status_rank = t2.status_rank
    inner join prices as t3
        on t1.property_id = t3.property_id
            and t1.list_id = t3.list_id
            and t1.status = t3.status
            and t1.status_date = t3.status_date;""")
    print("rets14 completed")
    
    # Remove all status changes after the date of 'sale', remove any 'delisted' status changes on the date of 'sale' for all list_ids
    queryAthena("""drop table if exists data_science.stage_rets15;""")
    queryAthena("""
    create table data_science.stage_rets15 as
    select distinct t1.* from data_science.stage_rets14 as t1
    inner join (
        select
            list_id
            , property_id
            , case when sum(case when status = 'sale' then 1 else 0 end) >= 1 then 1 else 0 end as sale_ind
        from data_science.stage_rets14
        group by 1, 2
        having sum(case when status = 'sale' then 1 else 0 end) = 0
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
    union all
    select t1.* from data_science.stage_rets14 as t1
    inner join (
        select
            list_id
            , property_id
            , min(status_date) as sale_date
        from data_science.stage_rets14
        where status = 'sale'
        group by 1, 2
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
            and ((t1.status_date <= t2.sale_date and t1.status != 'delisted')
                or (t1.status_date < t2.sale_date and t1.status = 'delisted'));""")
    print("rets15 completed")
    
    # Remove excess delisted records
    queryAthena("""drop table if exists data_science.stage_rets16;""")
    queryAthena("""
    create table data_science.stage_rets16 as
    select a1.* from data_science.stage_rets15 as a1
    left join (
        select
            t1.*
            , t2.status as prev_status
        from (
            select
                t1.property_id
                , t1.list_id
                , t1.status
                , t1.status_date
                , max(t2.status_date) as prev_status_date
            from data_science.stage_rets15 as t1
            inner join data_science.stage_rets15 as t2
                on t1.list_id = t2.list_id
                    and t1.property_id = t2.property_id
                    and t2.status_date < t1.status_date
            where t1.status = 'delisted'
            group by 1, 2, 3, 4
            ) as t1
        inner join data_science.stage_rets15 as t2
            on t1.list_id = t2.list_id
                and t1.property_id = t2.property_id
                and t1.prev_status_date = t2.status_date
        ) as a2
        on a1.list_id = a2.list_id
            and a1.property_id = a2.property_id
            and a1.status = a2.status
            and a1.status_date = a2.status_date
            and a2.prev_status = 'delisted'
    where a2.property_id is null;""")
    print("rets16 completed")
    
    # Conform data to final look
    queryAthena("""drop table if exists data_science.stage_rets;""")
    queryAthena("""
    create table data_science.stage_rets as
    with list_init as (
        select
            property_id
            , list_id
            , min(status_date) as list_date_init
        from data_science.stage_rets16
        where status = 'active'
        group by 1, 2
        ),
    next_status as (
        select
            t1.property_id
            , t1.list_id
            , t1.status_date
            , min(t2.status_date) as next_status_date
        from data_science.stage_rets16 as t1
        inner join data_science.stage_rets16 as t2
            on t1.property_id = t2.property_id
                and t1.list_id = t2.list_id
                and t1.status_date < t2.status_date
        group by 1, 2, 3
        ),
    list_price as (
        select
            a1.property_id
            , a1.list_id
            , a1.status
            , a1.status_date
            , cast(approx_percentile(a2.price, 0.5) as integer) as list_price
        from (
            select
                t1.property_id
                , t1.list_id
                , t1.status
                , t1.status_date
                , max(t2.status_date) as prev_status_date
            from data_science.stage_rets16 as t1
            inner join data_science.stage_rets16 as t2
                on t1.property_id = t2.property_id
                    and t1.list_id = t2.list_id
                    and t1.status_date > t2.status_date
            where t1.status = 'sale'
            group by 1, 2, 3, 4
            ) as a1
        inner join data_science.stage_rets14 as a2
            on a1.property_id = a2.property_id
                and a1.list_id = a2.list_id
                and a1.prev_status_date = a2.status_date
        group by 1, 2, 3, 4
        )
    select distinct
        t1.list_id
        , t1.property_id
        , t1.address_street_number
        , t1.address_dir_prefix
        , t1.address_street_name
        , t1.address_dir_suffix
        , t1.address_street_suffix
        , t1.address_unit_number
        , t1.address_city
        , t1.address_state
        , t1.address_zip
        , t1.address_county
        , cast(null as integer) as fips_county_cd
        , cast(null as varchar(10)) as cbsa_id
        , t1.property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t2.list_date_init as list_date
        , t1.status_date
        , t1.status
        , case when t3.next_status_date is null then date('2099-12-31') else t3.next_status_date end as next_status_date
        , case when t1.status = 'active' and t1.status_date = t2.list_date_init then 1 else 0 end as initial_listing_ind
        , case when t1.status = 'sale' then 1 else 0 end as sale_ind
        , case when t1.status = 'pending' then 1 else 0 end as pending_ind
        , case when t1.status = 'active' then 1 else 0 end as active_ind
        , case when t1.status = 'delisted' then 1 else 0 end as delisted_ind
        , case when t1.status = 'sale' then t1.price end as sale_price
        , case when t1.status = 'sale' then t4.list_price else t1.price end as list_price
        , case when t1.status = 'pending' then date_diff('day', t2.list_date_init, t1.status_date) end as dom
    from data_science.stage_rets16 as t1
    inner join list_init as t2
        on t1.property_id = t2.property_id
            and t1.list_id = t2.list_id
    left join next_status as t3
        on t1.property_id = t3.property_id
            and t1.list_id = t3.list_id
            and t1.status_date = t3.status_date
    left join list_price as t4
        on t1.property_id = t4.property_id
            and t1.list_id = t4.list_id
            and t1.status = t4.status
            and t1.status_date = t4.status_date;""")
    print("rets completed")
    
    # Combine rets and attom
    queryAthena("""drop table if exists data_science.stage_listings;""")
    queryAthena("""
    create table data_science.stage_listings as
    select distinct
        list_id
        , property_id
        , address_street_number
        , address_dir_prefix
        , address_street_name
        , address_dir_suffix
        , address_street_suffix
        , address_unit_number
        , address_city
        , address_state
        , address_zip
        , address_county
        , fips_county_cd
        , cbsa_id
        , property_type
        , latitude
        , longitude
        , market
        , bedrooms
        , full_baths
        , half_baths
        , year_built
        , sqft
        , list_date
        , status_date
        , status
        , next_status_date
        , initial_listing_ind
        , sale_ind
        , pending_ind
        , active_ind
        , delisted_ind
        , sale_price
        , list_price
        , dom
    from data_science.stage_attom
    union all
    select * from data_science.stage_rets;""")
    print("stage_listings completed")

    # Clean address data and next_status_date
    queryAthena("""drop table if exists data_science.prog_listings;""")
    queryAthena("""
    create table data_science.prog_listings as
    select distinct
        t1.list_id
        , t1.property_id
        , t2.address_street_number
        , t3.address_dir_prefix
        , t4.address_street_name
        , t5.address_dir_suffix
        , t6.address_street_suffix
        , t7.address_unit_number
        , t8.address_city
        , t9.address_state
        , t10.address_zip
        , t11.address_county
        , t12.fips_county_cd
        , t13.cbsa_id
        , case when t7.address_unit_number is not null then 'Multi Unit' else t1.property_type end as property_type
        , t1.latitude
        , t1.longitude
        , t1.market
        , t1.bedrooms
        , t1.full_baths
        , t1.half_baths
        , t1.year_built
        , t1.sqft
        , t1.list_date
        , t1.status_date
        , t1.status
        , case when t1.status = 'active' and date_diff('day', t1.status_date, current_date) > 365 and date_diff('day', t1.status_date, t1.next_status_date) > 365
            then date_add('year', 1, t1.status_date) else t1.next_status_date end as next_status_date
        , t1.initial_listing_ind
        , t1.sale_ind
        , t1.pending_ind
        , t1.active_ind
        , t1.delisted_ind
        , t1.sale_price
        , t1.list_price
        , case when t1.dom < 0 or t1.dom > 730 then null else t1.dom end as dom
    from data_science.stage_listings as t1
    left join (
        select
            list_id
            , property_id
            , address_street_number
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_street_number) as order_number
        from (
            select
                list_id
                , property_id
                , address_street_number
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_street_number is not null
            group by 1, 2, 3)
        ) as t2
        on t1.list_id = t2.list_id
            and t1.property_id = t2.property_id
            and t2.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_dir_prefix
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_dir_prefix) as order_number
        from (
            select
                list_id
                , property_id
                , address_dir_prefix
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_dir_prefix in ('e', 'w', 's', 'n', 'ne', 'nw', 'se', 'sw')
            group by 1, 2, 3)
        ) as t3
        on t1.list_id = t3.list_id
            and t1.property_id = t3.property_id
            and t3.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_street_name
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_street_name) as order_number
        from (
            select
                list_id
                , property_id
                , address_street_name
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_street_name is not null
            group by 1, 2, 3)
        ) as t4
        on t1.list_id = t4.list_id
            and t1.property_id = t4.property_id
            and t4.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_dir_suffix
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_dir_suffix) as order_number
        from (
            select
                list_id
                , property_id
                , address_dir_suffix
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_dir_suffix in ('e', 'w', 's', 'n', 'ne', 'nw', 'se', 'sw')
            group by 1, 2, 3)
        ) as t5
        on t1.list_id = t5.list_id
            and t1.property_id = t5.property_id
            and t5.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_street_suffix
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_street_suffix) as order_number
        from (
            select
                list_id
                , property_id
                , address_street_suffix
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_street_suffix is not null
            group by 1, 2, 3)
        ) as t6
        on t1.list_id = t6.list_id
            and t1.property_id = t6.property_id
            and t6.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_unit_number
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_unit_number) as order_number
        from (
            select
                list_id
                , property_id
                , address_unit_number
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_unit_number is not null
            group by 1, 2, 3)
        ) as t7
        on t1.list_id = t7.list_id
            and t1.property_id = t7.property_id
            and t7.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_city
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_city) as order_number
        from (
            select
                list_id
                , property_id
                , address_city
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_city is not null
            group by 1, 2, 3)
        ) as t8
        on t1.list_id = t8.list_id
            and t1.property_id = t8.property_id
            and t8.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_state
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_state) as order_number
        from (
            select
                list_id
                , property_id
                , address_state
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_state is not null
            group by 1, 2, 3)
        ) as t9
        on t1.list_id = t9.list_id
            and t1.property_id = t9.property_id
            and t9.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_zip
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_zip) as order_number
        from (
            select
                list_id
                , property_id
                , address_zip
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_zip is not null
            group by 1, 2, 3)
        ) as t10
        on t1.list_id = t10.list_id
            and t1.property_id = t10.property_id
            and t10.order_number = 1
    left join (
        select
            list_id
            , property_id
            , address_county
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, address_county) as order_number
        from (
            select
                list_id
                , property_id
                , address_county
                , count(*) as record_cnt
            from data_science.stage_listings
            where address_county is not null
            group by 1, 2, 3)
        ) as t11
        on t1.list_id = t11.list_id
            and t1.property_id = t11.property_id
            and t11.order_number = 1
    left join (
        select
            list_id
            , property_id
            , fips_county_cd
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, fips_county_cd) as order_number
        from (
            select
                list_id
                , property_id
                , fips_county_cd
                , count(*) as record_cnt
            from data_science.stage_listings
            where fips_county_cd is not null
            group by 1, 2, 3)
        ) as t12
        on t1.list_id = t12.list_id
            and t1.property_id = t12.property_id
            and t12.order_number = 1
    left join (
        select
            list_id
            , property_id
            , cbsa_id
            , record_cnt
            , rank() over (partition by list_id, property_id order by list_id, property_id, record_cnt desc, cbsa_id) as order_number
        from (
            select
                list_id
                , property_id
                , cbsa_id
                , count(*) as record_cnt
            from data_science.stage_listings
            where cbsa_id is not null
            group by 1, 2, 3)
        ) as t13
        on t1.list_id = t13.list_id
            and t1.property_id = t13.property_id
            and t13.order_number = 1;
    """)
    print("prog_listings completed")


def get_date_data():
    queryAthena("""drop table if exists data_science.stage_prog_date;""")
    queryAthena("""
    create table data_science.stage_prog_date as
    (select date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01')) as dt) union all
    (select date_add('month', -1, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -2, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -3, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -4, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -5, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -6, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -7, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -8, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -9, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -10, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt) union all
    (select date_add('month', -11, date(concat(cast(year(current_date) + 1 as varchar(4)), '-01-01'))) as dt);""")
    
    queryAthena("""drop table if exists data_science.prog_date_backbone;""")
    queryAthena("""
    create table data_science.prog_date_backbone as
    select dt as report_date from (
        select date_add('day', -1, dt) as dt from data_science.stage_prog_date
        union all
        select date_add('day', -1, date_add('year', -1, dt)) as dt from data_science.stage_prog_date
        union all
        select date_add('day', -1, date_add('year', -2, dt)) as dt from data_science.stage_prog_date
        union all
        select date_add('day', -1, date_add('year', -3, dt)) as dt from data_science.stage_prog_date
        union all
        select date_add('day', -1, date_add('year', -4, dt)) as dt from data_science.stage_prog_date
        union all
        select date_add('day', -1, date_add('year', -5, dt)) as dt from data_science.stage_prog_date
        union all
        select date_add('day', -1, date_add('year', -6, dt)) as dt from data_science.stage_prog_date
        union all
        select date_add('day', -1, date_add('year', -7, dt)) as dt from data_science.stage_prog_date
        union all
        select date_add('day', -1, date_add('year', -8, dt)) as dt from data_science.stage_prog_date)
    where dt < current_date
    ;""")


def eliminate_step_tables():
    
    queryAthena("""drop table if exists data_science.stage_prog_date;""")
    
    queryAthena("""drop table if exists data_science.stage_attom1;""")
    queryAthena("""drop table if exists data_science.stage_attom2;""")
    queryAthena("""drop table if exists data_science.stage_attom3;""")
    queryAthena("""drop table if exists data_science.stage_attom4;""")
    queryAthena("""drop table if exists data_science.stage_attom5;""")
    queryAthena("""drop table if exists data_science.stage_attom6;""")
    queryAthena("""drop table if exists data_science.stage_attom7;""")
    queryAthena("""drop table if exists data_science.stage_attom8;""")
    queryAthena("""drop table if exists data_science.stage_attom_overlaps1;""")
    queryAthena("""drop table if exists data_science.stage_attom_overlaps2;""")
    queryAthena("""drop table if exists data_science.stage_attom_overlaps3;""")
    queryAthena("""drop table if exists data_science.stage_attom_overlaps;""")
    queryAthena("""drop table if exists data_science.stage_attom9;""")
    queryAthena("""drop table if exists data_science.stage_attom10;""")
    queryAthena("""drop table if exists data_science.stage_attom11;""")
    queryAthena("""drop table if exists data_science.stage_attom12;""")
    queryAthena("""drop table if exists data_science.stage_attom13;""")
    queryAthena("""drop table if exists data_science.stage_attom14;""")
    queryAthena("""drop table if exists data_science.stage_attom;""")
    print("All attom stage tables dropped.")
    
    queryAthena("""drop table if exists data_science.stage_rets1;""")
    queryAthena("""drop table if exists data_science.stage_rets2;""")
    queryAthena("""drop table if exists data_science.stage_rets3;""")
    queryAthena("""drop table if exists data_science.stage_rets4;""")
    queryAthena("""drop table if exists data_science.stage_rets5;""")
    queryAthena("""drop table if exists data_science.stage_rets6;""")
    queryAthena("""drop table if exists data_science.stage_rets7;""")
    queryAthena("""drop table if exists data_science.stage_rets8;""")
    queryAthena("""drop table if exists data_science.stage_rets9;""")
    queryAthena("""drop table if exists data_science.stage_rets10;""")
    queryAthena("""drop table if exists data_science.stage_rets_overlaps1;""")
    queryAthena("""drop table if exists data_science.stage_rets_overlaps2;""")
    queryAthena("""drop table if exists data_science.stage_rets_overlaps3;""")
    queryAthena("""drop table if exists data_science.stage_rets_overlaps;""")
    queryAthena("""drop table if exists data_science.stage_rets11;""")
    queryAthena("""drop table if exists data_science.stage_rets12;""")
    queryAthena("""drop table if exists data_science.stage_rets13;""")
    queryAthena("""drop table if exists data_science.stage_rets14;""")
    queryAthena("""drop table if exists data_science.stage_rets15;""")
    queryAthena("""drop table if exists data_science.stage_rets16;""")
    queryAthena("""drop table if exists data_science.stage_rets;""")
    queryAthena("""drop table if exists data_science.stage_listings;""")
    print("All rets stage tables dropped.")

get_attom_data()
get_rets_and_final_data()
get_date_data()

prog_listing_cnt = queryAthena("""select count(*) from data_science.prog_listings;""").iloc[0, 0]

if prog_listing_cnt > 100000000:
    print("The table data_science.prog_listings has " + str(prog_listing_cnt) + " rows.")
    eliminate_step_tables()
else:
    print("The table data_science.prog_listings has " + str(prog_listing_cnt) + " rows, check if and where the process failed before deleting the step tables.")
