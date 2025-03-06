with
identification_ogrn as (
    select documentnumber, 
           key
    from (
        select interim.documentnumber,
               interim.key,
               row_number() over (
                   partition by interim.documentnumber
                   order by startdate desc
               ) as row_number
        from prx_parallelrun.identification_ogrn as interim
    )
    where row_number = 1
),
identificatation_inn as (
    select documentnumber,
           key
    from (
        select interim.documentnumber,
               interim.key,
               row_number() over (
                   partition by interim.documentnumber
                   order by startdate desc
               ) as row_number
        from prx_parallelrun.identificatation_inn as interim
    )
    where row_number = 1
)
select cast(productoffer.ucpid as string) as epk_id,
       cast(identificatation_inn.documentnumber as string) as inn,
       cast(identification_ogrn.documentnumber as string) as ogrn,
       sbcccproduct.name,
       productoffer.creationtime,
       productoffer.dealid,
       productoffer.productcode,
       productoffer.productofferdescription,
       productoffer.productofferstatuscode,
       productoffer.productofferid,
       productoffer.productgroupcode,
       productoffer.closingcomment,
       productoffer.offercampaign,
       productoffer.offerstartdate,
       productoffer.offerenddate,
       productoffer.sbrfofferrefusalcode,
       productoffer.sbrfsalestagecode,
       deal_stage.name as deal_status,
       offer_status.name as offer_status,
       sale_stage.name as sale_status,
       task_status.name as task_status,
       task_channel.name as task_channel,
       task_result.name as task_result,
       case taskentity.tasktype
           when 'CALLING' then 'Звонок'
           when 'APPOINTMENT' then 'Встреча'
           when 'CHECKUP' then 'Проверка'
           when 'OUTFLOW' then 'Отток'
           when 'NEWS' then 'Новости'
           when 'ONBOARDING' then 'Адаптация'
           when 'MEET_MANAGER' then 'Встреча с менеджером'
           when 'CUSTOMER_SERVICE' then 'Обслуживание клиентов'
           when 'FOLLOWUP' then 'Следовать'
           when 'SERVICE_TASK_SERVICE' then 'Обслуживание по задаче'
           when 'ACTIVATION' then 'Активация'
           when 'INFORMATION' then 'Информация'
           when 'SERVICE_TASK_ORGANIZATION' then 'Обслуживание организации'
           when 'SERVICE_TASK_PRODUCT' then 'Обслуживание по продукту'
           when 'SERVICE_TASK_AGREEMENT' then 'Обслуживание по соглашению'
           when 'PRODUCT_REGISTRATION' then 'Регистрация продукта'
           else 'Неизвестно'
       end as tasktype
from prx_deals.productofferentity as productoffer
inner join prx_sbc.sbcccproduct as sbcccproduct
    on substr(sbcccproduct.businesscode, 4) = productoffer.productcode
    and sbcccproduct.name in (
        'Пакет услуг (0000000001)',
        'Пакет услуг (0000000002)',
        'Пакет услуг (0000000004)',
        'Пакет услуг (0000000005)',
        'Пакет услуг (0000000007)',
        'Пакет услуг (0000000008)',
        'Пакет услуг (0000000012)',
        'Пакет услуг (0000000015)',
        'Пакет услуг (1000000000)',
        'Пакет услуг (1000000001)',
        'Пакет услуг (0000000111)',
        'Пакет услуг (0000000234)',
        'Пакет услуг (0000000245)',
        'Пакет услуг (0000000267)',
        'Пакет услуг (0000000300)',
        'Пакет услуг (0000000301)',
        'Пакет услуг (0000000451)',
        'Пакет услуг (0000000452)',
        'Пакет услуг (0000000453)',
        'Пакет услуг (0000000467)',
        'Пакет услуг (0000000512)',
        'Пакет услуг (0000000518)',
        'Пакет услуг (0000100000)',
        'Пакет услуг (0111000000)',
        'Пакет услуг (0000000689)',
        'Пакет услуг (0000000700)',
        'Пакет услуг (0000000722)',
        'Пакет услуг (0000000724)',
        'Пакет услуг (0000000736)',
        'Пакет услуг (0000000769)',
        'Пакет услуг (0000000800)',
        'Пакет услуг (0000000999)',
        'Пакет услуг (0000001997)',
        'Пакет услуг (0000001998)',
        'Пакет услуг (0000001999)'
    )
left join prx_deals.offertaskrelationentity as offertaskrelationentity
    on offertaskrelationentity.productofferid = productoffer.productofferid
left join prx_deals.taskentity as taskentity
    on offertaskrelationentity.taskid = taskentity.taskid
left join prx_deals.dealentity as dealentity
    on dealentity.dealid = productoffer.dealid
inner join prx_parallelrun.organization as organization
    on organization.id = productoffer.ucpid
    and (
         organization.startdate
         between '2024-01-01 00:00:00.000000'
         and '2024-12-31 59:59:60.999999'
    )
inner join identificatation_inn
    on substring(identificatation_inn.key, 1, 32) = organization.key
inner join identification_ogrn
    on substring(identification_ogrn.key, 1, 32) = organization.key
left join prx_dds.deal_stage as deal_stage
    on dealentity.dealstagecode = deal_stage.code
left join prx_dds.offer_status as offer_status
    on productoffer.productofferstatuscode = offer_status.code
left join prx_dds.sale_stage as sale_stage
    on productoffer.sbrfsalestagecode = sale_stage.code
left join prx_dds.task_status as task_status
    on taskentity.taskstatus = task_status.code
left join prx_dds.sbrf_channel as task_channel
    on taskentity.taskchannelcode = task_channel.code
left join prx_dds.task_result as task_result
    on taskentity.taskresult = task_result.code
where (
       productoffer.creationtime
       between '2024-01-01 00:00:00.000000'
       and '2024-12-31 59:59:60.999999'
)
and (
     productoffer.productofferdescription like '%Предложить%'
     or productoffer.productofferdescription like '%У клиента%'
)
order by sbcccproduct.name, productoffer.creationtime desc;
