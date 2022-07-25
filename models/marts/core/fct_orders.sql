with orders as (

    select
        order_id,
        customer_id,
        order_date
    from
        {{ ref('stg_orders')}}
)


,order_payments as (

    select 
        order_id,
        sum(
            case when
                stg_payments.status = 'success' then amount 
            else 0 
            end) as amount
    from
        {{ ref('stg_payments')}}
    group by 1

)

,final as (

    select 
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(order_payments.amount, 0) as amount
    from
        orders
            left join order_payments using (order_id)
)

select * from final