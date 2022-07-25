WITH customers AS (
    SELECT 
        *
    FROM 
        {{ ref('stg_customers')}}
)

,orders AS (

    SELECT
        *
    FROM
        {{ ref('stg_orders')}}
)

,fct_orders AS (

    SELECT
        *
    FROM
        {{ ref('fct_orders')}}
)

,customer_orders AS (

    SELECT 
        orders.customer_id,
        min(orders.order_date) AS first_order_date,
        max(orders.order_date) AS most_recent_order_date,
        count(orders.order_id) AS number_of_orders,
        sum(coalesce(fct_orders.amount, 0)) as amount
    FROM
        orders
            LEFT JOIN fct_orders using (order_id)
    GROUP BY 1

)

,final AS (

    SELECT 
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) AS number_of_orders,
        coalesce(customer_orders.amount, 0) as life_time_value

    FROM
        customers 
            LEFT JOIN customer_orders USING(customer_id)
)

SELECT * FROM final

