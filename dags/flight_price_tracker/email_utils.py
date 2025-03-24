def prepare_email_body(**kwargs):
    rows_to_notify = kwargs["ti"].xcom_pull(
        key="rows_to_notify", task_ids="prepare_price_alerts"
    )
    if not rows_to_notify:
        return None

    body = "The following locations have flight deals:\n\n"
    for row in rows_to_notify:
        location = row["location"]
        cheapest_price = row["cheapest_price"]
        avg_price = row["average_price"]
        body += f"Location: {location}, Cheapest Price: ${cheapest_price}, Average Price: ${avg_price}\n"

    return body
