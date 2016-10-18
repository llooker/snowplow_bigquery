- connection: production

- include: "*.view.lookml"       # include all the views
- include: "*.dashboard.lookml"  # include all the dashboards

# NOTE: please see https://www.looker.com/docs/r/dialects/bigquery
# NOTE: for BigQuery specific considerations

- explore: event
  sql_always_where: ${event.domain_userid} is not null
  joins:
    - join: session
      type: inner
      relationship: many_to_one
      sql_on: ${event.domain_userid} = ${session.domain_userid} AND ${event.domain_session_index} = ${session.domain_session_index}
  
- explore: session
