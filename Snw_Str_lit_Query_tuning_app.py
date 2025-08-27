import streamlit as st
import json
import time
import re
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

# Reuse active Snowflake session inside Streamlit
session = get_active_session()

def get_query_tuning_recs(
    session: Session,
    query_id: str,
    model: str = "claude-3-sonnet",
    test_run: bool = False
) -> str:
    try:
        # Step 1: Retrieve query text
        query_text_df = session.sql(f"""
            SELECT QUERY_TEXT
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE QUERY_ID = '{query_id}'
            LIMIT 1
        """)
        rows = query_text_df.collect()
        if not rows:
            return f"❌ Error: No query found for QUERY_ID = {query_id}"
        query_text = rows[0]["QUERY_TEXT"]

        # Step 2: Generate the execution plan
        plan_df = session.sql(f"EXPLAIN USING JSON {query_text}")
        plan_rows = plan_df.collect()
        if not plan_rows:
            return "❌ Error: Could not generate query plan"
        plan_json = plan_rows[0][0]

        # Step 3: Retrieve execution stats
        hist_df = session.sql(f"""
            SELECT QUERY_ID, START_TIME, END_TIME, TOTAL_ELAPSED_TIME, 
                   ROWS_PRODUCED, ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE QUERY_ID = '{query_id}'
            LIMIT 1
        """)
        hist_rows = hist_df.collect()
        if not hist_rows:
            return "❌ Error: Could not retrieve execution history"
        hist_row = hist_rows[0]

        hist_dict = {
            col: (hist_row[col].isoformat() if hasattr(hist_row[col], "isoformat") else hist_row[col])
            for col in hist_df.schema.names
        }

        # Step 4: Build AI prompt
        prompt = f"""
You are a Snowflake SQL performance tuning assistant.
Analyze the query plan and execution stats below and provide optimization recommendations.

- Display the Original SQL text
- Suggest a Tuned SQL query (if possible)
- Explain key improvements (indexes, pruning, clustering, filters, etc.)

Query Plan (JSON):
{plan_json}

Execution Stats:
{json.dumps(hist_dict, indent=2)}

Original SQL:
{query_text}
"""

        # Step 5: Call Cortex AI
        cortex_sql = """
            SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?) AS RECOMMENDATION
        """
        rec_df = session.sql(cortex_sql, [model, prompt])
        rec_rows = rec_df.collect()
        if not rec_rows:
            return "❌ Error: No recommendation returned"
        recommendation = rec_rows[0]["RECOMMENDATION"]

        # Step 6: Optional test run (execute query as-is, discard results)
        if test_run:
            try:
                # Run original query
                t0 = time.time()
                session.sql(query_text).collect()  # execute original SQL
                orig_time = round(time.time() - t0, 2)

                # Extract tuned SQL from AI response
                tuned_sql = None
                sql_blocks = re.findall(
                    r"```sql(.*?)```", recommendation, flags=re.DOTALL | re.IGNORECASE
                )
                if sql_blocks:
                    tuned_sql = sql_blocks[0].strip()
                else:
                    for line in recommendation.splitlines():
                        if line.strip().upper().startswith("SELECT"):
                            tuned_sql = line.strip()
                            break

                if tuned_sql:
                    tuned_sql_clean = re.sub(r";\s*$", "", tuned_sql.strip())

                    # Run tuned SQL as-is
                    t1 = time.time()
                    session.sql(tuned_sql_clean).collect()
                    tuned_time = round(time.time() - t1, 2)

                    recommendation += f"""

--- 🔎 Execution Benchmark ---
Original Query -> {orig_time}s
Tuned Query    -> {tuned_time}s
"""
                else:
                    recommendation += "\n⚠️ Tuned SQL could not be extracted for test run"

            except Exception as e:
                recommendation += f"\n⚠️ Test run failed: {str(e)}"

        return recommendation

    except Exception as e:
        return f"❌ Error analyzing query: {str(e)}"


# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Snowflake Query Tuning Assistant", layout="wide")

st.title("🔧 Snowflake Query Tuning Assistant (Cortex AI)")

with st.form("tuning_form"):
    query_id = st.text_input("Enter Query ID:", "")
    model = st.selectbox("Choose Cortex Model:", ["claude-4-sonnet", "mistral-large", "gpt-4o-mini"])
    test_run = st.checkbox("Run test benchmark (executes queries)", value=False)
    submitted = st.form_submit_button("Analyze Query")

if submitted:
    if not query_id:
        st.error("⚠️ Please enter a valid Query ID")
    else:
        with st.spinner("Analyzing query with Cortex AI... ⏳"):
            recs = get_query_tuning_recs(session, query_id, model, test_run)
        st.subheader("📋 Recommendations")
        st.markdown(recs)
