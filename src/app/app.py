"""Reel Insights -- Databricks App.

Tabs:
  Overview            -- original starter content (unchanged).
  Data Steward Review  -- the human half of the correction feedback loop
                          (see src/transform/silver_pos_ticket_sales.py).
                          Lists rows quarantined in pos_quality_issues with
                          resolution_status = PENDING_REVIEW, and lets a
                          steward submit a decision -- CORRECT (with
                          corrected field values), APPROVE_AS_IS, or
                          REJECT_PERMANENTLY -- which is written to
                          silver.pos_corrections. The next
                          build_medallion_theater_sales run reads that table
                          and the row flows through Silver accordingly.

Connects to the SQL warehouse bound as the "sql-warehouse" resource in
resources/apps.yml (its ID is injected as DATABRICKS_WAREHOUSE_ID via
app.yaml). Auth to Databricks itself is automatic: Databricks Apps injects
service-principal credentials into the runtime, so WorkspaceClient() needs
no explicit host/token. Because actions are attributed to the app's service
principal (not the browser user) from Unity Catalog's point of view,
"reviewed_by" is a plain free-text field the steward fills in themselves --
that is the actual audit signal for "who made this call", not the
underlying Databricks identity.
"""

import logging
import os

import gradio as gr
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("reel_insights_app")

WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID")
CATALOG = os.environ.get("APP_CATALOG", "theater_management")
SILVER_SCHEMA = os.environ.get("APP_SILVER_SCHEMA", "silver")

_workspace_client: WorkspaceClient | None = None


def _client() -> WorkspaceClient:
    global _workspace_client
    if _workspace_client is None:
        _workspace_client = WorkspaceClient()
    return _workspace_client


def _run_query(sql: str) -> pd.DataFrame:
    """Runs a SELECT and returns the result as a DataFrame. Raises on failure."""
    resp = _client().statement_execution.execute_statement(
        statement=sql,
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        schema=SILVER_SCHEMA,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    if resp.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"Query failed ({resp.status.state.value}): {resp.status.error}")
    columns = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    return pd.DataFrame(rows, columns=columns)


def _run_statement(sql: str) -> None:
    """Runs a DML/DDL statement. Raises on failure."""
    resp = _client().statement_execution.execute_statement(
        statement=sql,
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        schema=SILVER_SCHEMA,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        wait_timeout="30s",
    )
    if resp.status.state.value != "SUCCEEDED":
        raise RuntimeError(f"Statement failed ({resp.status.state.value}): {resp.status.error}")


def _sql_literal(value) -> str:
    """Formats a Python value as a safe SQL literal: NULL, a quote-escaped
    string, or a bare number. Used instead of parameterized placeholders so
    optional/blank form fields map cleanly to SQL NULL."""
    if value is None or value == "":
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


PENDING_REVIEW_QUERY = """
SELECT
  transaction_id, pos_source, rejection_reason, theater_id, screen_id,
  movie_id, format, ticket_type, ticket_price, seats_sold,
  showtime, purchase_timestamp, quality_checked_at
FROM pos_quality_issues
WHERE resolution_status = 'PENDING_REVIEW'
ORDER BY quality_checked_at DESC, transaction_id
LIMIT 200
"""


def load_pending_review() -> pd.DataFrame:
    """Loads currently-quarantined, not-yet-reviewed rows. Returns an
    error-message DataFrame (instead of raising) so a warehouse cold-start or
    transient failure shows up in the table rather than crashing the tab."""
    try:
        return _run_query(PENDING_REVIEW_QUERY)
    except Exception as exc:  # noqa: BLE001 -- surfaced directly in the UI
        logger.exception("Failed to load pending review rows")
        return pd.DataFrame({"error": [str(exc)]})


def on_row_select(evt: gr.SelectData, table: pd.DataFrame) -> str:
    """Auto-fills transaction_id when a row in the pending-review table is clicked."""
    try:
        return str(table.iloc[evt.index[0]]["transaction_id"])
    except Exception:  # noqa: BLE001
        return ""


def submit_correction(
    transaction_id: str,
    decision: str,
    corrected_theater_id: str,
    corrected_screen_id: str,
    corrected_movie_id: str,
    corrected_format: str,
    corrected_showtime: str,
    corrected_ticket_price: str,
    corrected_seats_sold: str,
    corrected_purchase_timestamp: str,
    review_notes: str,
    reviewed_by: str,
) -> str:
    if not transaction_id or not transaction_id.strip():
        return "Enter or select a transaction_id first."
    if not decision:
        return "Choose a decision (Correct / Approve As-Is / Reject Permanently)."
    if not reviewed_by or not reviewed_by.strip():
        return "Enter your name/ID in Reviewed By -- corrections are audited."

    price_value = None
    if corrected_ticket_price and str(corrected_ticket_price).strip():
        try:
            price_value = float(corrected_ticket_price)
        except ValueError:
            return "Corrected Ticket Price must be a number (or left blank)."

    seats_value = None
    if corrected_seats_sold and str(corrected_seats_sold).strip():
        try:
            seats_value = int(corrected_seats_sold)
        except ValueError:
            return "Corrected Seats Sold must be a whole number (or left blank)."

    txn_id_literal = _sql_literal(transaction_id.strip())
    set_values = {
        "decision": _sql_literal(decision),
        "corrected_theater_id": _sql_literal(corrected_theater_id),
        "corrected_screen_id": _sql_literal(corrected_screen_id),
        "corrected_movie_id": _sql_literal(corrected_movie_id),
        "corrected_format": _sql_literal(corrected_format),
        "corrected_showtime": _sql_literal(corrected_showtime),
        "corrected_ticket_price": _sql_literal(price_value),
        "corrected_seats_sold": _sql_literal(seats_value),
        "corrected_purchase_timestamp": _sql_literal(corrected_purchase_timestamp),
        "review_notes": _sql_literal(review_notes),
        "reviewed_by": _sql_literal(reviewed_by.strip()),
        "reviewed_at": "current_timestamp()",
    }

    # MERGE (not plain INSERT) so re-submitting for the same transaction_id
    # revises the existing correction instead of erroring on a duplicate key
    # -- a steward can change their mind before the next pipeline run reads it.
    merge_sql = f"""
        MERGE INTO pos_corrections AS t
        USING (SELECT {txn_id_literal} AS transaction_id) AS s
        ON t.transaction_id = s.transaction_id
        WHEN MATCHED THEN UPDATE SET
          decision = {set_values["decision"]},
          corrected_theater_id = {set_values["corrected_theater_id"]},
          corrected_screen_id = {set_values["corrected_screen_id"]},
          corrected_movie_id = {set_values["corrected_movie_id"]},
          corrected_format = {set_values["corrected_format"]},
          corrected_showtime = {set_values["corrected_showtime"]},
          corrected_ticket_price = {set_values["corrected_ticket_price"]},
          corrected_seats_sold = {set_values["corrected_seats_sold"]},
          corrected_purchase_timestamp = {set_values["corrected_purchase_timestamp"]},
          review_notes = {set_values["review_notes"]},
          reviewed_by = {set_values["reviewed_by"]},
          reviewed_at = {set_values["reviewed_at"]}
        WHEN NOT MATCHED THEN INSERT (
          transaction_id, decision, corrected_theater_id, corrected_screen_id,
          corrected_movie_id, corrected_format, corrected_showtime,
          corrected_ticket_price, corrected_seats_sold,
          corrected_purchase_timestamp, review_notes, reviewed_by, reviewed_at
        )
        VALUES (
          {txn_id_literal}, {set_values["decision"]}, {set_values["corrected_theater_id"]},
          {set_values["corrected_screen_id"]}, {set_values["corrected_movie_id"]},
          {set_values["corrected_format"]}, {set_values["corrected_showtime"]},
          {set_values["corrected_ticket_price"]}, {set_values["corrected_seats_sold"]},
          {set_values["corrected_purchase_timestamp"]}, {set_values["review_notes"]},
          {set_values["reviewed_by"]}, {set_values["reviewed_at"]}
        )
    """

    try:
        _run_statement(merge_sql)
    except Exception as exc:  # noqa: BLE001 -- surfaced directly in the UI
        logger.exception("Failed to submit correction")
        return f"Failed to submit correction: {exc}"

    return (
        f"Correction recorded for transaction_id={transaction_id.strip()} "
        f"(decision={decision}). It takes effect the next time "
        "build_medallion_theater_sales runs silver_pos_ticket_sales."
    )


data = pd.DataFrame({'x': [x for x in range(30)],
                     'y': [2 ** x for x in range(30)]})

with gr.Blocks(css='footer {visibility: hidden}') as gradio_app:
    with gr.Tabs():
        with gr.Tab("Overview"):
            with gr.Row():
                with gr.Column(scale=3):
                    gr.Markdown('# Hello world!')
                    gr.ScatterPlot(value=data, height=400, width=700,
                                   container=False, x='x', y='y',
                                   y_title='Fun with data', x_title='Apps')

        with gr.Tab("Data Steward Review"):
            gr.Markdown(
                "## Correction Feedback Loop\n"
                "Review rows the medallion pipeline quarantined "
                "(`pos_quality_issues`, `resolution_status = PENDING_REVIEW`) "
                "and submit a decision. The next `build_medallion_theater_sales` "
                "run reads your decision from `silver.pos_corrections`."
            )
            refresh_btn = gr.Button("Refresh Pending Review Rows")
            pending_table = gr.Dataframe(interactive=False, wrap=True)

            with gr.Row():
                transaction_id_input = gr.Textbox(
                    label="transaction_id", placeholder="Click a row above, or type one",
                )
                decision_input = gr.Radio(
                    choices=["CORRECT", "APPROVE_AS_IS", "REJECT_PERMANENTLY"],
                    label="Decision",
                )

            gr.Markdown(
                "### Corrected values\n"
                "Only used when Decision = CORRECT; leave a field blank to keep the original value."
            )
            with gr.Row():
                corrected_theater_id_input = gr.Textbox(label="corrected_theater_id")
                corrected_screen_id_input = gr.Textbox(label="corrected_screen_id")
                corrected_movie_id_input = gr.Textbox(label="corrected_movie_id")
                corrected_format_input = gr.Textbox(label="corrected_format")
            with gr.Row():
                corrected_showtime_input = gr.Textbox(
                    label="corrected_showtime", placeholder="YYYY-MM-DDTHH:MM:SS",
                )
                corrected_ticket_price_input = gr.Textbox(label="corrected_ticket_price")
                corrected_seats_sold_input = gr.Textbox(label="corrected_seats_sold")
                corrected_purchase_timestamp_input = gr.Textbox(
                    label="corrected_purchase_timestamp", placeholder="YYYY-MM-DDTHH:MM:SS",
                )

            review_notes_input = gr.Textbox(
                label="review_notes", lines=3,
                placeholder="Why? (required for the audit trail)",
            )
            reviewed_by_input = gr.Textbox(label="reviewed_by", placeholder="Your name or ID")

            submit_btn = gr.Button("Submit Decision", variant="primary")
            result_box = gr.Textbox(label="Result", interactive=False)

            refresh_btn.click(fn=load_pending_review, outputs=pending_table)
            pending_table.select(fn=on_row_select, inputs=pending_table, outputs=transaction_id_input)
            submit_btn.click(
                fn=submit_correction,
                inputs=[
                    transaction_id_input, decision_input,
                    corrected_theater_id_input, corrected_screen_id_input,
                    corrected_movie_id_input, corrected_format_input,
                    corrected_showtime_input, corrected_ticket_price_input,
                    corrected_seats_sold_input, corrected_purchase_timestamp_input,
                    review_notes_input, reviewed_by_input,
                ],
                outputs=result_box,
            ).then(fn=load_pending_review, outputs=pending_table)

    # Loaded once per browser session (not at app-process startup), so a
    # cold SQL warehouse doesn't block the app's own startup/health check.
    gradio_app.load(fn=load_pending_review, outputs=pending_table)

if __name__ == '__main__':
    gradio_app.launch(server_name="0.0.0.0", server_port=8000)
