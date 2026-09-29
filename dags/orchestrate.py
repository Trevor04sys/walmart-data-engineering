from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from airflow.utils.email import send_email
from datetime import timedelta
import os
import time

EMAIL = "joshp3781@gmail.com"


def task_failure_email(context):
    """Send an email when a task fails."""
    ti = context["task_instance"]
    dag_run = context.get("dag_run")

    subject = f"Airflow Task Failed: {ti.task_id}"

    body = f"""
    <h2>Airflow Task Failure</h2>
    <p><b>DAG:</b> {ti.dag_id}</p>
    <p><b>Task:</b> {ti.task_id}</p>
    <p><b>Execution date:</b> {ti.logical_date}</p>
    <p><b>Try number:</b> {ti.try_number}</p>
    <p><b>Run ID:</b> {dag_run.run_id if dag_run else "N/A"}</p>
    <p><b>Log URL:</b> <a href="{ti.log_url}">View task logs</a></p>
    """

    send_email(
        to=EMAIL,
        subject=subject,
        html_content=body,
    )


def dag_success_email(context):
    """Send an email when the entire DAG succeeds."""
    dag_run = context["dag_run"]

    subject = f"Airflow DAG Succeeded: {dag_run.dag_id}"

    body = f"""
    <h2>Airflow DAG Completed Successfully</h2>
    <p><b>DAG:</b> {dag_run.dag_id}</p>
    <p><b>Run ID:</b> {dag_run.run_id}</p>
    <p><b>Execution date:</b> {dag_run.logical_date}</p>
    <p><b>Start time:</b> {dag_run.start_date}</p>
    <p><b>End time:</b> {dag_run.end_date}</p>
    """

    send_email(
        to=EMAIL,
        subject=subject,
        html_content=body,
    )


@dag(
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": task_failure_email,
    },
    on_success_callback=dag_success_email,
)

def orchestrate():

    # ==========================================================
    # 1. INGEST CDC
    # ==========================================================

    @task(execution_timeout=timedelta(minutes=45))
    def ingest_cdc():

        pipeline_id = "706fe495-4eec-4f0d-9713-2492d3c3617a"

        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")

        if not host:
            raise ValueError("DATABRICKS_HOST is not set")

        if not token:
            raise ValueError("DATABRICKS_TOKEN is not set")

        w = WorkspaceClient(
            host=f"https://{host}",
            token=token,
        )

        # --------------------------------------------------
        # Check for an already-running pipeline update
        # --------------------------------------------------

        updates_response = w.pipelines.list_updates(
            pipeline_id=pipeline_id,
            max_results=10,
        )

        active_states = [
            "CREATED",
            "WAITING_FOR_RESOURCES",
            "INITIALIZING",
            "RUNNING",
            "RESETTING",
        ]

        update_id = None

        for update in updates_response.updates or []:

            state = update.state

            if state and state.value in active_states:

                update_id = update.update_id

                print(
                    f"Found existing active Databricks update: "
                    f"{update_id}"
                )

                print(
                    f"Existing update state: "
                    f"{state.value}"
                )

                break

        # --------------------------------------------------
        # Start a new update if none is active
        # --------------------------------------------------

        if update_id is None:

            response = w.pipelines.start_update(
                pipeline_id=pipeline_id,
            )

            update_id = response.update_id

            print(
                f"Started new Databricks pipeline update: "
                f"{update_id}"
            )

        # --------------------------------------------------
        # Monitor the update
        # --------------------------------------------------

        start_time = time.time()
        timeout = 30 * 60

        while True:

            status = w.pipelines.get_update(
                pipeline_id=pipeline_id,
                update_id=update_id,
            )

            state = status.update.state

            print(
                f"Databricks pipeline state: "
                f"{state.value}"
            )

            if state.value == "COMPLETED":

                print(
                    "Databricks pipeline completed successfully."
                )

                break

            if state.value in [
                "FAILED",
                "CANCELED",
                "CANCELING",
                "DRIVER_UNAVAILABLE",
            ]:

                raise RuntimeError(
                    f"Databricks pipeline failed "
                    f"with state: {state.value}"
                )

            if time.time() - start_time > timeout:

                raise TimeoutError(
                    "Databricks pipeline did not complete "
                    "within 30 minutes."
                )

            time.sleep(20)

        return update_id


    # ==========================================================
    # 2. SOURCE FRESHNESS
    # ==========================================================

    @task.bash(execution_timeout=timedelta(minutes=10))
    def source_freshness():

        return (
            "cd /opt/airflow/walmart_project "
            "&& dbt source freshness"
        )


    # ==========================================================
    # 3. SILVER TECHNICAL
    # ==========================================================

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select path:models/silver_technical",
        execution_timeout=timedelta(minutes=30),
    )


    # ==========================================================
    # 4. SILVER TECHNICAL TESTS
    #
    # Tests defined in:
    # models/silver_technical/properties.yml
    #
    # Includes:
    # - not_null
    # - unique
    # - relationships
    # ==========================================================

    silver_technical_test = BashOperator(
        task_id="silver_technical_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select path:models/silver_technical",
        execution_timeout=timedelta(minutes=15),
    )


    # ==========================================================
    # 5. SILVER BUSINESS / OBT
    # ==========================================================

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select path:models/silver_business",
        execution_timeout=timedelta(minutes=30),
    )


    # ==========================================================
    # 6. SILVER BUSINESS / OBT TESTS
    #
    # Tests:
    # tests/silver_business/
    #
    # - test_obt.sql
    # - test_obt_grain.sql
    # - test_order_total_reconciliation.sql
    # ==========================================================

    silver_business_test = BashOperator(
        task_id="silver_business_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select path:tests/silver_business",
        execution_timeout=timedelta(minutes=15),
    )


    # ==========================================================
    # 7. GOLD EPHEMERAL
    # ==========================================================

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select path:models/gold/ephemeral",
        execution_timeout=timedelta(minutes=15),
    )


    # ==========================================================
    # 8. GOLD DIMENSIONAL / SCD2
    # ==========================================================

    gold_dimensional = BashOperator(
        task_id="gold_dimensional",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt snapshot",
        execution_timeout=timedelta(minutes=30),
    )


    # ==========================================================
    # 9. GOLD DIMENSION TESTS
    #
    # Tests:
    # tests/gold_dimensions/
    #
    # - test_dim_customers_one_current.sql
    # - test_dim_customers_no_overlap.sql
    # ==========================================================

    gold_dimension_test = BashOperator(
        task_id="gold_dimension_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select path:tests/gold_dimensions",
        execution_timeout=timedelta(minutes=15),
    )


    # ==========================================================
    # 10. GOLD FACT
    # ==========================================================

    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt run --select path:models/gold/fact",
        execution_timeout=timedelta(minutes=30),
    )


    # ==========================================================
    # 11. GOLD FACT TESTS
    #
    # Tests:
    # tests/gold_fact/
    #
    # - test_fact_grain.sql
    # - test_fact_customer_temporal_consistency.sql
    # - test_fact_product_temporal_consistency.sql
    # - test_fact_store_temporal_consistency.sql
    # ==========================================================

    gold_fact_test = BashOperator(
        task_id="gold_fact_test",
        cwd="/opt/airflow/walmart_project",
        bash_command="dbt test --select path:tests/gold_fact",
        execution_timeout=timedelta(minutes=15),
    )


    # ==========================================================
    # DAG DEPENDENCY / DATA LINEAGE
    # ==========================================================

    (
        ingest_cdc()
        >> source_freshness()
        >> silver_technical
        >> silver_technical_test
        >> silver_business
        >> silver_business_test
        >> gold_ephemeral
        >> gold_dimensional
        >> gold_dimension_test
        >> gold_fact
        >> gold_fact_test
    )


# ==============================================================
# DAG INSTANCE
# ==============================================================

orchestrate_dag = orchestrate()