import datetime

from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

DAG_NAME = "example_subdag_operator"

with DAG(
        dag_id=DAG_NAME,
        default_args={"retries": 2},
        start_date=datetime.datetime(2022, 1, 1),
        schedule="@once",
        tags=['example', 'sub_dag', 'task_group']
) as dag:
    # SubDag
    start_sub_dag = EmptyOperator(
        task_id="start",
    )

    section_1_sub_dag = SubDagOperator(
        task_id="section-1",
        subdag=subdag(DAG_NAME, "section-1", dag.default_args),
    )

    some_other_task_sub_dag = EmptyOperator(
        task_id="some-other-task",
    )

    section_2_sub_dag = [SubDagOperator(
        task_id=f"section-2_{i}",
        subdag=subdag(DAG_NAME, f"section-2_{i}", dag.default_args),
    ) for i in range(5)]

    end_sub_dag = EmptyOperator(
        task_id="end",
    )

    # Task Group
    start_task_group = EmptyOperator(
        task_id="start_task_group",
    )
    with TaskGroup(group_id='section_1_task_group') as section_1_task_group:
        some_other_task_group = [EmptyOperator(
            task_id=f"some_other_task_group_{i}"
        ) for i in range(5)]

    with TaskGroup(group_id='section_2_task_group') as section_2_task_group:
        section_2_task_group = [EmptyOperator(
            task_id=f"section_2_task_group_{i}"
        ) for i in range(5)]

    end_task_group = EmptyOperator(
        task_id="end_task_group"
    )

    start_sub_dag >> section_1_sub_dag >> some_other_task_sub_dag >> section_2_sub_dag >> end_sub_dag
    start_task_group >> section_1_task_group >> section_2_task_group >> end_task_group
