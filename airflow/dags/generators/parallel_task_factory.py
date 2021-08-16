"""
Author: Adil Rashitov
Created at: 15.08.2021
About:
    Performs factory of parallel tasks
"""


def get_parallel_task(AirflowOperator: object,
                      operator_args: dict,
                      n_tasks: int,
                      op_kwargs: dict = {}):
    """
        Airflow task factory function

        Arguments:
        * python_callable (callable): Python callable object
        * AirflowOperator (object): Any apache airflow operator
        * op_kwargs (str): Tasks `op_kwargs` arguments
        * extra_operator_args (dict): AirflowOperator arguments

        Returns:
        * (Any): Apache airflow task
    """
    op_kwargs['n_tasks'] = n_tasks

    # 1. Extraction of naming template
    task_id = str(operator_args['task_id'])
    tasks = []

    for task_index in range(n_tasks):

        op_kwargs['task_index'] = task_index

        # 2. Building task_id for each task
        op_args = dict(operator_args)
        op_args['task_id'] = f"{task_id}_{task_index}"

        tasks.append(AirflowOperator(**op_args,
                                     op_kwargs=op_kwargs))
    return tasks
