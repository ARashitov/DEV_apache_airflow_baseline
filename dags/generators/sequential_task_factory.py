"""
Author: Adil Rashitov
Created at: 15.08.2021
About:
    Performs factory of sequential task
"""


def get_sequential_task(AirflowOperator: object,
                        operator_args: dict,
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
    return AirflowOperator(**operator_args, op_kwargs=op_kwargs)
