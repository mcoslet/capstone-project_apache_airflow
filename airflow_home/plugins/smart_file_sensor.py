from airflow.sensors.filesystem import FileSensor
from airflow.utils.decorators import apply_defaults
from typing import Any


#
# This is a deprecated early-access feature that will be removed in Airflow 2.4.0.
# It is superseded by Deferrable Operators, which offer a more flexible way to achieve efficient long-running sensors,
# as well as allowing operators to also achieve similar efficiency gains.
# If you are considering writing a new Smart Sensor, you should instead write it as a Deferrable Operator.
#
class SmartFileSensor(FileSensor):
    poke_context_fields = ('filepath', 'fs_conn_id')

    @apply_defaults
    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)

    def is_smart_sensor_compatible(self):
        result = (
                not self.soft_fail
                and super().is_smart_sensor_compatible()
        )
        return result
