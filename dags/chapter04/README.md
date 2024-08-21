Листинг 4.2 был пропущен, так как я посчитал не целесообразно для него выделять целый файл.
В нём описывается что {{execution_date.year}} под копотом имеет либу pendulum
и сравнивают datetime с oendulum, вот пример:
```
>>> from datetime import datetime
>>> import pendulum
>>> datetime.now().year
2020
>>> pendulum.now().year
2020
````

Листинг 4.4 Так же был не удостоен отдельного файла.
Это ответ из листинга 4.3.
Все переменные записываются в  ** kwargs и  передаются функции print()
```{'conf': <***.configuration.AirflowConfigParser object at 0x7fa287ddccb0>,
   'dag': <DAG: chapter4_print_context>,
   'dag_run': <DagRun chapter4_print_context @ 2024-08-18 00:00:00+00:00:
               scheduled__2024-08-18T00:00:00+00:00,
               state:running,
               queued_at: 2024-08-21 06:02:07.937706+00:00. externally
               triggered: False>,
               'data_interval_end': DateTime(2024, 8, 19, 0, 0, 0, tzinfo=Timezone('UTC')),
               'data_interval_start': DateTime(2024, 8, 18, 0, 0, 0, tzinfo=Timezone('UTC')),
                'ds': '2024-08-18',
                'ds_nodash': '20240818',
                ....
   }
```
