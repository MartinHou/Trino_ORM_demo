from ppl_datalake.impl_iceberg import TableIceberg
from utils.trino_utils import IcebergDB
from datetime import datetime
from typing import List
from types import SimpleNamespace
from backend.settings import TIME_ZONE
import json
from configs import TRINO_CONFIG_PATH
import configparser

__all__ = ['TrinoWorkflow', 'TrinoResult']

config = configparser.ConfigParser()

config.read(TRINO_CONFIG_PATH)

result_tb = config.get('DEFAULT', 'result_tb')
workflow_tb = config.get('DEFAULT', 'workflow_tb')


class TrinoQuerySet:
    def __init__(self, table: TableIceberg, table_name: str,
                 namespace: str) -> None:
        self._ns = namespace
        self._table = table
        self._table_name = table_name
        self._fields = []
        self._json_keys = []    # fields that are json type
        self._datetime_fields = []  # fields that are datetime type
        self._map = {}  # map from the field name in the database to the returned field name
        self._filter_map = {}   # map from the field name in the filter to the field name in the database
        self._conditions = []   # filter conditions, connected by 'AND'
        self._orderby = []
        self._groupby = []
        self._values_fields = ''    # sql following 'SELECT' keyword
        self._limit = ''
        self._offset = ''

    def _convert_fields(self) -> None:
        """
            Generate self._values_fields
        """
        processed_fields = []
        for field in self._fields:
            if field in self._datetime_fields:
                processed_fields.append(
                    f"FORMAT_DATETIME(CAST({field} AT TIME ZONE '{TIME_ZONE}' AS TIMESTAMP), 'yyyy-MM-dd''T''HH:mm:ss') {field}"
                )
            elif field in self._map:
                processed_fields.append(f"{field} {self._map[field]}")
            else:
                processed_fields.append(field)
        self._values_fields = ', '.join(processed_fields)

    def _get_negative_op(self, op: str) -> str:
        """
        This function returns the negative counterpart of an operation.

        :param op: str, operation
        :return: str, negative operation
        """
        m = {
            'in': 'not in',
            'eq': '!=',
            'ne': '=',
            'gt': '<=',
            'lt': '>=',
            'gte': '<',
            'lte': '>'
        }
        if op in m:
            return m[op]
        return op

    def _get_op(self, op: str) -> str:
        """
        This function returns the operation.

        :param op: str, operation
        :return: str, operation
        """
        m = {
            'in': 'in',
            'eq': '=',
            'ne': '!=',
            'gt': '>',
            'lt': '<',
            'gte': '>=',
            'lte': '<='
        }
        if op in m:
            return m[op]
        return op

    def filter(self, join_by_or=False, **kwargs):
        """
            Usage: .filter(join_by_or=False, id=1, input_md5__in=['id', 'input_md5'], batch_id_id__in=aaa,bbb,ccc)
            Filters are connected by 'AND' only for simplicity for now. 
            However, you may specify the operator using 'join_by_or' within one filter.
            Example:
                .filter(id=1, input_md5__in=['id1', 'input_md5a'])
                .filter(join_by_or=True, id=2, input_md5__in=['id2', 'input_md5b'])
                will generate SQL like
                    WHERE (id='1' AND input_md5 IN ('id1', 'input_md5a')) AND (id='2' OR input_md5 IN ('id2', 'input_md5b'))
        """
        if not kwargs:
            return self
        logical_op = ' OR ' if join_by_or else ' AND '
        conditions = []
        for k, v in kwargs.items():
            if isinstance(v, str) and '__' in k and k.split('__')[1] == 'in':
                v = v.split(',')

            if isinstance(v, str) and '__' in k:
                field, op = k.split('__')
                if field in self._filter_map:
                    field = self._filter_map[field]
                op = self._get_op(op)
                if op == 'contains':
                    conditions.append(f"{field} LIKE '%{v}%'")
                elif op == 'icontains':
                    conditions.append(f"LOWER({field}) LIKE LOWER('%{v}%')")
                else:
                    if not (v.startswith("'") and v.endswith("'")):
                        v = f"'{v}'"
                    conditions.append(f'{field} {op} {v}')

            elif isinstance(v, (list, tuple)) and '__' in k:
                field, op = k.split('__')
                if field in self._filter_map:
                    field = self._filter_map[field]
                op = self._get_op(op)
                for i in range(len(v)):
                    if isinstance(v[i], str) and not (v[i].startswith("'")
                                                      and v[i].endswith("'")):
                        v[i] = f"'{v[i]}'"
                conditions.append(f'{field} {op} ({", ".join(v)})')
            else:
                if k in self._filter_map:
                    k = self._filter_map[k]
                if isinstance(
                        v,
                        str) and not (v.startswith("'") and v.endswith("'")):
                    conditions.append(f"{k}='{v}'")
                else:
                    conditions.append(f"{k}={v}")
        self._conditions.append(f"({logical_op.join(conditions)})")
        return self

    # def filter(self, **kwargs):
    #     """
    #         Usage: .filter(id=1, input_md5__in=['id', 'input_md5'], batch_id_id__in=aaa,bbb,ccc)
    #     """
    #     for k, v in kwargs.items():
    #         if isinstance(v, str) and '__' in k:
    #             field, op = k.split('__')
    #             if field in self._filter_map:
    #                 field = self._filter_map[field]
    #             op = self._get_op(op)
    #             if op == 'in':
    #                 self.filter(**{k: v.split(',')})
    #                 continue
    #             elif op == 'contains':
    #                 self._conditions.append(f"{field} LIKE '%{v}%'")
    #             elif op == 'icontains':
    #                 self._conditions.append(
    #                     f"LOWER({field}) LIKE LOWER('%{v}%')")
    #             else:
    #                 if not (v.startswith("'") and v.endswith("'")):
    #                     v = f"'{v}'"
    #                 self._conditions.append(f'{field} {op} {v}')

    #         elif isinstance(v, (list, tuple)) and '__' in k:
    #             field, op = k.split('__')
    #             if field in self._filter_map:
    #                 field = self._filter_map[field]
    #             op = self._get_op(op)
    #             for i in range(len(v)):
    #                 if isinstance(v[i], str) and not (v[i].startswith("'")
    #                                                   and v[i].endswith("'")):
    #                     v[i] = f"'{v[i]}'"
    #             self._conditions.append(f'{field} {op} ({", ".join(v)})')
    #         else:
    #             if k in self._filter_map:
    #                 k = self._filter_map[k]
    #             if isinstance(
    #                     v,
    #                     str) and not (v.startswith("'") and v.endswith("'")):
    #                 self._conditions.append(f"{k}='{v}'")
    #             else:
    #                 self._conditions.append(f"{k}={v}")
    #     return self

    def exclude(self, **kwargs):
        """
        Usage: .exclude(id=1, input_md5__in=['id', 'input_md5'], batch_id_id__in=aaa,bbb,ccc)
        The inner operation is 'AND'.
        """
        for k, v in kwargs.items():
            if isinstance(v, str) and '__' in k:
                field, op = k.split('__')
                if field in self._filter_map:
                    field = self._filter_map[field]
                op = self._get_negative_op(op)
                if op == 'in':
                    self.exclude(**{k: v.split(',')})
                    continue
                elif op == 'contains':
                    self._conditions.append(f"{field} NOT LIKE '%{v}%'")
                elif op == 'icontains':
                    self._conditions.append(
                        f"LOWER({field}) NOT LIKE LOWER('%{v}%')")
                else:
                    if not (v.startswith("'") and v.endswith("'")):
                        v = f"'{v}'"
                    self._conditions.append(f'{field} {op} {v}')

            elif isinstance(v, (list, tuple)) and '__' in k:
                field, op = k.split('__')
                if field in self._filter_map:
                    field = self._filter_map[field]
                op = self._get_negative_op(op)
                for i in range(len(v)):
                    if isinstance(v[i], str) and not (v[i].startswith("'")
                                                      and v[i].endswith("'")):
                        v[i] = f"'{v[i]}'"
                self._conditions.append(f'{field} {op} ({", ".join(v)})')
            else:
                if k in self._filter_map:
                    k = self._filter_map[k]
                if isinstance(
                        v,
                        str) and not (v.startswith("'") and v.endswith("'")):
                    self._conditions.append(f"{k}!='{v}'")
                else:
                    self._conditions.append(f"{k}!={v}")
        return self

    def limit(self, limit: int):
        self._limit = f'LIMIT {limit}'
        return self

    def offset(self, offset: int):
        self._offset = f'OFFSET {offset}'
        return self

    def order_by(self, columns: List[tuple[str, str]]):
        """
            Usage: .order_by([('id', 'DESC'), ('input_md5', 'ASC')])
        """
        self._orderby = columns
        return self

    def create_time_range(self, start: datetime, end: datetime):
        """
            Suggest to use this function to specify the range of create_time.
            Dont query without specifying the range of create_time unless you know what you are doing.
        """
        self._conditions.append(
            "create_time>=TIMESTAMP '%s %s'" % (start, TIME_ZONE))
        self._conditions.append(
            "create_time<TIMESTAMP '%s %s'" % (end, TIME_ZONE))
        return self

    def remove_fields(self, *fields):
        for field in fields:
            if field in self._fields:
                self._fields.remove(field)
        return self

    def values(self, *fields):
        self._fields = list(fields)
        return self

    def group_by(self, *fields):
        self._groupby = list(fields)
        return self

    def fetch(self):
        # if not self._create_time_start or not self._create_time_end:
        #     raise Exception('Must specify the rough range of create_time.')

        self._convert_fields()
        conditions = f' AND '.join(
            self._conditions) if self._conditions else ''
        orderby = ''
        if self._orderby:
            orderby = 'ORDER BY '
            for ele in self._orderby:
                orderby += f'{ele[0]} {ele[1]}, '
            if orderby.endswith(', '):
                orderby = orderby[:-2]
        groupby = ''
        if self._groupby:
            groupby = 'GROUP BY '
            for ele in self._groupby:
                groupby += f'{ele}, '
            if groupby.endswith(', '):
                groupby = groupby[:-2]

        sql = """
            SELECT %s
            FROM %s.%s
            WHERE %s
            %s %s %s %s
        """
        ret = []
        for one in self._table.query_by_sql(
                sql % (self._values_fields, self._ns, self._table_name,
                       conditions, groupby, orderby, self._offset,
                       self._limit)):
            for key in self._json_keys:
                if one.get(key) is not None:
                    one[key] = json.loads(one[key])
            ret.append(SimpleNamespace(**one))  # SimpleNamespace is a dict-like object which can be accessed by dot
        return ret


class TrinoWorkflow(IcebergDB):
    """
        Due to 'tag' is used as a key in Iceberg, we use '_tag' to represent 'tag' in Iceberg.
    """
    def __init__(self) -> None:
        super().__init__(workflow_tb)
        self.objects = self.QuerySet(self._table, self._table_name, self._ns)

    class QuerySet(TrinoQuerySet):
        def __init__(self, table: TableIceberg, table_name: str,
                     namespace: str):
            super().__init__(table, table_name, namespace)
            self._fields = [
                'workflow_id', 'workflow_type', 'workflow_name', 'user',
                'workflow_input', 'workflow_output', 'log', 'workflow_status',
                'priority', '_tag', 'create_time', 'update_time',
                'batch_id_id', 'hook', 'device', 'tos_id', 'device_num',
                'data_source', 'category', 'upload_ttl', 'bag_nums', 'metric'
            ]
            self._json_keys = [
                'workflow_input', 'workflow_output', 'log', 'tag', 'hook',
                'metric'
            ]
            self._datetime_fields = ['create_time', 'update_time']
            self._map = {  # map the field name in the database to the returned field name
                'batch_id_id': 'batch_id',
                '_tag': 'tag'
            }
            self._filter_map = {
                'tag': '_tag',
                'batch_id': 'batch_id_id',
            }


class TrinoResult(IcebergDB):
    def __init__(self) -> None:
        super().__init__(result_tb)
        self.objects = self.QuerySet(self._table, self._table_name, self._ns)

    class QuerySet(TrinoQuerySet):
        def __init__(self, table: TableIceberg, table_name: str,
                     namespace: str):
            super().__init__(table, table_name, namespace)
            self._fields = [
                'id', 'input_md5', 'output_md5', 'log', 'metric',
                'create_time', 'update_time', 'workflow_id_id',
                'error_details', 'error_stage', 'error_type'
            ]
            self._json_keys = ['log', 'metric']
            self._datetime_fields = ['create_time', 'update_time']
            self._map = {  # map the field name in the database to the returned field name
                'workflow_id_id': 'workflow_id',
            }
            self._filter_map = {
                'workflow_id': 'workflow_id_id',
            }


# ============= new =====================
# class TrinoHandler(IcebergDB):
#     def __init__(self, table_name: str) -> None:
#         super().__init__(table_name)
#         self.sql = ''

#     def _convert_sql(self):
#         ...

#     def run_mysql(self, sql: str) -> List[dict]:
#         self.sql = sql
#         self._convert_sql()
#         return self.run_trino(self.sql)

#     def run_trino(self, sql: str) -> List[dict]:
#         self.sql = sql
#         return list(self._table.query_by_sql(self.sql))

# class ResultTrinoHandler(TrinoHandler):
#     def __init__(self) -> None:
#         super().__init__('results')

#     def _convert_sql(self):
#         self.sql = self.sql.replace('result', 'ars.results')

# class WorkflowTrinoHandler(TrinoHandler):
#     def __init__(self) -> None:
#         super().__init__('workflows')
#         self.mysql2trino = {
#             '`': '',
#             'workflows': 'ars.results',
#             'tag': '_tag',

#         }

#     def _convert_sql(self):
#         self.sql = self.sql.replace('workflows', 'ars.results')
